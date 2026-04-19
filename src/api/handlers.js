/**
 * handlers.js — OpenAI 兼容 API 路由处理器
 *
 * 端点：
 *   GET  /v1/models                — 模型列表
 *   POST /v1/chat/completions      — 聊天补全（支持流式 SSE）
 *   POST /v1/images/generations    — 图片生成
 *   GET  /files/:filename          — 静态文件服务（图片下载）
 */
import { createReadStream, existsSync, statSync } from 'node:fs';
import { basename, extname, join, resolve } from 'node:path';
import { createGeminiSession, disconnect } from '../index.js';
import config from '../config.js';
import { sleep } from '../util.js';
import { enqueue } from './queue.js';
import { writeSSEHeaders, writeSSEChunk, writeSSEDone } from './stream.js';

// ── 模型映射 ──

const MODEL_MAP = {
  'gemini-3.1-pro': 'pro',
  'gemini-pro': 'pro',
  'gemini-3.1-flash': 'quick',
  'gemini-flash': 'quick',
  'gemini-2.5-flash-thinking': 'think',
  'gemini-thinking': 'think',
};

const MODELS = [
  { id: 'gemini-2.5-pro', created: 1700000000, owned_by: 'google' },
  { id: 'gemini-2.0-flash', created: 1700000000, owned_by: 'google' },
  { id: 'gemini-2.5-flash-thinking', created: 1700000000, owned_by: 'google' },
];

/**
 * 生成唯一的 completion id
 */
function makeId() {
  return 'chatcmpl-' + Math.random().toString(36).slice(2, 14) + Date.now().toString(36);
}

/**
 * 粗略估算 token 数
 * 中文按 2 token/字，英文按 0.75 token/word
 */
function estimateTokens(text) {
  if (!text) return 0;
  const cjk = (text.match(/[\u4e00-\u9fff\u3400-\u4dbf]/g) || []).length;
  const rest = text.replace(/[\u4e00-\u9fff\u3400-\u4dbf]/g, '');
  const words = rest.split(/\s+/).filter(Boolean).length;
  return Math.ceil(cjk * 2 + words * 0.75);
}

// ── 工具函数 ──

function sendJSON(res, statusCode, data) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(JSON.stringify(data));
}

function sendError(res, status, message, type = 'invalid_request_error', code = null) {
  sendJSON(res, status, {
    error: { message, type, code },
  });
}

/**
 * 从请求 body 解析 JSON
 */
export function parseBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    const MAX = 10 * 1024 * 1024; // 10 MB

    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX) {
        reject(new Error('Request body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });

    req.on('end', () => {
      try {
        const raw = Buffer.concat(chunks).toString('utf-8');
        resolve(raw ? JSON.parse(raw) : {});
      } catch {
        reject(new Error('Invalid JSON'));
      }
    });

    req.on('error', reject);
  });
}

/**
 * 从 messages 数组中提取 prompt 文本和图片 URL
 */
function extractPrompt(messages) {
  if (!Array.isArray(messages) || messages.length === 0) {
    return { prompt: '', images: [] };
  }

  // 收集 system 消息
  const systemParts = [];
  let userContent = '';
  const images = [];

  for (const msg of messages) {
    if (msg.role === 'system' || msg.role === 'developer') {
      const text = typeof msg.content === 'string' ? msg.content : '';
      if (text) systemParts.push(text);
    }
  }

  // 取最后一条 user 消息
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg.role !== 'user') continue;

    if (typeof msg.content === 'string') {
      userContent = msg.content;
    } else if (Array.isArray(msg.content)) {
      // 多模态内容
      for (const part of msg.content) {
        if (part.type === 'text') {
          userContent += part.text;
        } else if (part.type === 'image_url' && part.image_url?.url) {
          images.push(part.image_url.url);
        }
      }
    }
    break;
  }

  const prompt = systemParts.length > 0
    ? `[System: ${systemParts.join('\n')}]\n\n${userContent}`
    : userContent;

  return { prompt, userContent, images };
}

/**
 * 根据请求的 model 名称切换 Gemini 模型
 */
async function switchModelIfNeeded(ops, modelName) {
  const internalModel = MODEL_MAP[modelName];
  if (!internalModel) return; // 未知 model 不切换

  try {
    const current = await ops.getCurrentModel();
    if (current.ok && current.model !== internalModel) {
      await ops.switchToModel(internalModel);
    }
  } catch {
    // 切换失败不阻塞请求
  }
}

// ── 端点处理器 ──

/**
 * GET /v1/models
 */
export function handleModels(_req, res) {
  sendJSON(res, 200, {
    object: 'list',
    data: MODELS.map(m => ({
      id: m.id,
      object: 'model',
      created: m.created,
      owned_by: m.owned_by,
    })),
  });
}

/**
 * POST /v1/chat/completions
 */
export async function handleChatCompletions(req, res) {
  let body;
  try {
    body = await parseBody(req);
  } catch (err) {
    sendError(res, 400, err.message);
    return;
  }

  const { messages, model, stream } = body;

  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    sendError(res, 400, 'messages is required and must be a non-empty array');
    return;
  }

  const { prompt, userContent, images } = extractPrompt(messages);
  if (!userContent.trim()) {
    sendError(res, 400, 'No user message content found');
    return;
  }

  try {
    await enqueue(() => stream
      ? streamChatCompletion(res, prompt, images, model || 'gemini-2.5-pro')
      : nonStreamChatCompletion(res, prompt, images, model || 'gemini-2.5-pro')
    );
  } catch (err) {
    if (err.status === 429) {
      sendError(res, 429, 'Too many requests, please try again later', 'rate_limit_error');
    } else if (!res.headersSent) {
      sendError(res, 500, err.message || 'Internal server error', 'server_error');
    }
  }
}

/**
 * 非流式 Chat Completion
 */
async function nonStreamChatCompletion(res, prompt, images, modelName) {
  const { ops } = await createGeminiSession();

  try {
    await switchModelIfNeeded(ops, modelName);

    // 上传参考图
    if (images.length > 0) {
      for (const img of images) {
        await ops.uploadImage(img);
      }
    }

    const result = await ops.sendAndWait(prompt, { timeout: 120_000 });

    if (!result.ok) {
      sendError(res, 504, `Gemini response failed: ${result.error}`, 'server_error');
      return;
    }

    const text = result.text || '';
    const id = makeId();
    const created = Math.floor(Date.now() / 1000);

    sendJSON(res, 200, {
      id,
      object: 'chat.completion',
      created,
      model: modelName,
      choices: [{
        index: 0,
        message: { role: 'assistant', content: text },
        finish_reason: 'stop',
      }],
      usage: {
        prompt_tokens: estimateTokens(prompt),
        completion_tokens: estimateTokens(text),
        total_tokens: estimateTokens(prompt) + estimateTokens(text),
      },
    });
  } finally {
    disconnect();
  }
}

/**
 * 流式 Chat Completion（SSE）
 *
 * 逻辑：fillPrompt → click send → 轮询 getLatestTextResponse 计算 delta → SSE chunk
 */
async function streamChatCompletion(res, prompt, images, modelName) {
  const { ops } = await createGeminiSession();

  try {
    await switchModelIfNeeded(ops, modelName);

    // 上传参考图
    if (images.length > 0) {
      for (const img of images) {
        await ops.uploadImage(img);
      }
    }

    // 启动 SSE
    writeSSEHeaders(res);

    const id = makeId();
    const created = Math.floor(Date.now() / 1000);

    // 发送 role chunk
    writeSSEChunk(res, {
      id,
      object: 'chat.completion.chunk',
      created,
      model: modelName,
      choices: [{
        index: 0,
        delta: { role: 'assistant', content: '' },
        finish_reason: null,
      }],
    });

    // 填写并发送
    const fillResult = await ops.fillPrompt(prompt);
    if (!fillResult.ok) {
      writeSSEChunk(res, {
        id, object: 'chat.completion.chunk', created, model: modelName,
        choices: [{ index: 0, delta: { content: '[Error: failed to fill prompt]' }, finish_reason: 'stop' }],
      });
      writeSSEDone(res);
      return;
    }

    await sleep(300);
    const clickResult = await ops.click('sendBtn');
    if (!clickResult.ok) {
      writeSSEChunk(res, {
        id, object: 'chat.completion.chunk', created, model: modelName,
        choices: [{ index: 0, delta: { content: '[Error: failed to send]' }, finish_reason: 'stop' }],
      });
      writeSSEDone(res);
      return;
    }

    // 轮询提取增量文本
    const timeout = 120_000;
    const interval = 800;
    const start = Date.now();
    let prevText = '';
    let finished = false;

    // 客户端断开时停止轮询
    let aborted = false;
    res.on('close', () => { aborted = true; });

    // 等一小段时间让 Gemini 开始生成
    await sleep(1500);

    while (!aborted && Date.now() - start < timeout) {
      const status = await ops.getStatus();

      // 提取当前文本
      const textResp = await ops.getLatestTextResponse();
      if (textResp.ok && textResp.text) {
        const currentText = textResp.text;
        if (currentText.length > prevText.length) {
          const delta = currentText.slice(prevText.length);
          prevText = currentText;

          writeSSEChunk(res, {
            id, object: 'chat.completion.chunk', created, model: modelName,
            choices: [{ index: 0, delta: { content: delta }, finish_reason: null }],
          });
        }
      }

      // 检查是否完成
      if (status.status === 'mic') {
        // 最后再拉一次确保拿到完整文本
        const finalResp = await ops.getLatestTextResponse();
        if (finalResp.ok && finalResp.text && finalResp.text.length > prevText.length) {
          const delta = finalResp.text.slice(prevText.length);
          writeSSEChunk(res, {
            id, object: 'chat.completion.chunk', created, model: modelName,
            choices: [{ index: 0, delta: { content: delta }, finish_reason: null }],
          });
        }
        finished = true;
        break;
      }

      await sleep(interval);
    }

    // 发送结束标记
    writeSSEChunk(res, {
      id, object: 'chat.completion.chunk', created, model: modelName,
      choices: [{
        index: 0,
        delta: {},
        finish_reason: finished ? 'stop' : 'length',
      }],
    });

    writeSSEDone(res);
  } finally {
    disconnect();
  }
}

/**
 * POST /v1/images/generations
 */
export async function handleImageGenerations(req, res) {
  let body;
  try {
    body = await parseBody(req);
  } catch (err) {
    sendError(res, 400, err.message);
    return;
  }

  const { prompt, response_format = 'b64_json', n = 1 } = body;

  if (!prompt || typeof prompt !== 'string' || !prompt.trim()) {
    sendError(res, 400, 'prompt is required and must be a non-empty string');
    return;
  }

  // Gemini 一次只能生成一张
  if (n !== 1) {
    sendError(res, 400, 'Only n=1 is supported');
    return;
  }

  try {
    const result = await enqueue(() => generateImageHandler(prompt, response_format, req));
    sendJSON(res, 200, result);
  } catch (err) {
    if (err.status === 429) {
      sendError(res, 429, 'Too many requests, please try again later', 'rate_limit_error');
    } else if (!res.headersSent) {
      sendError(res, 500, err.message || 'Internal server error', 'server_error');
    }
  }
}

async function generateImageHandler(prompt, responseFormat, req) {
  const { ops } = await createGeminiSession();

  try {
    await ops.ensureModelPro();

    const fullSize = responseFormat === 'url';
    const result = await ops.generateImage(prompt, { fullSize, timeout: 180_000 });

    if (!result.ok) {
      throw new Error(`Image generation failed: ${result.error}`);
    }

    const created = Math.floor(Date.now() / 1000);
    const data = [];

    if (responseFormat === 'url' && result.filePath) {
      // 返回可访问的 HTTP URL
      const filename = basename(result.filePath);
      const host = req.headers.host || `127.0.0.1:${config.apiPort}`;
      const protocol = req.headers['x-forwarded-proto'] || 'http';
      const url = `${protocol}://${host}/files/${encodeURIComponent(filename)}`;
      data.push({ url, revised_prompt: prompt });
    } else if (result.dataUrl) {
      // 去掉 data:image/png;base64, 前缀
      const b64 = result.dataUrl.replace(/^data:[^;]+;base64,/, '');
      data.push({ b64_json: b64, revised_prompt: prompt });
    } else if (result.filePath) {
      // fullSize=false 但结果是文件路径（fallback）
      const filename = basename(result.filePath);
      const host = req.headers.host || `127.0.0.1:${config.apiPort}`;
      const protocol = req.headers['x-forwarded-proto'] || 'http';
      const url = `${protocol}://${host}/files/${encodeURIComponent(filename)}`;
      data.push({ url, revised_prompt: prompt });
    } else {
      throw new Error('No image data in result');
    }

    return { created, data };
  } finally {
    disconnect();
  }
}

/**
 * GET /files/:filename — 静态文件服务
 */
export function handleFileServing(req, res) {
  const url = req.url || '';
  const match = url.match(/^\/files\/([^?#]+)/);
  if (!match) {
    sendError(res, 404, 'File not found');
    return;
  }

  const filename = decodeURIComponent(match[1]);

  // 路径遍历防护
  if (filename.includes('..') || filename.includes('/') || filename.includes('\\')) {
    sendError(res, 400, 'Invalid filename');
    return;
  }

  const filePath = join(resolve(config.outputDir), filename);
  const safePath = resolve(config.outputDir);

  // 二次校验确保在 outputDir 内
  if (!filePath.startsWith(safePath)) {
    sendError(res, 403, 'Access denied');
    return;
  }

  if (!existsSync(filePath)) {
    sendError(res, 404, 'File not found');
    return;
  }

  const stat = statSync(filePath);
  const ext = extname(filename).toLowerCase();
  const mimeTypes = {
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.gif': 'image/gif',
    '.webp': 'image/webp',
    '.svg': 'image/svg+xml',
  };

  res.writeHead(200, {
    'Content-Type': mimeTypes[ext] || 'application/octet-stream',
    'Content-Length': stat.size,
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'public, max-age=3600',
  });

  createReadStream(filePath).pipe(res);
}
