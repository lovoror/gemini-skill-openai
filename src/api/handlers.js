/**
 * handlers.js — OpenAI 兼容 API 路由处理器
 *
 * 端点：
 *   GET  /v1/models                — 模型列表
 *   POST /v1/chat/completions      — 聊天补全（支持流式 SSE）
 *   POST /v1/images/generations    — 图片生成
 *   GET  /files/:filename          — 静态文件服务（图片下载）
 */
import { createReadStream, existsSync, statSync, writeFileSync, mkdirSync } from 'node:fs';
import { basename, extname, join, resolve } from 'node:path';
import { createGeminiSession, disconnect } from '../index.js';
import config from '../config.js';
import { sleep } from '../util.js';
import { enqueue } from './queue.js';
import { writeSSEHeaders, writeSSEChunk, writeSSEDone } from './stream.js';
import { apiLogger, sanitizeForLog } from './logger.js';

// ── 模型目录 ──
// browser 内部只有 pro / quick / think 三档；图片生成仍复用 MCP 的 ensureModelPro() 流程。
// 因此 chat 与 images 必须分开校验，不能把 image-only model 当成聊天模型来切换。

const CHAT_MODEL_MAP = {
  // ── Gemini 3.1 ──
  'gemini-3.1-pro':         'pro',
  'gemini-3.1-pro-preview': 'pro',
  'gemini-3.1-flash':       'quick',

  // ── Gemini 3 ──
  'gemini-3-pro':         'pro',
  'gemini-3-pro-high':    'pro',
  'gemini-3-pro-preview': 'pro',
  'gemini-3-flash':         'quick',
  'gemini-3-flash-preview': 'quick',

  // ── Gemini 2.5 ──
  'gemini-2.5-pro':              'pro',
  'gemini-2.5-flash':            'quick',
  'gemini-2.5-flash-lite':       'quick',
  'gemini-2.5-flash-thinking':   'think',

  // ── 通用别名 ──
  'gemini-pro':     'pro',
  'gemini-flash':   'quick',
  'gemini-thinking': 'think',
};

const IMAGE_MODEL_IDS = [
  // 当前底层统一走 ensureModelPro() + generateImage()，不区分分辨率/比例。
  // 待 ops.generateImage() 支持参数化后再按需扩展此列表。
  'gemini-3.1-flash-image',
];

const IMAGE_MODEL_SET = new Set(IMAGE_MODEL_IDS);

// Unix 时间戳（近似发布日期）
const T_31 = 1745000000; // Gemini 3.1 系列 ≈ 2026-04
const T_3  = 1735000000; // Gemini 3   系列 ≈ 2025-12
const T_25 = 1720000000; // Gemini 2.5 系列 ≈ 2024-07

const MODELS = [
  // ── Gemini 3.1 文本 ──
  { id: 'gemini-3.1-pro',         created: T_31, owned_by: 'google' },
  { id: 'gemini-3.1-pro-preview', created: T_31, owned_by: 'google' },
  { id: 'gemini-3.1-flash',       created: T_31, owned_by: 'google' },

  // ── Gemini 3.1 图片生成 ──
  { id: 'gemini-3.1-flash-image', created: T_31, owned_by: 'google' },

  // ── Gemini 3 ──
  { id: 'gemini-3-pro',           created: T_3, owned_by: 'google' },
  { id: 'gemini-3-pro-high',      created: T_3, owned_by: 'google' },
  { id: 'gemini-3-pro-preview',   created: T_3, owned_by: 'google' },
  { id: 'gemini-3-flash',         created: T_3, owned_by: 'google' },
  { id: 'gemini-3-flash-preview', created: T_3, owned_by: 'google' },

  // ── Gemini 2.5 ──
  { id: 'gemini-2.5-pro',            created: T_25, owned_by: 'google' },
  { id: 'gemini-2.5-flash',          created: T_25, owned_by: 'google' },
  { id: 'gemini-2.5-flash-lite',     created: T_25, owned_by: 'google' },
  { id: 'gemini-2.5-flash-thinking', created: T_25, owned_by: 'google' },
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
        const parsed = raw ? JSON.parse(raw) : {};
        req.apiRequestBody = sanitizeForLog(parsed);
        resolve(parsed);
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
  const internalModel = CHAT_MODEL_MAP[modelName];
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
    apiLogger.log('chat_completion_parse_error', {
      reqId: req.apiReqId,
      message: err?.message,
    });
    sendError(res, 400, err.message);
    return;
  }

  const { messages, model, stream, response_format } = body;

  // model 校验优先（可以不传，不传时使用默认值）
  if (model && IMAGE_MODEL_SET.has(model)) {
    sendError(res, 400, 'Image generation models are only supported on /v1/images/generations');
    return;
  }

  if (model && !CHAT_MODEL_MAP[model]) {
    sendError(res, 400, `Unsupported chat model: ${model}`);
    return;
  }

  const requestedModel = model || 'gemini-2.5-pro';

  apiLogger.log('chat_completion_request', {
    reqId: req.apiReqId,
    model: requestedModel,
    stream: Boolean(stream),
    messageCount: Array.isArray(messages) ? messages.length : 0,
  });

  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    sendError(res, 400, 'messages is required and must be a non-empty array');
    return;
  }

  const { prompt: rawPrompt, userContent, images } = extractPrompt(messages);
  if (!userContent.trim()) {
    sendError(res, 400, 'No user message content found');
    return;
  }

  // response_format 支持：当 type 为 json_object 时，追加 JSON 强制指令
  const jsonMode = response_format?.type === 'json_object';
  const prompt = jsonMode
    ? rawPrompt + '\n\n[IMPORTANT: You must respond with valid JSON only. No explanations, no markdown code fences, no extra text before or after the JSON.]'
    : rawPrompt;

  try {
    await enqueue(() => stream
      ? streamChatCompletion(res, prompt, images, requestedModel)
      : nonStreamChatCompletion(res, prompt, images, requestedModel)
    );
  } catch (err) {
    apiLogger.log('chat_completion_error', {
      reqId: req.apiReqId,
      model: requestedModel,
      stream: Boolean(stream),
      message: err?.message,
      stack: err?.stack,
      status: err?.status,
    });
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
    // 每次请求进入临时对话，保持无状态（与 OpenAI API 语义一致）
    await ops.click('newChatBtn');
    await sleep(250);
    await ops.clickTempChat();

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
    // 每次请求进入临时对话，保持无状态（与 OpenAI API 语义一致）
    await ops.click('newChatBtn');
    await sleep(250);
    await ops.clickTempChat();

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

    // 记录发送前已有的响应数量，防止误取历史回复
    const beforeCount = await ops.countResponses();

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

      // 提取当前文本（仅使用新生成的响应）
      const textResp = await ops.getLatestTextResponse();
      if (textResp.ok && textResp.text && textResp.index >= beforeCount) {
        const currentText = textResp.text;
        if (currentText.length > prevText.length) {
          if (currentText.startsWith(prevText)) {
            const delta = currentText.slice(prevText.length);
            writeSSEChunk(res, {
              id, object: 'chat.completion.chunk', created, model: modelName,
              choices: [{ index: 0, delta: { content: delta }, finish_reason: null }],
            });
          }
          // else: stripLabel 行为在流式中途变化，跳过本轮 delta 防止内容错位
          prevText = currentText;
        }
      }

      // 检查是否完成
      if (status.status === 'mic') {
        // 最后再拉一次确保拿到完整文本（仅使用新生成的响应）
        const finalResp = await ops.getLatestTextResponse();
        if (finalResp.ok && finalResp.text && finalResp.index >= beforeCount && finalResp.text.length > prevText.length) {
          if (finalResp.text.startsWith(prevText)) {
            const delta = finalResp.text.slice(prevText.length);
            writeSSEChunk(res, {
              id, object: 'chat.completion.chunk', created, model: modelName,
              choices: [{ index: 0, delta: { content: delta }, finish_reason: null }],
            });
          }
          // else: 前缀不匹配，跳过避免错位
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
    apiLogger.log('image_generation_parse_error', {
      reqId: req.apiReqId,
      message: err?.message,
    });
    sendError(res, 400, err.message);
    return;
  }

  const { prompt, response_format = 'b64_json', n = 1, model = 'gemini-3.1-flash-image' } = body;

  apiLogger.log('image_generation_request', {
    reqId: req.apiReqId,
    model,
    responseFormat: response_format,
    n,
  });

  if (!prompt || typeof prompt !== 'string' || !prompt.trim()) {
    sendError(res, 400, 'prompt is required and must be a non-empty string');
    return;
  }

  if (typeof model !== 'string' || !IMAGE_MODEL_SET.has(model)) {
    sendError(res, 400, `Unsupported image model: ${model}`);
    return;
  }

  // Gemini 一次只能生成一张
  if (n !== 1) {
    sendError(res, 400, 'Only n=1 is supported');
    return;
  }

  try {
    const result = await enqueue(() => generateImageHandler(prompt, response_format, req, model));
    sendJSON(res, 200, result);
  } catch (err) {
    apiLogger.log('image_generation_error', {
      reqId: req.apiReqId,
      model,
      responseFormat: response_format,
      message: err?.message,
      stack: err?.stack,
      status: err?.status,
    });
    if (err.status === 429) {
      sendError(res, 429, 'Too many requests, please try again later', 'rate_limit_error');
    } else if (!res.headersSent) {
      sendError(res, 500, err.message || 'Internal server error', 'server_error');
    }
  }
}

async function generateImageHandler(prompt, responseFormat, req, modelName) {
  // 注意：modelName 的 2k / 4k / aspect-ratio 后缀当前仅作声明用途，
  // 底层 ops.generateImage() 尚未支持分辨率/比例参数化，所有图片模型统一走 ensureModelPro()。
  // 后续如需区分，可在此处解析 modelName 并向 ops.generateImage() 传入对应参数。
  const { ops } = await createGeminiSession();

  try {
    // 每次生图都进入临时对话（与 MCP gemini_temp_chat 相同模式）：
    //   1. newChatBtn → 空白页
    //   2. 250ms 等页面稳定
    //   3. clickTempChat → 隔离 session，不记录历史，不受旧对话影响
    await ops.click('newChatBtn');
    await sleep(250);
    await ops.clickTempChat();

    await ops.ensureModelPro();

    // 显式要求生成图片，防止 Gemini 误以为是文字对话而返回文字描述
    const imagePrompt = `Generate an image of: ${prompt}`;

    // 统一使用 base64 提取路径（fullSize CDP 下载流程依赖 Gemini UI 悬浮按钮，不稳定）
    // response_format=url 时：提取 base64 → 写文件 → 返回 /files/ URL
    const result = await ops.generateImage(imagePrompt, { fullSize: false, timeout: 180_000 });

    if (!result.ok) {
      throw new Error(`Image generation failed: ${result.error}`);
    }

    const created = Math.floor(Date.now() / 1000);
    const data = [];

    if (responseFormat === 'url' && result.dataUrl) {
      // 将 base64 写入文件，返回可访问的 HTTP URL
      const outputDir = resolve(config.outputDir);
      mkdirSync(outputDir, { recursive: true });
      const mimeMatch = result.dataUrl.match(/^data:([^;]+);base64,/);
      const mime = mimeMatch ? mimeMatch[1] : 'image/jpeg';
      const ext = mime.split('/')[1] || 'jpg';
      const filename = `gemini_${Date.now()}.${ext}`;
      const filePath = join(outputDir, filename);
      const b64 = result.dataUrl.replace(/^data:[^;]+;base64,/, '');
      writeFileSync(filePath, Buffer.from(b64, 'base64'));
      const host = req.headers.host || `127.0.0.1:${config.apiPort}`;
      const protocol = req.headers['x-forwarded-proto'] || 'http';
      const url = `${protocol}://${host}/files/${encodeURIComponent(filename)}`;
      data.push({ url, revised_prompt: prompt });
    } else if (result.dataUrl) {
      // 去掉 data:image/png;base64, 前缀
      const b64 = result.dataUrl.replace(/^data:[^;]+;base64,/, '');
      data.push({ b64_json: b64, revised_prompt: prompt });
    } else if (result.filePath) {
      // fallback：文件路径（已存在于磁盘）
      const filename = basename(result.filePath);
      const host = req.headers.host || `127.0.0.1:${config.apiPort}`;
      const protocol = req.headers['x-forwarded-proto'] || 'http';
      const url = `${protocol}://${host}/files/${encodeURIComponent(filename)}`;
      data.push({ url, revised_prompt: prompt });
    } else {
      throw new Error('No image data in result');
    }

    return { created, data, model: modelName };
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
