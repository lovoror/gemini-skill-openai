/**
 * server.js — OpenAI 兼容 API 服务器入口
 *
 * 独立进程运行，通过 browser.js 连接 Daemon 管理的浏览器，
 * 对外提供 OpenAI 标准的 RESTful HTTP API。
 *
 * 启动方式：
 *   node src/api/server.js
 *   API_PORT=3000 API_KEY=your-key node src/api/server.js
 *
 * API 端点：
 *   GET  /v1/models              — 模型列表
 *   POST /v1/chat/completions    — 聊天补全（支持 SSE 流式）
 *   POST /v1/images/generations  — 图片生成
 *   GET  /files/:filename        — 静态文件服务（生成图片下载）
 *   GET  /health                 — 健康检查
 */
import { createServer } from 'node:http';
import config from '../config.js';
import {
  handleModels,
  handleChatCompletions,
  handleImageGenerations,
  handleFileServing,
} from './handlers.js';
import { apiLogger } from './logger.js';

const PORT = config.apiPort;
const API_KEY = config.apiKey;
const DAEMON_URL = `http://127.0.0.1:${config.daemonPort}`;

/**
 * 通知 Daemon 优雅退出（必要时）
 */
async function shutdownDaemon() {
  if (!config.daemonStopOnExit) return;
  try {
    await fetch(`${DAEMON_URL}/shutdown`, {
      method: 'POST',
      signal: AbortSignal.timeout(3000),
    });
    console.log('[api] 🛑 已通知 Daemon 退出');
  } catch {
    // Daemon 未运行或已退出，忽略
  }
}

// ── 认证中间件 ──

function checkAuth(req) {
  if (!API_KEY) return true; // 未配置则跳过

  const auth = req.headers.authorization || '';
  if (auth === `Bearer ${API_KEY}`) return true;

  return false;
}

// ── CORS 处理 ──

function handleCORS(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return true;
  }
  return false;
}

// ── 路由表 ──

const routes = {
  'GET /v1/models': handleModels,
  'POST /v1/chat/completions': handleChatCompletions,
  'POST /v1/images/generations': handleImageGenerations,
};

// ── HTTP 服务器 ──

const server = createServer((req, res) => {
  const reqId = apiLogger.nextRequestId();
  req.apiReqId = reqId;

  const startedAt = Date.now();
  let responseBytes = 0;
  let writeChunks = 0;
  let endPreview = '';
  // 流式响应内容捕获：保留头部 500 字节 + 滚动尾部 800 字节
  let streamHead = '';
  let streamTail = '';
  const HEAD_LIMIT = 500;
  const TAIL_LIMIT = 800;

  const originalWrite = res.write.bind(res);
  const originalEnd = res.end.bind(res);

  res.write = (chunk, encoding, cb) => {
    if (chunk) {
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk), encoding || 'utf-8');
      responseBytes += buf.length;
      writeChunks += 1;
      // 捕获流式内容用于日志预览
      const text = buf.toString('utf-8');
      if (streamHead.length < HEAD_LIMIT) {
        streamHead += text;
        if (streamHead.length > HEAD_LIMIT) streamHead = streamHead.slice(0, HEAD_LIMIT);
      }
      streamTail += text;
      if (streamTail.length > TAIL_LIMIT) streamTail = streamTail.slice(-TAIL_LIMIT);
    }
    return originalWrite(chunk, encoding, cb);
  };

  res.end = (chunk, encoding, cb) => {
    if (chunk) {
      const text = Buffer.isBuffer(chunk) ? chunk.toString('utf-8') : String(chunk);
      endPreview = text.length > 800 ? `${text.slice(0, 800)}...[truncated]` : text;
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(text, encoding || 'utf-8');
      responseBytes += buf.length;
      writeChunks += 1;
    }
    return originalEnd(chunk, encoding, cb);
  };

  res.on('finish', () => {
    // 非流式用 endPreview；流式用 streamHead + streamTail
    let responsePreview = endPreview || null;
    if (!responsePreview && streamHead) {
      const tail = streamTail && streamTail !== streamHead ? `\n...[tail]...\n${streamTail}` : '';
      responsePreview = streamHead + tail;
    }
    apiLogger.log('http_request', {
      reqId,
      method: req.method,
      path: (req.url || '/').split('?')[0],
      statusCode: res.statusCode,
      durationMs: Date.now() - startedAt,
      responseBytes,
      writeChunks,
      requestBody: req.apiRequestBody || null,
      responsePreview,
    });
  });

  // CORS 预检
  if (handleCORS(req, res)) return;

  const { method, url } = req;
  const path = (url || '/').split('?')[0];

  // 健康检查（不需要认证）
  if (method === 'GET' && path === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      service: 'gemini-openai-api',
      uptime: Math.round(process.uptime()),
    }));
    return;
  }

  // 静态文件服务（不需要认证）
  if (method === 'GET' && path.startsWith('/files/')) {
    handleFileServing(req, res);
    return;
  }

  // 认证检查
  if (!checkAuth(req)) {
    apiLogger.log('auth_failed', {
      reqId,
      method,
      path,
      hasAuthorizationHeader: Boolean(req.headers.authorization),
    });
    res.writeHead(401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      error: {
        message: 'Invalid API key. Provide a valid key via Authorization: Bearer <key>',
        type: 'authentication_error',
        code: 'invalid_api_key',
      },
    }));
    return;
  }

  // 路由分发
  const routeKey = `${method} ${path}`;
  const handler = routes[routeKey];

  if (handler) {
    handler(req, res);
  } else {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      error: { message: `Not found: ${path}`, type: 'invalid_request_error', code: null },
    }));
  }
});

server.listen(PORT, () => {
  console.log(`[api] 🚀 OpenAI 兼容 API 已启动 — http://127.0.0.1:${PORT}`);
  console.log(`[api] 🔑 认证: ${API_KEY ? '已启用（需 Bearer Token）' : '未启用（无需认证）'}`);
  console.log(`[api]    GET  /v1/models              — 模型列表`);
  console.log(`[api]    POST /v1/chat/completions    — 聊天补全`);
  console.log(`[api]    POST /v1/images/generations  — 图片生成`);
  console.log(`[api]    GET  /files/:filename        — 文件下载`);
  console.log(`[api]    GET  /health                 — 健康检查`);
  console.log(`[api] 🧾 文件日志: ${config.apiLogEnabled ? `已启用 (${config.apiLogDir})` : '未启用'}`);
});

// ── 优雅退出 ──

const SIGNALS = ['SIGINT', 'SIGTERM', 'SIGHUP'];

SIGNALS.forEach(sig => {
  process.on(sig, async () => {
    console.log(`\n[api] 🛑 收到 ${sig}，关闭服务器...`);
    await shutdownDaemon();
    server.close(() => {
      console.log('[api] ✅ 服务器已关闭');
      process.exit(0);
    });
  });
});

process.on('uncaughtException', (err) => {
  apiLogger.log('uncaught_exception', { message: err?.message, stack: err?.stack });
  console.error('[api] ❌ 未捕获异常:', err.message);
});

process.on('unhandledRejection', (reason) => {
  apiLogger.log('unhandled_rejection', {
    reason: reason instanceof Error
      ? { message: reason.message, stack: reason.stack }
      : String(reason),
  });
  console.error('[api] ❌ 未处理 rejection:', reason);
});
