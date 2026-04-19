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
  parseBody,
} from './handlers.js';

const PORT = config.apiPort;
const API_KEY = config.apiKey;

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
});

// ── 优雅退出 ──

const SIGNALS = ['SIGINT', 'SIGTERM', 'SIGHUP'];

SIGNALS.forEach(sig => {
  process.on(sig, () => {
    console.log(`\n[api] 🛑 收到 ${sig}，关闭服务器...`);
    server.close(() => {
      console.log('[api] ✅ 服务器已关闭');
      process.exit(0);
    });
  });
});

process.on('uncaughtException', (err) => {
  console.error('[api] ❌ 未捕获异常:', err.message);
});

process.on('unhandledRejection', (reason) => {
  console.error('[api] ❌ 未处理 rejection:', reason);
});
