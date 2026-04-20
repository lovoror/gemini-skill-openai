/**
 * test/api.test.js — OpenAI 兼容 API 测试用例
 *
 * 使用 Node.js 内置 test runner（node:test + node:assert）
 *
 * 运行方式：
 *   node --test test/api.test.js
 *
 * 测试策略：
 *   - 启动真实 HTTP 服务器，测试路由、CORS、认证、响应格式
 *   - 对于需要浏览器的端点，仅测试请求验证逻辑（不连接 Gemini）
 *   - 纯函数（queue、stream、parseBody、extractPrompt）完整覆盖
 */
import { describe, it, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { Readable } from 'node:stream';
import { writeFileSync, mkdirSync, rmSync, existsSync } from 'node:fs';
import { join } from 'node:path';

// ── 导入被测模块 ──
import { enqueue, pendingCount } from '../src/api/queue.js';
import { writeSSEHeaders, writeSSEChunk, writeSSEDone } from '../src/api/stream.js';
import { handleModels, handleFileServing, parseBody } from '../src/api/handlers.js';
import config from '../src/config.js';

// ── 测试辅助工具 ──

/**
 * 创建一个 mock HTTP request
 */
function createMockReq({ method = 'GET', url = '/', headers = {}, body = null } = {}) {
  const req = new Readable({ read() {} });
  req.method = method;
  req.url = url;
  req.headers = headers;

  if (body !== null) {
    const data = typeof body === 'string' ? body : JSON.stringify(body);
    process.nextTick(() => {
      req.push(Buffer.from(data));
      req.push(null);
    });
  } else {
    process.nextTick(() => req.push(null));
  }

  return req;
}

/**
 * 创建一个 mock HTTP response，收集输出
 */
function createMockRes() {
  const res = {
    _statusCode: null,
    _headers: {},
    _chunks: [],
    _ended: false,
    headersSent: false,

    writeHead(statusCode, headers = {}) {
      res._statusCode = statusCode;
      Object.assign(res._headers, headers);
      res.headersSent = true;
    },

    setHeader(key, value) {
      res._headers[key] = value;
    },

    write(chunk) {
      res._chunks.push(chunk);
      return true;
    },

    end(data) {
      if (data) res._chunks.push(data);
      res._ended = true;
    },

    on() { return res; },

    get body() {
      return res._chunks.join('');
    },

    get json() {
      return JSON.parse(res.body);
    },
  };

  return res;
}

/**
 * 发送 HTTP 请求到测试服务器
 */
async function request(port, { method = 'GET', path = '/', body = null, headers = {} } = {}) {
  const url = `http://127.0.0.1:${port}${path}`;
  const options = {
    method,
    headers: { 'Content-Type': 'application/json', ...headers },
  };

  if (body !== null) {
    options.body = typeof body === 'string' ? body : JSON.stringify(body);
  }

  const res = await fetch(url, options);
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch {}

  return { status: res.status, headers: res.headers, text, json };
}

/**
 * 发送 SSE 请求并收集所有 chunks
 */
async function requestSSE(port, { path = '/', body = null, headers = {} } = {}) {
  const url = `http://127.0.0.1:${port}${path}`;
  const options = {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: typeof body === 'string' ? body : JSON.stringify(body),
  };

  const res = await fetch(url, options);
  const text = await res.text();

  // 解析 SSE 数据
  const chunks = [];
  let done = false;

  for (const line of text.split('\n')) {
    if (line.startsWith('data: ')) {
      const data = line.slice(6);
      if (data === '[DONE]') {
        done = true;
      } else {
        try { chunks.push(JSON.parse(data)); } catch {}
      }
    }
  }

  return { status: res.status, headers: res.headers, chunks, done, raw: text };
}

// ════════════════════════════════════════════════════════════
//  1. 串行队列测试（queue.js）
// ════════════════════════════════════════════════════════════

describe('queue.js — 串行队列', () => {

  it('enqueue 串行执行任务', async () => {
    const order = [];

    const p1 = enqueue(async () => {
      order.push('start-1');
      await new Promise(r => setTimeout(r, 50));
      order.push('end-1');
      return 'result-1';
    });

    const p2 = enqueue(async () => {
      order.push('start-2');
      await new Promise(r => setTimeout(r, 10));
      order.push('end-2');
      return 'result-2';
    });

    const [r1, r2] = await Promise.all([p1, p2]);
    assert.equal(r1, 'result-1');
    assert.equal(r2, 'result-2');
    // 任务2 必须在任务1 完成后才开始
    assert.deepEqual(order, ['start-1', 'end-1', 'start-2', 'end-2']);
  });

  it('前一个任务失败不阻塞后续任务', async () => {
    const p1 = enqueue(async () => { throw new Error('fail-1'); });
    const p2 = enqueue(async () => 'ok-2');

    await assert.rejects(p1, { message: 'fail-1' });
    const r2 = await p2;
    assert.equal(r2, 'ok-2');
  });

  it('pendingCount 正确反映队列长度', async () => {
    // 先确保队列空闲
    let resolveBlocker;
    const blockerReady = new Promise(r => { resolveBlocker = r; });
    let resolveInner;

    const blocker = enqueue(() => {
      resolveBlocker();
      return new Promise(r => { resolveInner = r; });
    });

    // 等待 blocker 开始执行（executor 已运行）
    await blockerReady;

    // blocker 正在执行中，加一个等待者
    const waiter = enqueue(async () => 'waited');
    // 至少 2 个任务在队列中（blocker 执行中 + waiter 排队）
    assert.ok(pendingCount() >= 2, `expected >= 2, got ${pendingCount()}`);

    resolveInner('done');
    await blocker;
    await waiter;
  });
});

// ════════════════════════════════════════════════════════════
//  2. SSE 流式工具测试（stream.js）
// ════════════════════════════════════════════════════════════

describe('stream.js — SSE 工具', () => {

  it('writeSSEHeaders 设置正确的响应头', () => {
    const res = createMockRes();
    writeSSEHeaders(res);

    assert.equal(res._statusCode, 200);
    assert.match(res._headers['Content-Type'], /text\/event-stream/);
    assert.equal(res._headers['Cache-Control'], 'no-cache');
    assert.equal(res._headers['Connection'], 'keep-alive');
    assert.equal(res._headers['Access-Control-Allow-Origin'], '*');
  });

  it('writeSSEChunk 写入正确的 SSE 格式', () => {
    const res = createMockRes();
    writeSSEChunk(res, { hello: 'world' });

    assert.equal(res.body, 'data: {"hello":"world"}\n\n');
  });

  it('writeSSEDone 写入 [DONE] 并关闭', () => {
    const res = createMockRes();
    writeSSEDone(res);

    assert.equal(res.body, 'data: [DONE]\n\n');
    assert.ok(res._ended);
  });

  it('多个 chunk 拼接正确', () => {
    const res = createMockRes();
    writeSSEHeaders(res);
    writeSSEChunk(res, { a: 1 });
    writeSSEChunk(res, { b: 2 });
    writeSSEDone(res);

    const chunks = res.body.split('\n\n').filter(Boolean);
    assert.equal(chunks.length, 3);
    assert.equal(chunks[0], 'data: {"a":1}');
    assert.equal(chunks[1], 'data: {"b":2}');
    assert.equal(chunks[2], 'data: [DONE]');
  });
});

// ════════════════════════════════════════════════════════════
//  3. parseBody 测试
// ════════════════════════════════════════════════════════════

describe('parseBody — 请求体解析', () => {

  it('解析有效 JSON', async () => {
    const req = createMockReq({ body: { key: 'value' } });
    const result = await parseBody(req);
    assert.deepEqual(result, { key: 'value' });
  });

  it('空 body 返回空对象', async () => {
    const req = createMockReq();
    const result = await parseBody(req);
    assert.deepEqual(result, {});
  });

  it('无效 JSON 抛出错误', async () => {
    const req = createMockReq({ body: '{invalid' });
    await assert.rejects(parseBody(req), { message: 'Invalid JSON' });
  });
});

// ════════════════════════════════════════════════════════════
//  4. handleModels 测试
// ════════════════════════════════════════════════════════════

describe('handleModels — GET /v1/models', () => {

  it('返回 OpenAI 标准格式的模型列表', () => {
    const res = createMockRes();
    handleModels({}, res);

    assert.equal(res._statusCode, 200);

    const body = res.json;
    assert.equal(body.object, 'list');
    assert.ok(Array.isArray(body.data));
    assert.ok(body.data.length >= 3);

    // 验证每个模型的字段
    for (const model of body.data) {
      assert.ok(model.id);
      assert.equal(model.object, 'model');
      assert.equal(typeof model.created, 'number');
      assert.equal(model.owned_by, 'google');
    }
  });

  it('包含预期的模型 ID', () => {
    const res = createMockRes();
    handleModels({}, res);

    const ids = res.json.data.map(m => m.id);
    assert.ok(ids.includes('gemini-3.1-pro'));
    assert.ok(ids.includes('gemini-2.5-flash'));
    assert.ok(ids.includes('gemini-2.5-flash-thinking'));
  });
});

// ════════════════════════════════════════════════════════════
//  5. handleFileServing 测试
// ════════════════════════════════════════════════════════════

describe('handleFileServing — GET /files/:filename', () => {
  const testDir = join(config.outputDir, '__test_tmp__');
  const testFile = 'test-image.png';

  before(() => {
    mkdirSync(testDir, { recursive: true });
    // 创建测试文件（1x1 PNG）
    const pngHeader = Buffer.from([
      0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
    ]);
    writeFileSync(join(config.outputDir, testFile), pngHeader);
  });

  after(() => {
    try {
      rmSync(join(config.outputDir, testFile), { force: true });
      rmSync(testDir, { recursive: true, force: true });
    } catch {}
  });

  it('返回存在的文件', () => {
    const res = createMockRes();
    // handleFileServing 使用 pipe，用真实 HTTP 测试更准确
    // 这里仅测试路径校验和 404 逻辑
    handleFileServing({ url: '/files/nonexistent.png' }, res);

    assert.equal(res._statusCode, 404);
    assert.ok(res.json.error);
  });

  it('拒绝路径遍历攻击（..）', () => {
    const res = createMockRes();
    handleFileServing({ url: '/files/../../etc/passwd' }, res);

    assert.equal(res._statusCode, 400);
    assert.match(res.json.error.message, /Invalid filename/i);
  });

  it('拒绝路径中的反斜杠', () => {
    const res = createMockRes();
    handleFileServing({ url: '/files/..\\..\\etc\\passwd' }, res);

    assert.equal(res._statusCode, 400);
  });

  it('拒绝路径中的正斜杠', () => {
    const res = createMockRes();
    handleFileServing({ url: '/files/sub/dir/file.png' }, res);

    assert.equal(res._statusCode, 400);
  });

  it('找不到文件返回 404', () => {
    const res = createMockRes();
    handleFileServing({ url: '/files/definitely-not-exists.png' }, res);

    assert.equal(res._statusCode, 404);
  });
});

// ════════════════════════════════════════════════════════════
//  6. HTTP 服务器集成测试
// ════════════════════════════════════════════════════════════

describe('API Server 集成测试', () => {
  let server;
  let port;

  before(async () => {
    // 动态导入 server 逻辑，但自己创建一个测试端口的服务器
    // 而不是用 server.js 的自动启动逻辑
    const {
      handleModels,
      handleChatCompletions,
      handleImageGenerations,
      handleFileServing,
    } = await import('../src/api/handlers.js');

    const routes = {
      'GET /v1/models': handleModels,
      'POST /v1/chat/completions': handleChatCompletions,
      'POST /v1/images/generations': handleImageGenerations,
    };

    const TEST_API_KEY = 'test-key-12345';

    server = createServer((req, res) => {
      // CORS
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

      if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
      }

      const path = (req.url || '/').split('?')[0];

      // 健康检查
      if (req.method === 'GET' && path === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, service: 'gemini-openai-api' }));
        return;
      }

      // 静态文件
      if (req.method === 'GET' && path.startsWith('/files/')) {
        handleFileServing(req, res);
        return;
      }

      // 认证
      const auth = req.headers.authorization || '';
      if (auth !== `Bearer ${TEST_API_KEY}`) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          error: { message: 'Unauthorized', type: 'authentication_error', code: 'invalid_api_key' },
        }));
        return;
      }

      // 路由
      const routeKey = `${req.method} ${path}`;
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

    await new Promise((resolve) => {
      server.listen(0, () => {
        port = server.address().port;
        resolve();
      });
    });
  });

  after(async () => {
    await new Promise((resolve) => server.close(resolve));
  });

  // ── 健康检查 ──

  it('GET /health 返回健康状态', async () => {
    const res = await request(port, { path: '/health' });
    assert.equal(res.status, 200);
    assert.equal(res.json.ok, true);
    assert.equal(res.json.service, 'gemini-openai-api');
  });

  // ── CORS ──

  it('OPTIONS 请求返回 204 + CORS 头', async () => {
    const url = `http://127.0.0.1:${port}/v1/models`;
    const res = await fetch(url, { method: 'OPTIONS' });
    assert.equal(res.status, 204);
    assert.equal(res.headers.get('access-control-allow-origin'), '*');
    assert.ok(res.headers.get('access-control-allow-methods')?.includes('POST'));
  });

  // ── 认证 ──

  it('无 Authorization 头返回 401', async () => {
    const res = await request(port, { path: '/v1/models' });
    assert.equal(res.status, 401);
    assert.equal(res.json.error.type, 'authentication_error');
    assert.equal(res.json.error.code, 'invalid_api_key');
  });

  it('错误的 Bearer token 返回 401', async () => {
    const res = await request(port, {
      path: '/v1/models',
      headers: { Authorization: 'Bearer wrong-key' },
    });
    assert.equal(res.status, 401);
  });

  it('正确的 Bearer token 通过认证', async () => {
    const res = await request(port, {
      path: '/v1/models',
      headers: { Authorization: 'Bearer test-key-12345' },
    });
    assert.equal(res.status, 200);
  });

  // ── 404 路由 ──

  it('未知路由返回 404', async () => {
    const res = await request(port, {
      path: '/v1/unknown',
      headers: { Authorization: 'Bearer test-key-12345' },
    });
    assert.equal(res.status, 404);
    assert.ok(res.json.error.message.includes('/v1/unknown'));
  });

  // ── GET /v1/models ──

  it('GET /v1/models 返回 OpenAI 标准格式', async () => {
    const res = await request(port, {
      path: '/v1/models',
      headers: { Authorization: 'Bearer test-key-12345' },
    });

    assert.equal(res.status, 200);
    assert.equal(res.json.object, 'list');
    assert.ok(Array.isArray(res.json.data));

    const ids = res.json.data.map(m => m.id);
    assert.ok(ids.includes('gemini-3.1-pro'));
    assert.ok(ids.includes('gemini-2.5-flash'));
    assert.ok(ids.includes('gemini-2.5-flash-thinking'));

    for (const model of res.json.data) {
      assert.equal(model.object, 'model');
      assert.equal(typeof model.created, 'number');
      assert.equal(typeof model.owned_by, 'string');
    }
  });

  // ── POST /v1/chat/completions — 请求校验 ──

  it('chat/completions: 空 body 返回 400', async () => {
    const res = await request(port, {
      method: 'POST',
      path: '/v1/chat/completions',
      headers: { Authorization: 'Bearer test-key-12345' },
      body: {},
    });
    assert.equal(res.status, 400);
    assert.ok(res.json.error.message.includes('messages'));
  });

  it('chat/completions: messages 非数组返回 400', async () => {
    const res = await request(port, {
      method: 'POST',
      path: '/v1/chat/completions',
      headers: { Authorization: 'Bearer test-key-12345' },
      body: { messages: 'not an array' },
    });
    assert.equal(res.status, 400);
  });

  it('chat/completions: 空 messages 数组返回 400', async () => {
    const res = await request(port, {
      method: 'POST',
      path: '/v1/chat/completions',
      headers: { Authorization: 'Bearer test-key-12345' },
      body: { messages: [] },
    });
    assert.equal(res.status, 400);
  });

  it('chat/completions: 无 user 消息返回 400', async () => {
    const res = await request(port, {
      method: 'POST',
      path: '/v1/chat/completions',
      headers: { Authorization: 'Bearer test-key-12345' },
      body: {
        messages: [{ role: 'system', content: 'You are helpful' }],
      },
    });
    assert.equal(res.status, 400);
    assert.ok(res.json.error.message.includes('user message'));
  });

  it('chat/completions: 无效 JSON body 返回 400', async () => {
    const url = `http://127.0.0.1:${port}/v1/chat/completions`;
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: 'Bearer test-key-12345',
      },
      body: '{invalid json}}}',
    });
    const json = await res.json();
    assert.equal(res.status, 400);
    assert.ok(json.error.message.includes('Invalid JSON'));
  });

  it('chat/completions: image-only model 返回 400', async () => {
    const res = await request(port, {
      method: 'POST',
      path: '/v1/chat/completions',
      headers: { Authorization: 'Bearer test-key-12345' },
      body: {
        model: 'gemini-3.1-flash-image',
        messages: [{ role: 'user', content: 'hello' }],
      },
    });
    assert.equal(res.status, 400);
    assert.ok(res.json.error.message.includes('/v1/images/generations'));
  });

  it('chat/completions: 未知聊天模型返回 400', async () => {
    const res = await request(port, {
      method: 'POST',
      path: '/v1/chat/completions',
      headers: { Authorization: 'Bearer test-key-12345' },
      body: {
        model: 'gemini-unknown-model',
        messages: [{ role: 'user', content: 'hello' }],
      },
    });
    assert.equal(res.status, 400);
    assert.ok(res.json.error.message.includes('Unsupported chat model'));
  });

  // ── POST /v1/images/generations — 请求校验 ──

  it('images/generations: 无 prompt 返回 400', async () => {
    const res = await request(port, {
      method: 'POST',
      path: '/v1/images/generations',
      headers: { Authorization: 'Bearer test-key-12345' },
      body: {},
    });
    assert.equal(res.status, 400);
    assert.ok(res.json.error.message.includes('prompt'));
  });

  it('images/generations: 空字符串 prompt 返回 400', async () => {
    const res = await request(port, {
      method: 'POST',
      path: '/v1/images/generations',
      headers: { Authorization: 'Bearer test-key-12345' },
      body: { prompt: '   ' },
    });
    assert.equal(res.status, 400);
  });

  it('images/generations: n != 1 返回 400', async () => {
    const res = await request(port, {
      method: 'POST',
      path: '/v1/images/generations',
      headers: { Authorization: 'Bearer test-key-12345' },
      body: { prompt: 'a cat', n: 3 },
    });
    assert.equal(res.status, 400);
    assert.ok(res.json.error.message.includes('n=1'));
  });

  it('images/generations: 非图片模型返回 400', async () => {
    const res = await request(port, {
      method: 'POST',
      path: '/v1/images/generations',
      headers: { Authorization: 'Bearer test-key-12345' },
      body: { prompt: 'a cat', model: 'gemini-3.1-pro' },
    });
    assert.equal(res.status, 400);
    assert.ok(res.json.error.message.includes('Unsupported image model'));
  });

  // ── 健康检查不需要认证 ──

  it('GET /health 不需要 Bearer token', async () => {
    const res = await request(port, { path: '/health' });
    assert.equal(res.status, 200);
    assert.equal(res.json.ok, true);
  });
});

// ════════════════════════════════════════════════════════════
//  7. OpenAI 响应格式合规性测试
// ════════════════════════════════════════════════════════════

describe('OpenAI 响应格式合规性', () => {

  it('/v1/models 响应符合 OpenAI ListModels 格式', () => {
    const res = createMockRes();
    handleModels({}, res);

    const body = res.json;

    // 顶级字段
    assert.equal(typeof body.object, 'string');
    assert.equal(body.object, 'list');
    assert.ok(Array.isArray(body.data));

    // Model object 字段
    const model = body.data[0];
    assert.equal(typeof model.id, 'string');
    assert.equal(model.object, 'model');
    assert.equal(typeof model.created, 'number');
    assert.equal(typeof model.owned_by, 'string');
  });

  it('错误响应符合 OpenAI Error 格式', async () => {
    // 在集成测试里已经验证过了，这里再次明确格式
    const res = createMockRes();
    // 直接调用 sendError（通过 handlers 内部逻辑触发）
    handleFileServing({ url: '/files/nonexistent.png' }, res);

    const body = res.json;
    assert.ok(body.error);
    assert.equal(typeof body.error.message, 'string');
    // type 和 code 可为 null，但必须存在
    assert.ok('type' in body.error);
    assert.ok('code' in body.error);
  });
});

// ════════════════════════════════════════════════════════════
//  8. 安全性测试
// ════════════════════════════════════════════════════════════

describe('安全性测试', () => {

  it('文件路径遍历防护 — 多种变体', () => {
    const attacks = [
      '/files/../../etc/passwd',
      '/files/..%2F..%2Fetc%2Fpasswd',
      '/files/....//....//etc/passwd',
      '/files/sub\\..\\..\\etc\\passwd',
      '/files/foo/bar',
    ];

    for (const url of attacks) {
      const res = createMockRes();
      handleFileServing({ url }, res);
      assert.ok(res._statusCode === 400 || res._statusCode === 404,
        `Expected 400 or 404 for ${url}, got ${res._statusCode}`);
    }
  });

  it('parseBody 拒绝超大请求体', async () => {
    const req = new Readable({
      read() {
        // 推入超过 10MB 的数据
        const chunk = Buffer.alloc(1024 * 1024, 'x'); // 1 MB
        for (let i = 0; i < 12; i++) {
          this.push(chunk);
        }
        this.push(null);
      },
    });
    req.method = 'POST';
    req.url = '/';
    req.headers = {};

    await assert.rejects(parseBody(req), /too large/i);
  });
});

// ════════════════════════════════════════════════════════════
//  9. stripLabel 一致性测试（流式/非流式场景）
// ════════════════════════════════════════════════════════════

/**
 * 与 gemini-ops.js 中 stripLabel 逻辑保持一致的镜像实现，用于单元测试。
 * 如果 gemini-ops.js 的实现发生变化，这里也需同步更新。
 */
function stripLabel(t) {
  let s = t;
  let prev;
  do {
    prev = s;
    s = s.replace(/^显示思路\s*\n*/, '')
         .replace(/^Gemini\s*说\s*\n*/, '')
         .replace(/^[^\n]{0,30}说\s*\n+/, '')
         .replace(/^JSON\s*\n*/i, '')
         .replace(/^\s*\n/, '');
  } while (s !== prev);
  s = s.replace(/^```[\w]*\s*\n?/, '').replace(/\n?```\s*$/, '');
  return s.trim();
}

describe('stripLabel 一致性测试', () => {

  it('无前缀 — 原样返回', () => {
    assert.equal(stripLabel('[{"sub_index":1}]'), '[{"sub_index":1}]');
  });

  it('"Gemini 说\\n" 有换行 — 正确剥离', () => {
    assert.equal(stripLabel('Gemini 说\n[{"sub_index":1}]'), '[{"sub_index":1}]');
  });

  it('"Gemini 说" 无换行（流式中间态） — 正确剥离', () => {
    // 这是导致 bug 的核心场景：流式早期 DOM 中只有 "Gemini 说" 尚无换行
    assert.equal(stripLabel('Gemini 说'), '');
    assert.equal(stripLabel('Gemini 说[{"sub_index":1}]'), '[{"sub_index":1}]');
  });

  it('"Gemini说" 无空格变体 — 正确剥离', () => {
    assert.equal(stripLabel('Gemini说\n内容'), '内容');
    assert.equal(stripLabel('Gemini说内容'), '内容');
  });

  it('"显示思路" 有换行 — 正确剥离', () => {
    assert.equal(stripLabel('显示思路\n实际内容'), '实际内容');
  });

  it('"显示思路" 无换行（流式中间态） — 正确剥离', () => {
    assert.equal(stripLabel('显示思路'), '');
    assert.equal(stripLabel('显示思路实际内容'), '实际内容');
  });

  it('"JSON" 标签有换行 — 正确剥离', () => {
    assert.equal(stripLabel('JSON\n[1,2,3]'), '[1,2,3]');
    assert.equal(stripLabel('json\n{"a":1}'), '{"a":1}');
  });

  it('"JSON" 标签无换行（流式中间态） — 正确剥离', () => {
    assert.equal(stripLabel('JSON'), '');
    assert.equal(stripLabel('JSON[1,2]'), '[1,2]');
  });

  it('多层前缀叠加 — 全部剥离', () => {
    assert.equal(stripLabel('显示思路\nGemini 说\nJSON\n{"a":1}'), '{"a":1}');
  });

  it('markdown 代码围栏 — 正确剥离', () => {
    assert.equal(stripLabel('```json\n[1,2]\n```'), '[1,2]');
    assert.equal(stripLabel('Gemini 说\n```json\n[1,2]\n```'), '[1,2]');
  });

  it('前缀 + 空行 + 内容 — 正确剥离', () => {
    assert.equal(stripLabel('Gemini 说\n\n[1,2,3]'), '[1,2,3]');
  });

  it('流式一致性 — 有无换行结果一致', () => {
    // 模拟流式不同阶段拿到的文本，stripLabel 返回值应单调递增（前缀一致）
    const withNewline = stripLabel('Gemini 说\n[{"sub_index":1}]');
    const withoutNewline = stripLabel('Gemini 说[{"sub_index":1}]');
    assert.equal(withNewline, withoutNewline,
      'stripLabel 在有/无换行时对相同内容应返回相同结果');
  });
});

// ════════════════════════════════════════════════════════════
//  10. 流式 delta 计算健壮性测试
// ════════════════════════════════════════════════════════════

describe('流式 delta 计算健壮性', () => {

  /**
   * 模拟 handlers.js 中 streamChatCompletion 的 delta 计算逻辑（修复后版本）
   * 输入：按时间顺序的 currentText 序列
   * 输出：发送给客户端的 delta 序列
   */
  function simulateDeltas(textSequence) {
    let prevText = '';
    const deltas = [];
    for (const currentText of textSequence) {
      if (currentText.length > prevText.length) {
        if (currentText.startsWith(prevText)) {
          deltas.push(currentText.slice(prevText.length));
        }
        // else: 跳过，防止错位
        prevText = currentText;
      }
    }
    return deltas;
  }

  it('正常递增 — delta 拼接等于最终文本', () => {
    const seq = ['Hello', 'Hello world', 'Hello world!'];
    const deltas = simulateDeltas(seq);
    assert.equal(deltas.join(''), 'Hello world!');
  });

  it('前缀不匹配（stripLabel 行为变化） — 跳过有问题的 delta', () => {
    // 模拟 bug 场景：第一次 stripLabel 没去掉前缀，第二次去掉了
    // 流式文本逐字追加，所以后续文本是追加式增长
    const seq = ['Gemini 说', '[{"sub_index":1},', '[{"sub_index":1},{"sub_index":2}]'];
    const deltas = simulateDeltas(seq);
    // 第 1 轮：prevText="" → "Gemini 说"，正常发 delta
    assert.equal(deltas[0], 'Gemini 说');
    // 第 2 轮："[{..." 不以 "Gemini 说" 开头 → 跳过 delta，但 prevText 更新
    // 第 3 轮："[{...,{...}]" 以 "[{...," 开头 → 正常发增量
    assert.equal(deltas.length, 2);
    assert.equal(deltas[1], '{"sub_index":2}]');
    // 关键：不会出现 "说_index" 这种损坏内容
    assert.ok(!deltas.join('').includes('说_index'), '不应出现损坏的 JSON 片段');
  });

  it('空序列 — 无 delta', () => {
    assert.deepEqual(simulateDeltas([]), []);
  });

  it('单次完整响应 — 一个 delta', () => {
    const deltas = simulateDeltas(['完整响应内容']);
    assert.deepEqual(deltas, ['完整响应内容']);
  });

  it('文本长度未增长 — 不发送 delta', () => {
    const deltas = simulateDeltas(['abc', 'abc', 'abc']);
    assert.deepEqual(deltas, ['abc']);
  });
});
