/**
 * logger.js — OpenAI 兼容 API 文件日志
 *
 * 目标：
 * 1. 记录请求/响应摘要，便于排查接口问题
 * 2. 自动脱敏（Authorization、api key、token、base64 大字段）
 * 3. 日志按天落盘，JSONL 格式，便于后续 grep/分析
 */
import { mkdirSync } from 'node:fs';
import { appendFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import config from '../config.js';

const REDACT_KEYS = new Set([
  'authorization',
  'api_key',
  'apikey',
  'token',
  'access_token',
  'refresh_token',
  'password',
  'secret',
  'b64_json',
  'dataurl',
]);

function isoDate(d = new Date()) {
  return d.toISOString().slice(0, 10);
}

function shortId() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function truncString(str, max) {
  if (typeof str !== 'string') return str;
  if (str.length <= max) return str;
  return `${str.slice(0, max)}...[truncated:${str.length - max}]`;
}

function sanitizeValue(value, maxString, depth = 0) {
  if (value == null) return value;
  if (depth >= 6) return '[depth_limited]';

  if (typeof value === 'string') {
    return truncString(value, maxString);
  }

  if (Array.isArray(value)) {
    const limit = 30;
    const arr = value.slice(0, limit).map((v) => sanitizeValue(v, maxString, depth + 1));
    if (value.length > limit) arr.push(`[truncated_items:${value.length - limit}]`);
    return arr;
  }

  if (typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      const lk = k.toLowerCase();
      if (REDACT_KEYS.has(lk)) {
        out[k] = '[redacted]';
        continue;
      }

      // OpenAI messages 中可能有超长 content，做结构化摘要
      if (lk === 'messages' && Array.isArray(v)) {
        out[k] = v.slice(0, 20).map((m) => {
          const role = m?.role || 'unknown';
          const content = m?.content;
          if (typeof content === 'string') {
            return { role, content: truncString(content, Math.min(maxString, 400)) };
          }
          if (Array.isArray(content)) {
            return {
              role,
              content: content.slice(0, 10).map((p) => {
                if (p?.type === 'text') {
                  return { type: 'text', text: truncString(String(p.text || ''), 240) };
                }
                if (p?.type === 'image_url') {
                  return { type: 'image_url', image_url: '[redacted_or_url]' };
                }
                return { type: p?.type || 'unknown' };
              }),
            };
          }
          return { role, content: '[unsupported_content_type]' };
        });
        continue;
      }

      out[k] = sanitizeValue(v, maxString, depth + 1);
    }
    return out;
  }

  return value;
}

export function sanitizeForLog(payload, maxString = config.apiLogMaxString) {
  return sanitizeValue(payload, maxString);
}

class ApiFileLogger {
  constructor() {
    this.enabled = !!config.apiLogEnabled;
    this.maxString = config.apiLogMaxString;
    this.dir = resolve(config.apiLogDir);
    this._queue = Promise.resolve();

    if (this.enabled) {
      mkdirSync(this.dir, { recursive: true });
    }
  }

  nextRequestId() {
    return `req_${shortId()}`;
  }

  log(event, data = {}) {
    if (!this.enabled) return;

    const record = {
      ts: new Date().toISOString(),
      event,
      ...sanitizeForLog(data, this.maxString),
    };

    const line = `${JSON.stringify(record)}\n`;
    const filePath = join(this.dir, `${isoDate()}.log`);

    this._queue = this._queue
      .then(() => appendFile(filePath, line, 'utf-8'))
      .catch((err) => {
        // 不让日志失败影响主流程
        console.error('[api][logger] write failed:', err?.message || err);
      });
  }
}

export const apiLogger = new ApiFileLogger();
