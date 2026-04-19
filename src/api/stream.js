/**
 * stream.js — SSE (Server-Sent Events) 流式响应工具
 *
 * 提供 OpenAI 兼容的 SSE 流式输出能力。
 */

/**
 * 设置 SSE 响应头并开始流式传输
 * @param {import('node:http').ServerResponse} res
 */
export function writeSSEHeaders(res) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'X-Accel-Buffering': 'no', // 禁用 Nginx 缓冲
  });
}

/**
 * 发送一个 SSE 数据块
 * @param {import('node:http').ServerResponse} res
 * @param {object} data - 将被 JSON.stringify 的数据对象
 */
export function writeSSEChunk(res, data) {
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

/**
 * 发送 SSE 结束标记并关闭连接
 * @param {import('node:http').ServerResponse} res
 */
export function writeSSEDone(res) {
  res.write('data: [DONE]\n\n');
  res.end();
}
