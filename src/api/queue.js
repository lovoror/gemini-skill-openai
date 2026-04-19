/**
 * queue.js — 请求串行队列
 *
 * 浏览器同一时刻只能处理一个操作（一个 Gemini 页面、一个输入框），
 * 所有 API 请求必须排队依次执行。
 */

/** 最大排队数，超出返回 429 */
const MAX_PENDING = 10;

let _pending = 0;
let _chain = Promise.resolve();

/**
 * 将异步任务加入串行队列
 *
 * @template T
 * @param {() => Promise<T>} fn - 要执行的异步函数
 * @returns {Promise<T>}
 * @throws {{ status: 429 }} 当队列已满时
 */
export function enqueue(fn) {
  if (_pending >= MAX_PENDING) {
    const err = new Error('Too many requests queued');
    err.status = 429;
    return Promise.reject(err);
  }

  _pending++;

  const task = _chain
    .then(() => fn())
    .finally(() => { _pending--; });

  // 无论成功失败，链继续（不让前一个错误阻断后续任务）
  _chain = task.catch(() => {});

  return task;
}

/**
 * 当前排队中的请求数
 * @returns {number}
 */
export function pendingCount() {
  return _pending;
}
