// 验证流式响应内容完整性
const resp = await fetch('http://127.0.0.1:3000/v1/chat/completions', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json', Authorization: 'Bearer sk-110110' },
  body: JSON.stringify({
    model: 'gemini-3.1-pro',
    messages: [{ role: 'user', content: '返回JSON数组: [{"a":1},{"a":2}]，纯JSON无其他文字' }],
    stream: true,
  }),
});

const text = await resp.text();
let full = '';
const deltas = [];
for (const line of text.split('\n')) {
  if (line.startsWith('data: ') && line.slice(6) !== '[DONE]') {
    try {
      const c = JSON.parse(line.slice(6));
      const d = c.choices?.[0]?.delta?.content;
      if (d !== undefined && d !== '') {
        deltas.push(d);
        full += d;
      }
    } catch {}
  }
}

console.log('=== DELTAS ===');
deltas.forEach((d, i) => console.log(`  [${i}] ${JSON.stringify(d)}`));
console.log('\n=== FULL CONTENT ===');
console.log(full);
console.log('\n=== JSON PARSE ===');
try {
  JSON.parse(full);
  console.log('OK');
} catch (e) {
  console.log('FAILED:', e.message);
  // 尝试清理后解析
  const cleaned = full.trim().replace(/^```(?:json)?\s*\n?/, '').replace(/\n?\s*```$/, '');
  try {
    JSON.parse(cleaned);
    console.log('CLEANED OK (had code fence)');
  } catch {
    console.log('CLEANED ALSO FAILED');
  }
}
