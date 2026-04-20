import fs from 'node:fs';

async function testOnce(label, bodyFile, stream) {
  const bodyRaw = JSON.parse(fs.readFileSync(bodyFile, 'utf-8'));
  bodyRaw.stream = stream;
  const body = JSON.stringify(bodyRaw);

  if (stream) {
    const resp = await fetch('http://127.0.0.1:3000/v1/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer sk-110110' },
      body,
    });
    const text = await resp.text();
    const chunks = [];
    let done = false;
    for (const line of text.split('\n')) {
      if (line.startsWith('data: ')) {
        const data = line.slice(6);
        if (data === '[DONE]') { done = true; }
        else { try { chunks.push(JSON.parse(data)); } catch {} }
      }
    }
    // 拼接所有 delta content
    let fullContent = '';
    for (const chunk of chunks) {
      const delta = chunk.choices?.[0]?.delta;
      if (delta?.content) fullContent += delta.content;
    }
    console.log(`${label} (stream):`);
    console.log('  Chunks count:', chunks.length);
    console.log('  Done marker:', done);
    console.log('  Content starts with:', JSON.stringify(fullContent.substring(0, 100)));
    console.log('  Content ends with:', JSON.stringify(fullContent.substring(fullContent.length - 100)));
    const trimmed = fullContent.trim();
    console.log('  Has code fence:', trimmed.startsWith('```'));
    try {
      JSON.parse(fullContent);
      console.log('  Direct JSON parse: OK');
    } catch (e) {
      console.log('  Direct JSON parse: FAILED -', e.message);
      const stripped = trimmed.replace(/^```(?:json)?\s*\n?/, '').replace(/\n?\s*```$/, '');
      try {
        JSON.parse(stripped);
        console.log('  Stripped JSON parse: OK (code fence wrapped)');
      } catch (e2) {
        console.log('  Stripped JSON parse: ALSO FAILED');
        console.log('  First 300 chars:', fullContent.substring(0, 300));
      }
    }
  } else {
    const resp = await fetch('http://127.0.0.1:3000/v1/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer sk-110110' },
      body,
    });
    const json = await resp.json();
    if (json.error) {
      console.log(`${label}: ERROR -`, json.error.message);
      return;
    }
    const content = json.choices[0].message.content;
    console.log(`${label} (non-stream):`);
    console.log('  Content starts with:', JSON.stringify(content.substring(0, 100)));
    console.log('  Content ends with:', JSON.stringify(content.substring(content.length - 100)));
    const trimmed = content.trim();
    console.log('  Has code fence:', trimmed.startsWith('```'));
    try {
      JSON.parse(content);
      console.log('  Direct JSON parse: OK');
    } catch (e) {
      console.log('  Direct JSON parse: FAILED -', e.message);
      const stripped = trimmed.replace(/^```(?:json)?\s*\n?/, '').replace(/\n?\s*```$/, '');
      try {
        JSON.parse(stripped);
        console.log('  Stripped JSON parse: OK (code fence wrapped)');
      } catch (e2) {
        console.log('  Stripped JSON parse: ALSO FAILED');
        console.log('  First 300 chars:', content.substring(0, 300));
      }
    }
  }
  console.log('');
}

(async () => {
  // 非流式验证
  await testOnce('Non-stream', 'test/test-request.json', false);
  // 流式验证
  await testOnce('Stream', 'test/test-request.json', true);
})();
