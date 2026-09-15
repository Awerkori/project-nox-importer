import WebSocket from 'ws';
const wsUrl = 'ws://127.0.0.1:9222/devtools/page/6CAB873D04D27E901617315CBB06DB3F';
const ws = new WebSocket(wsUrl);
let msgId = 1;
function send(method, params = {}) {
  return new Promise((resolve) => {
    const id = msgId++;
    const listener = (data) => {
      const res = JSON.parse(data);
      if (res.id === id) { ws.off('message', listener); resolve(res.result); }
    };
    ws.on('message', listener);
    ws.send(JSON.stringify({ id, method, params }));
  });
}
ws.on('open', async () => {
  const q = `
    CREATE TABLE IF NOT EXISTS debug_rpc (text text);
    TRUNCATE debug_rpc;
    INSERT INTO debug_rpc (text) SELECT pg_get_functiondef('public.importer_acquire_job'::regproc);
  `;
  await send('Runtime.evaluate', { expression: `
    (function() {
      const monaco = window.monaco;
      if (monaco) monaco.editor.getModels()[0].setValue(${JSON.stringify(q)});
    })()
  `});
  await new Promise(r => setTimeout(r, 500));
  await send('Runtime.evaluate', { expression: `
    (function() {
      const btns = Array.from(document.querySelectorAll('button'));
      const runBtn = btns.find(b => b.textContent === 'Run' || b.textContent.includes('Run'));
      if (runBtn) runBtn.click();
    })()
  `});
  setTimeout(() => process.exit(0), 3000);
});
