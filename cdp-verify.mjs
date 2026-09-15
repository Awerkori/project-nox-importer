import WebSocket from 'ws';

const wsUrl = 'ws://127.0.0.1:9222/devtools/page/6CAB873D04D27E901617315CBB06DB3F';
const ws = new WebSocket(wsUrl);

let msgId = 1;
function send(method, params = {}) {
  return new Promise((resolve) => {
    const id = msgId++;
    const listener = (data) => {
      const res = JSON.parse(data);
      if (res.id === id) {
        ws.off('message', listener);
        resolve(res.result);
      }
    };
    ws.on('message', listener);
    ws.send(JSON.stringify({ id, method, params }));
  });
}

ws.on('open', async () => {
  const q = "SELECT pg_get_functiondef('public.importer_acquire_job'::regproc);";
  
  await send('Runtime.evaluate', {
    expression: `
      (function() {
        const monaco = window.monaco;
        if (monaco && monaco.editor && monaco.editor.getModels().length > 0) {
          monaco.editor.getModels()[0].setValue(${JSON.stringify(q)});
        }
      })()
    `
  });
  
  await new Promise(r => setTimeout(r, 500));
  
  await send('Runtime.evaluate', {
    expression: `
      (function() {
        const btns = Array.from(document.querySelectorAll('button'));
        const runBtn = btns.find(b => b.textContent === 'Run' || b.textContent.includes('Run'));
        if (runBtn) runBtn.click();
      })()
    `
  });

  await new Promise(r => setTimeout(r, 2000));
  
  // Scrape the result grid!
  const res = await send('Runtime.evaluate', {
    expression: `
      (function() {
        const cells = Array.from(document.querySelectorAll('[role="gridcell"]'));
        return cells.map(c => c.textContent).join('\\n');
      })()
    `,
    returnByValue: true
  });
  
  console.log("RESULT:");
  console.log(res.value);
  process.exit(0);
});
