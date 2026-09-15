import WebSocket from 'ws';
import fs from 'fs';

const sql = fs.readFileSync('/home/awerkori/.Projects/project-nox-importer/migrations/20260914230000_queue_fairness.sql', 'utf8');

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
  console.log("Connected to Supabase tab via CDP");
  
  console.log("Injecting SQL...");
  const escapedSql = JSON.stringify(sql);
  
  await send('Runtime.evaluate', {
    expression: `
      (function() {
        const sqlText = ${escapedSql};
        const monaco = window.monaco;
        if (monaco && monaco.editor && monaco.editor.getModels().length > 0) {
          monaco.editor.getModels()[0].setValue(sqlText);
          return true;
        }
        return false;
      })()
    `,
    returnByValue: true
  });
  
  await new Promise(r => setTimeout(r, 1000));
  
  console.log("Clicking Run...");
  await send('Runtime.evaluate', {
    expression: `
      (function() {
        const btns = Array.from(document.querySelectorAll('button'));
        const runBtn = btns.find(b => b.textContent === 'Run' || b.textContent.includes('Run'));
        if (runBtn) {
          runBtn.click();
          return true;
        }
        return false;
      })()
    `,
    returnByValue: true
  });

  console.log("Waiting for execution...");
  await new Promise(r => setTimeout(r, 4000));
  
  // Get text of any error alerts or results
  const resultText = await send('Runtime.evaluate', {
    expression: `
      (function() {
        return document.body.innerText;
      })()
    `,
    returnByValue: true
  });
  
  fs.writeFileSync('supabase-page-text.txt', resultText.value?.result?.value || resultText.value || 'Error reading text');
  console.log("Done. Check supabase-page-text.txt.");
  process.exit(0);
});
