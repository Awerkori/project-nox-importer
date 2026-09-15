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
  
  // 1. Inject SQL into Monaco editor
  console.log("Injecting SQL...");
  await send('Runtime.evaluate', {
    expression: `
      (function() {
        if (window.monaco && window.monaco.editor && window.monaco.editor.getModels().length > 0) {
          window.monaco.editor.getModels()[0].setValue(\`${sql.replace(/`/g, '\\`').replace(/\$/g, '\\$')}\`);
          return true;
        }
        return false;
      })()
    `,
    returnByValue: true
  });
  
  await new Promise(r => setTimeout(r, 1000));
  
  // 2. Click the editor to focus it
  console.log("Focusing editor...");
  // Instead of finding coordinates, we can just trigger a click via JS
  await send('Runtime.evaluate', {
    expression: `
      (function() {
        const el = document.querySelector('.monaco-editor');
        if (el) el.click();
      })()
    `
  });
  
  await new Promise(r => setTimeout(r, 500));
  
  // 3. Send Ctrl+Enter using CDP Input domain
  console.log("Pressing Ctrl+Enter...");
  
  // modifier 2 = Control
  await send('Input.dispatchKeyEvent', {
    type: 'rawKeyDown',
    windowsVirtualKeyCode: 17, // Control
    nativeVirtualKeyCode: 17,
    macCharCode: 0,
    modifiers: 2
  });
  
  await send('Input.dispatchKeyEvent', {
    type: 'keyDown',
    windowsVirtualKeyCode: 13, // Enter
    nativeVirtualKeyCode: 13,
    macCharCode: 13,
    modifiers: 2,
    text: '\r'
  });
  
  await send('Input.dispatchKeyEvent', {
    type: 'keyUp',
    windowsVirtualKeyCode: 13,
    nativeVirtualKeyCode: 13,
    modifiers: 2
  });
  
  await send('Input.dispatchKeyEvent', {
    type: 'keyUp',
    windowsVirtualKeyCode: 17,
    nativeVirtualKeyCode: 17,
    modifiers: 0
  });

  // Also try clicking the "Run" button if Ctrl+Enter didn't work
  // The run button usually says "Run"
  await send('Runtime.evaluate', {
    expression: `
      (function() {
        const btns = Array.from(document.querySelectorAll('button'));
        const runBtn = btns.find(b => b.textContent.includes('Run'));
        if (runBtn) runBtn.click();
      })()
    `
  });

  console.log("Waiting for execution...");
  await new Promise(r => setTimeout(r, 4000));
  console.log("Done.");
  process.exit(0);
});
