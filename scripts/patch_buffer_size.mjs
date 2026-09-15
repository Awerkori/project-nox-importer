import fs from 'fs';
let content = fs.readFileSync('src/core/concurrency.ts', 'utf-8');

content = content.replace(
  'private bufferedPageSemaphore = new AsyncSemaphore(6);',
  'private bufferedPageSemaphore = new AsyncSemaphore(60);'
);
content = content.replace(
  'this.globalInflightRequestSemaphore = new AsyncSemaphore(16);',
  'this.globalInflightRequestSemaphore = new AsyncSemaphore(32);'
);

fs.writeFileSync('src/core/concurrency.ts', content, 'utf-8');
console.log("Buffered page and inflight semaphores patched");
