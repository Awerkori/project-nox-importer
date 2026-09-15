import fs from 'fs';

let engineCode = fs.readFileSync('src/core/engine.ts', 'utf-8');
engineCode = engineCode.replace(
  /const uploadConcurrency = \d+;/,
  'const uploadConcurrency = Math.min(6, Math.max(2, Math.floor(this.autotuner.getCurrentConcurrency() / 2)));'
);
fs.writeFileSync('src/core/engine.ts', engineCode, 'utf-8');

let concCode = fs.readFileSync('src/core/concurrency.ts', 'utf-8');
concCode = concCode.replace(
  /this\.globalMediaSemaphore = new AsyncSemaphore\(\d+\);/,
  'this.globalMediaSemaphore = new AsyncSemaphore(12); // Safe bounded concurrent image uploads'
);
fs.writeFileSync('src/core/concurrency.ts', concCode, 'utf-8');

console.log("Restored safe defaults.");
