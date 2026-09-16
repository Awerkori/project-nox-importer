import fs from 'fs';
let code = fs.readFileSync('src/core/engine.ts', 'utf8');
code = code.replace(/const requestedMax = 32;/, 'const requestedMax = 6;');
code = code.replace(/initialConcurrency: 32,/, 'initialConcurrency: Math.min(4, requestedMax),');
code = code.replace(/maxRssMb: 800,/, 'maxRssMb: 350,');
code = code.replace(/maxHeapMb: 400,/, 'maxHeapMb: 200,');
code = code.replace(/maxExternalAndBuffersMb: 300,/, 'maxExternalAndBuffersMb: 100,');
fs.writeFileSync('src/core/engine.ts', code);
