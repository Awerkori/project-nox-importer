import fs from 'fs';
let code = fs.readFileSync('src/core/engine.ts', 'utf8');
code = code.replace(/const requestedMax = \d+;/, 'const requestedMax = 32;');
code = code.replace(/initialConcurrency: Math.min\(4, requestedMax\),/, 'initialConcurrency: 32,');
code = code.replace(/maxRssMb: \d+,/, 'maxRssMb: 800,');
code = code.replace(/maxHeapMb: \d+,/, 'maxHeapMb: 400,');
code = code.replace(/maxExternalAndBuffersMb: \d+,/, 'maxExternalAndBuffersMb: 300,');
fs.writeFileSync('src/core/engine.ts', code);
