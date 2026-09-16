import fs from 'fs';
let code = fs.readFileSync('src/core/concurrency.ts', 'utf8');
code = code.replace(/maxConcurrency: \d+,/g, 'maxConcurrency: 32,');
code = code.replace(/maxRssMb: \d+,/g, 'maxRssMb: 800,');
code = code.replace(/maxHeapMb: \d+,/g, 'maxHeapMb: 400,');
code = code.replace(/maxExternalAndBuffersMb: \d+,/g, 'maxExternalAndBuffersMb: 300,');
code = code.replace(/initialConcurrency: \d+,/g, 'initialConcurrency: 32,');
fs.writeFileSync('src/core/concurrency.ts', code);
