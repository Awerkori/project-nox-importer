import fs from 'fs';
let engine = fs.readFileSync('src/core/engine.ts', 'utf8');

engine = engine.replace(
  /if \(classification === 'NETWORK_TRANSIENT'/g,
  "if (classification.retryClass === 'NETWORK_TRANSIENT'"
);

fs.writeFileSync('src/core/engine.ts', engine);
