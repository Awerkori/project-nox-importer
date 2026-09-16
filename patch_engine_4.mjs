import fs from 'fs';
let engine = fs.readFileSync('src/core/engine.ts', 'utf8');

engine = engine.replace(
  /classification.retryClass === 'NETWORK_TRANSIENT'/g,
  "classification.retryClass === 'QUEUE_RETRY_TIMEOUT'"
);

fs.writeFileSync('src/core/engine.ts', engine);
