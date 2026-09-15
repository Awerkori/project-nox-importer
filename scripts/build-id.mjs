import { readFileSync, writeFileSync } from 'fs';
import { createHash } from 'crypto';
import { join } from 'path';

// read build/index.js to generate an artifact hash
try {
  const content = readFileSync(join(process.cwd(), 'build', 'index.js'));
  const hash = createHash('md5').update(content).digest('hex').substring(0, 8);
  writeFileSync(join(process.cwd(), 'build', 'BUILD_ID'), hash);
} catch (err) {
  console.error("Failed to generate BUILD_ID:", err);
  writeFileSync(join(process.cwd(), 'build', 'BUILD_ID'), 'unknown');
}
