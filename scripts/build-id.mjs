import { readFileSync, writeFileSync, readdirSync } from 'fs';
import { createHash } from 'crypto';
import { join } from 'path';
import { execFileSync } from 'node:child_process';

// read build/index.js to generate an artifact hash
try {
  const root = join(process.cwd(), 'build');
  const hash = createHash('sha256');
  function visit(dir) {
    for (const item of readdirSync(dir, { withFileTypes: true }).sort((a,b) => a.name.localeCompare(b.name))) {
      const file = join(dir, item.name);
      if (item.isDirectory()) visit(file);
      else if (item.name.endsWith('.js')) hash.update(file.slice(root.length)).update(readFileSync(file));
    }
  }
  visit(root);
  let gitSha = process.env.GITHUB_SHA || process.env.GIT_SHA || 'unknown';
  try { gitSha = execFileSync('git', ['rev-parse', 'HEAD'], {encoding:'utf8',stdio:['ignore','pipe','ignore']}).trim(); } catch {}
  writeFileSync(join(root, 'BUILD_ID'), JSON.stringify({gitSha, artifactSha256:hash.digest('hex'), builtAt:new Date().toISOString()}));
} catch (err) {
  console.error("Failed to generate BUILD_ID:", err);
  writeFileSync(join(process.cwd(), 'build', 'BUILD_ID'), 'unknown');
}
