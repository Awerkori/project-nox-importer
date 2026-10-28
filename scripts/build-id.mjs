import { readFileSync, writeFileSync, readdirSync, copyFileSync, existsSync, mkdirSync } from 'fs';
import { createHash } from 'crypto';
import { join } from 'path';
import { execSync } from 'child_process';

// Copy assets required at runtime
for (const [srcRel, destDirRel, fileName] of [
  ['src/sources/manhastro/manhastro-catalog.json', 'build/sources/manhastro', 'manhastro-catalog.json'],
  ['src/sources/nexus/nexus-catalog.json', 'build/sources/nexus', 'nexus-catalog.json'],
  ['src/sources/imperiodabritannia/imperiodabritannia-catalog.json', 'build/sources/imperiodabritannia', 'imperiodabritannia-catalog.json'],
  ['src/core/tessdata/por.traineddata', 'build/core/tessdata', 'por.traineddata'],
  ['src/core/tessdata/eng.traineddata', 'build/core/tessdata', 'eng.traineddata'],
]) {
  try {
    const srcPath = join(process.cwd(), srcRel);
    const destDir = join(process.cwd(), destDirRel);
    const destPath = join(destDir, fileName);
    if (existsSync(srcPath)) {
      if (!existsSync(destDir)) mkdirSync(destDir, { recursive: true });
      copyFileSync(srcPath, destPath);
      console.log(`Copied ${fileName} to ${destDirRel}/`);
    }
  } catch (err) {
    console.warn(`Failed to copy ${fileName}:`, err.message);
  }
}

// Hash the runnable JavaScript plus the DIScloud startup contract. A changed
// START environment or npm start script can alter the live process even when
// build/*.js is identical, so it must not share an artifact identity with
// the prior deploy.
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
  for (const deploymentFile of ['discloud.config', 'package.json']) {
    const deploymentPath = join(process.cwd(), deploymentFile);
    if (existsSync(deploymentPath)) {
      hash.update(`deployment:${deploymentFile}`).update(readFileSync(deploymentPath));
    }
  }
  const artifactSha256 = hash.digest('hex');
  // Determine gitSha: prefer GIT_SHA if explicitly set, then local git commit,
  // then CI GITHUB_SHA, then fallback to artifact SHA.
  let gitSha = process.env.GIT_SHA;
  if (!gitSha) {
    try {
      gitSha = execSync('git rev-parse HEAD', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
    } catch {}
  }
  if (!gitSha || gitSha.length !== 40) {
    gitSha = process.env.GITHUB_SHA || `artifact:${artifactSha256}`;
  }
  writeFileSync(join(root, 'BUILD_ID'), JSON.stringify({gitSha, artifactSha256, builtAt:new Date().toISOString()}));
} catch (err) {
  console.error("Failed to generate BUILD_ID:", err);
  writeFileSync(join(process.cwd(), 'build', 'BUILD_ID'), 'unknown');
}
