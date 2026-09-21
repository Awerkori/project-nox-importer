import fs from 'node:fs';
import path from 'node:path';
import { execSync } from 'node:child_process';

const EXTENSIONS_DIR = '/home/awerkori/.Projects/Project-Nox/fonte-extensoes';

// 1. Get list of PT extensions in origin/main (Awerkori)
const originTree = execSync(`git -C "${EXTENSIONS_DIR}" ls-tree --name-only origin/main src/pt/`, { encoding: 'utf8' })
  .split('\n')
  .map(l => l.replace('src/pt/', '').trim())
  .filter(Boolean);

// 2. Get list of PT extensions in upstream/main (Keiyoushi)
const upstreamTree = execSync(`git -C "${EXTENSIONS_DIR}" ls-tree --name-only upstream/main src/pt/`, { encoding: 'utf8' })
  .split('\n')
  .map(l => l.replace('src/pt/', '').trim())
  .filter(Boolean);

const allExtensionIds = Array.from(new Set([...originTree, ...upstreamTree])).sort();

console.log(`Found ${originTree.length} PT extensions in Awerkori/fonte-extensoes`);
console.log(`Found ${upstreamTree.length} PT extensions in keiyoushi/extensions-source`);
console.log(`Total unique extension IDs: ${allExtensionIds.length}`);

// Function to read file from git tree
function getFileFromGit(ref, filePath) {
  try {
    return execSync(`git -C "${EXTENSIONS_DIR}" show "${ref}:${filePath}" 2>/dev/null`, { encoding: 'utf8' });
  } catch {
    return null;
  }
}

const extensionData = [];

for (const extId of allExtensionIds) {
  const inOrigin = originTree.includes(extId);
  const inUpstream = upstreamTree.includes(extId);
  const ref = inOrigin ? 'origin/main' : 'upstream/main';

  const gradlePath = `src/pt/${extId}/build.gradle.kts`;
  const gradleContent = getFileFromGit(ref, gradlePath) || '';

  // Extract name, baseUrl, theme, versionCode
  const nameMatch = gradleContent.match(/name\s*=\s*["']([^"']+)["']/i);
  const baseUrlMatch = gradleContent.match(/baseUrl\s*=\s*["']([^"']+)["']/i);
  const themeMatch = gradleContent.match(/theme\s*=\s*["']([^"']+)["']/i);
  const versionMatch = gradleContent.match(/versionCode\s*=\s*(\d+)/i);
  const classMatch = gradleContent.match(/className\s*=\s*["']([^"']+)["']/i);

  let name = nameMatch ? nameMatch[1].trim() : extId;
  let baseUrl = baseUrlMatch ? baseUrlMatch[1].trim() : '';
  const theme = themeMatch ? themeMatch[1].trim() : '';
  const versionCode = versionMatch ? parseInt(versionMatch[1], 10) : 1;

  // If baseUrl not in gradle, search kt files
  if (!baseUrl) {
    try {
      const ktFiles = execSync(`git -C "${EXTENSIONS_DIR}" ls-tree -r --name-only "${ref}" "src/pt/${extId}/"`, { encoding: 'utf8' })
        .split('\n')
        .filter(f => f.endsWith('.kt') && !f.contains('Test'));
      
      for (const kt of ktFiles) {
        const ktContent = getFileFromGit(ref, kt) || '';
        const urlInKt = ktContent.match(/baseUrl\s*(?::\s*String)?\s*=\s*["']([^"']+)["']/i) ||
                        ktContent.match(/override\s+val\s+baseUrl\s*=\s*["']([^"']+)["']/i) ||
                        ktContent.match(/["'](https?:\/\/[a-zA-Z0-9.\-_]+)["']/i);
        if (urlInKt && !urlInKt[1].includes('api.github') && !urlInKt[1].includes('schema.org')) {
          baseUrl = urlInKt[1].trim();
          break;
        }
      }
    } catch {}
  }

  extensionData.push({
    extensionId: extId,
    name,
    baseUrl,
    theme: theme || 'custom',
    versionCode,
    inAwerkori: inOrigin,
    inKeiyoushi: inUpstream
  });
}

fs.writeFileSync('pt_extensions_catalog.json', JSON.stringify(extensionData, null, 2));
console.log(`Saved catalog to pt_extensions_catalog.json (${extensionData.length} extensions)`);
