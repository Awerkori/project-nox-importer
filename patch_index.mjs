import fs from 'fs';
let content = fs.readFileSync('src/index.ts', 'utf8');

// Add imports at the top
content = "import fsSync from 'fs';\nimport path from 'path';\nimport { fileURLToPath } from 'url';\n" + content;

content = content.replace(
  "rootLogger.info('Starting Project Nox Importer daemon...');",
  "const __dirname = path.dirname(fileURLToPath(import.meta.url));\n  let buildCommit = 'unknown';\n  try {\n    buildCommit = fsSync.readFileSync(path.join(__dirname, 'COMMIT'), 'utf8').trim();\n  } catch (e) { /* ignore */ }\n\n  rootLogger.info(`Starting Project Nox Importer daemon... | Build: ${buildCommit}`);"
);
fs.writeFileSync('src/index.ts', content);
