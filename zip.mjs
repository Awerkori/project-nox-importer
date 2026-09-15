import AdmZip from 'adm-zip';
import fs from 'fs';
import path from 'path';

if (!fs.existsSync('dist-discloud')) fs.mkdirSync('dist-discloud');
const zip = new AdmZip();

function addDir(dir) {
  const files = fs.readdirSync(dir);
  for (const file of files) {
    if (file === 'node_modules' || file === '.git' || file === 'dist-discloud' || file === 'dist-vercel' || file.startsWith('.env') || file.endsWith('.zip')) continue;
    const fullPath = path.join(dir, file);
    const stat = fs.statSync(fullPath);
    if (stat.isDirectory()) {
      addDir(fullPath);
    } else {
      zip.addLocalFile(fullPath, dir === '.' ? '' : dir.replace(/^\.\//, ''));
    }
  }
}
addDir('.');
zip.writeZip('dist-discloud/project-nox-importer.zip');
