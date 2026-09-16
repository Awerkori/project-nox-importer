import fs from 'fs';
let pub = fs.readFileSync('src/core/publication.ts', 'utf8');
pub = pub.replace(/\.is\('published_at', null as any\)/g, "");
fs.writeFileSync('src/core/publication.ts', pub);
