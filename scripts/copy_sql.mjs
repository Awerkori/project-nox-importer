import fs from 'fs';
const mjs = fs.readFileSync('scripts/fix_rpc_direct3.mjs', 'utf8');
const match = mjs.match(/const sql = `([\s\S]*?)`;/);
if (match) {
  const ts = fs.readFileSync('src/fix_rpc.ts', 'utf8');
  const newTs = ts.replace(/const sql = `[\s\S]*?`;/, `const sql = \`${match[1]}\`;`);
  fs.writeFileSync('src/fix_rpc.ts', newTs);
  console.log('Copied successfully!');
}
