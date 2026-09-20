import fs from 'fs';
const glob = require('glob');

const testFiles = glob.sync('tests/**/*.test.ts');
for (const file of testFiles) {
  let content = fs.readFileSync(file, 'utf8');
  if (content.includes('migrations/007_importer_lease_recovery.sql')) {
    content = content.replace(
      "await db.exec(readFileSync(resolve('migrations/007_importer_lease_recovery.sql'), 'utf8'));",
      "await db.exec(readFileSync(resolve('migrations/007_importer_lease_recovery.sql'), 'utf8'));\n    await db.exec(`ALTER TABLE public.works ADD COLUMN IF NOT EXISTS latest_chapter_published_at timestamptz;`);"
    );
    fs.writeFileSync(file, content);
  }
}
