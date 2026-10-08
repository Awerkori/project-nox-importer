const fs = require('fs');
const files = [
  'tests/engine.test.ts', 'tests/daemon-simulation.test.ts',
  'tests/chapter-rescue.test.ts', 'tests/lease-recovery.test.ts',
  'tests/publication-barrier.test.ts', 'tests/releases-definitive-rule.test.ts',
  'tests/multisource-chapters.test.ts', 'tests/cooldown.test.ts'
];
const injection = `    await db.exec(\`
      ALTER TABLE public.importer_sources ADD COLUMN IF NOT EXISTS blocked_reason text;
      ALTER TABLE public.importer_sources ADD COLUMN IF NOT EXISTS blocked_details jsonb NOT NULL DEFAULT '{}'::jsonb;
      CREATE TABLE IF NOT EXISTS public.importer_confirmed_gaps (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), work_id uuid NOT NULL, start_sort_key numeric NOT NULL, end_sort_key numeric NOT NULL, confirmed_at timestamptz NOT NULL DEFAULT now(), primary_source text);
      INSERT INTO public.settings (key, value) VALUES ('publication_safety_barrier', 'OPEN') ON CONFLICT (key) DO UPDATE SET value = excluded.value;
    \`);`;

for (const f of files) {
  if (!fs.existsSync(f)) continue;
  let c = fs.readFileSync(f, 'utf8');
  // Remove the botched injection first
  c = c.replace(/await db\.exec\(`ALTER TABLE public\.importer_sources ADD COLUMN IF NOT EXISTS blocked_reason text;.*?;`\);\n/g, '');
  
  if (f === 'tests/multisource-chapters.test.ts' || f === 'tests/cooldown.test.ts') {
    c = c.replace(/(await db\.exec\(readFileSync\(resolve\('migrations\/005_importer_page_provider_column\.sql'\), 'utf8'\)\);)/, `$1\n${injection}`);
  } else {
    c = c.replace(/(await db\.exec\(readFileSync\(resolve\('migrations\/007_importer_lease_recovery\.sql'\), 'utf8'\)\);)/, `$1\n${injection}`);
  }
  fs.writeFileSync(f, c);
}
