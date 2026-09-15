import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const { data: jobs } = await supabase
  .from('importer_queue')
  .select('id, status, last_error, payload, attempts')
  .eq('source', 'maidscan')
  .eq('status', 'FAILED')
  .limit(5);

for (const j of (jobs || [])) {
  const p = typeof j.payload === 'string' ? JSON.parse(j.payload) : j.payload;
  console.log('FAILED job:', j.last_error?.slice(0, 150));
  if (p?.chapterUrl || p?.sourceChapterId) console.log('  chapterURL:', p.chapterUrl || p.sourceChapterId);
}
