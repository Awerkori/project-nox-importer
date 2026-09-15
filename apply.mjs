import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
const sql = readFileSync('/home/awerkori/.Projects/project-nox-importer/migrations/20260914171500_restore_recovery_gap_execution.sql', 'utf8');
async function run() {
  const statements = sql.split(';').filter(s => s.trim());
  for (const stmt of statements) {
    const { error: e2 } = await supabase.rpc('exec_sql', { sql: stmt });
    if (e2) console.log('Stmt failed:', e2.message);
    else console.log('OK');
  }
}
run();
