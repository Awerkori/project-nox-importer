import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const sql = readFileSync('/home/awerkori/.Projects/project-nox-importer/migrations/20260914220000_expand_source_status_and_fix_mangaonline.sql', 'utf8');

// Execute via rpc
const { error } = await supabase.rpc('exec_sql', { sql });
if (error) {
  console.log('rpc failed:', error.message);
  // Try direct approach
  const statements = sql.split(';').filter(s => s.trim());
  for (const stmt of statements) {
    const { error: e2 } = await supabase.rpc('exec_sql', { sql: stmt });
    if (e2) console.log('Stmt failed:', e2.message, '|', stmt.slice(0, 60));
    else console.log('OK:', stmt.slice(0, 60));
  }
} else {
  console.log('Migration applied successfully');
}

// Verify final state
const { data } = await supabase.from('importer_sources').select('id, status, enabled').eq('id', 'mangaonline');
console.log('mangaonline:', data);
