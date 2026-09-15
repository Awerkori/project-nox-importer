import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const s = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const checkTable = async (tableName) => {
  const { data: row, error: rowErr } = await s.from(tableName).select('*').limit(1);
  if (row && row.length) {
    console.log(tableName, 'columns:', Object.keys(row[0]).join(', '));
  } else {
    // If empty table, let's try to get column names via an impossible query
    const { data: cols, error: e } = await s.from(tableName).select('*').eq('id', '99999999-9999-9999-9999-999999999999');
    if (e) {
      console.log(tableName, 'error:', e.message);
    } else {
      console.log(tableName, 'columns:', Object.keys(cols[0] || {}).join(', '));
    }
  }
}
await checkTable('works');
await checkTable('work_aliases');
