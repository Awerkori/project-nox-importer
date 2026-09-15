import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => l.split('=')));
const sql = `
ALTER TYPE work_kind ADD VALUE IF NOT EXISTS 'UNKNOWN';
ALTER TYPE work_status ADD VALUE IF NOT EXISTS 'UNKNOWN';
`;
async function run() {
  const res = await fetch(`${env.SUPABASE_URL}/rest/v1/rpc/exec_sql`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
      'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
    },
    body: JSON.stringify({ sql_string: sql })
  });
  console.log(res.status, await res.text());
}
run();
