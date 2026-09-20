import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

async function run() {
  const start = Date.now();
  const { data, error } = await sb.rpc('admin_get_system_health');
  console.log(`DB Health (${Date.now() - start}ms):`, data || error);
}
run();
