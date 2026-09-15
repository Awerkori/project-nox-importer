import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => l.split('=')));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  await sb.from('works').delete().eq('slug', 'test-unknown-123');
}
run();
