import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => l.split('=')));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data, error } = await sb.from('works').insert({
    id: crypto.randomUUID(),
    title: 'Test UNKNOWN',
    slug: 'test-unknown-123',
    kind: 'UNKNOWN',
    status: 'UNKNOWN'
  });
  console.log("Error:", error);
}
run();
