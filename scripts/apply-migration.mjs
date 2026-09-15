import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => { const i = l.indexOf('='); return [l.slice(0,i), l.slice(i+1)]; }));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const sql = readFileSync('migrations/20260914235000_releases_rpc_and_fairness.sql', 'utf-8');

// Split on semicolons but handle $$ blocks carefully
// Supabase admin JS client doesn't support raw SQL directly, we'll use pg-meta API
const projectRef = env.SUPABASE_URL.match(/https?:\/\/([^.]+)\./)?.[1];
const response = await fetch(`${env.SUPABASE_URL}/rest/v1/rpc/`, {
  method: 'POST',
  headers: {
    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({ query: sql })
});

// Try the pg-meta endpoint
const res2 = await fetch(`https://api.supabase.com/v1/projects/${projectRef}/database/query`, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${env.SUPABASE_ACCESS_TOKEN || env.SUPABASE_PAT || ''}`,
  },
  body: JSON.stringify({ query: sql })
});
console.log('pg-meta response:', res2.status, await res2.text().catch(() => ''));
