import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// Kuro bridge works! Session renewed. Re-enable Kuro.
const { error, data } = await supabase
  .from('importer_sources')
  .update({
    status: 'ACTIVE',
    enabled: true,
    blocked_reason: null,
    blocked_details: null,
    cooldown_until: null,
    updated_at: new Date().toISOString()
  })
  .eq('id', 'kuro')
  .select();

console.log('Kuro reactivation:', error ? 'FAIL: ' + error.message : 'OK');
console.log('State:', data?.[0]?.status, data?.[0]?.enabled);
