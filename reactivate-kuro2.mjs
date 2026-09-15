import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { const m = line.match(/^([A-Z_]+)=(.*)$/); if (m) env[m[1]] = m[2].trim(); }
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

// blocked_details has not-null constraint - use empty string
const { error, data } = await supabase
  .from('importer_sources')
  .update({
    status: 'ACTIVE',
    enabled: true,
    blocked_reason: '',
    blocked_details: '',
    cooldown_until: null,
    updated_at: new Date().toISOString()
  })
  .eq('id', 'kuro')
  .select();

console.log('Kuro reactivation:', error ? 'FAIL: ' + error.message : 'OK');
if (data?.[0]) console.log('State:', data[0].status, 'enabled:', data[0].enabled);
