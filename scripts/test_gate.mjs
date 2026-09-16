import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data } = await sb.from('importer_sources').select('status, enabled, cooldown_until').eq('id', 'manhastro').maybeSingle();
  console.log(data);
  const cached = {
    enabled: data.enabled !== false,
    status: data.status || 'ACTIVE',
    cooldownUntil: data.cooldown_until ? new Date(data.cooldown_until).getTime() : 0,
  };
  const isBlocked = !cached.enabled || cached.status === 'PAUSED' || cached.status === 'DISABLED' || cached.status === 'UPSTREAM_BLOCKED' || cached.status === 'EXCLUDED_BY_POLICY';
  console.log("isBlocked?", isBlocked);
}
run();
