import dotenv from 'dotenv';
dotenv.config();
import { createClient } from '@supabase/supabase-js';
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function main() {
  await sb.from('importer_queue').update({
    status: 'QUEUED',
    locked_by: null,
    locked_at: null,
    lease_expires_at: null
  }).eq('id', '59310295-7b65-4827-bb39-20fd0d726886');
  console.log("Unlocked job.");
}
main();
