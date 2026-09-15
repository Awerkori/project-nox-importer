import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: importing } = await sb.from('importer_queue').select('id, locked_by, locked_at, lease_expires_at, source').eq('status', 'IMPORTING');
  console.log('Active jobs:');
  console.log(importing);
  
  const now = new Date();
  for (const job of importing || []) {
    const lockTime = new Date(job.locked_at);
    const diff = (now - lockTime) / 60000;
    console.log(`Job ${job.id} locked by ${job.locked_by} at ${job.locked_at} (${diff.toFixed(1)} mins ago)`);
  }
}
main().catch(console.error);
