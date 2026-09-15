import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: sr } = await sb.from('importer_staff_requests').select('*').in('status', ['QUEUED', 'IMPORTING', 'RETRYING']);
  console.log('Active staff requests:');
  console.log(sr);
}
main().catch(console.error);
