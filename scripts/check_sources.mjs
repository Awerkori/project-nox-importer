import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data: s } = await sb.from('importer_sources').select('id, enabled, status, cooldown_until');
  console.log('Sources:', s);
}
main().catch(console.error);
