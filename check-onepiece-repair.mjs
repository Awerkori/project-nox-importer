import dotenv from 'dotenv';
dotenv.config();
import { createClient } from '@supabase/supabase-js';

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function main() {
  const { data, error } = await sb
    .from('importer_queue')
    .select('*')
    .eq('id', 'a0e5a1d1-4751-4cd2-8d62-fdd4f543c359');
  
  if (error) console.error(error);
  else console.log(data);
}
main();
