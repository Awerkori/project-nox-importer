import dotenv from 'dotenv';
dotenv.config();
import { createClient } from '@supabase/supabase-js';
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function main() {
  const { data } = await sb.from('importer_queue').select('*').eq('id', '59310295-7b65-4827-bb39-20fd0d726886');
  console.dir(data[0], { depth: null });
}
main();
