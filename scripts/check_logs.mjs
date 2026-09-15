import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(url, key);

async function run() {
  const d = new Date(Date.now() - 5 * 60000).toISOString();
  const { data, error } = await supabase
    .from('importer_queue')
    .select('id, status, updated_at')
    .gt('updated_at', d)
    .order('updated_at', { ascending: false })
    .limit(10);
    
  console.log("Recent queue updates (last 5 min):");
  console.log(data);
}
run();
