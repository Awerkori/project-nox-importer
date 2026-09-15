import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const tenMinsAgo = new Date(Date.now() - 10 * 60000).toISOString();
  const { data, error } = await sb.from('chapters').select('id, created_at').gte('published_at', tenMinsAgo);
  console.log('Error:', error);
  console.log('Count:', data ? data.length : 0);
}
run();
