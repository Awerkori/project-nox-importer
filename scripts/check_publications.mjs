import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const tenMinsAgo = new Date(Date.now() - 10 * 60000).toISOString();
  const { data, error } = await sb.from('chapters')
    .select('id, number, published_at, work_id(title)')
    .gte('published_at', tenMinsAgo)
    .order('published_at', { ascending: false });
  console.log(`Published in last 10 mins: ${data ? data.length : 0}`);
}
main().catch(console.error);
