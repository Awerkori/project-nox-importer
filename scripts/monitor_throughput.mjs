import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function check() {
  const tenMinsAgo = new Date(Date.now() - 10 * 60000).toISOString();
  const { data } = await sb.from('chapters').select('id').gte('published_at', tenMinsAgo);
  console.log(`[${new Date().toISOString()}] Published in last 10 mins: ${data ? data.length : 0}`);
}

setInterval(check, 10000);
check();
