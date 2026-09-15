import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(url, key);

async function run() {
  const d = new Date(Date.now() - 60000).toISOString();
  const { count } = await supabase
    .from('chapters')
    .select('id', { count: 'exact', head: true })
    .gt('published_at', d);
    
  console.log(count);
}

run();
