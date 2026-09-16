import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  global: { fetch: (...args) => fetch(args[0], { ...args[1], signal: AbortSignal.timeout(10000) }) }
});

async function run() {
  console.log("Testing simple select...");
  let start1 = Date.now();
  const { data: d1 } = await supabase.from('settings').select('*').limit(1);
  console.log(`Simple select time: ${Date.now() - start1}ms`);

  console.log("Testing get_recent_releases RPC...");
  let start2 = Date.now();
  const { data: d2 } = await supabase.rpc('get_recent_releases', {
      p_limit: 16,
      p_chapters_per_work: 3,
      p_cursor_time: null,
      p_cursor_id: null
    });
  console.log(`get_recent_releases time: ${Date.now() - start2}ms`);
}
run();
