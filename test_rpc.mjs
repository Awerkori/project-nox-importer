import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  global: { fetch: (...args) => fetch(args[0], { ...args[1], signal: AbortSignal.timeout(6000) }) }
});

async function run() {
  console.log("Testing get_recent_releases RPC via API Gateway...");
  let start = Date.now();
  try {
    const { data, error } = await supabase.rpc('get_recent_releases', { limit_val: 20 });
    const time = Date.now() - start;
    if (error) {
      console.log(`RPC ERROR: ${error.message} | Time: ${time}ms`);
    } else {
      console.log(`RPC SUCCESS! Items: ${data?.length} | Time: ${time}ms`);
    }
  } catch(e) {
    console.log(`RPC EXCEPTION: ${e.message} | Time: ${Date.now() - start}ms`);
  }
}
run();
