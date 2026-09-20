import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  global: { fetch: (...args) => fetch(args[0], { ...args[1], signal: AbortSignal.timeout(6000) }) }
});

async function run() {
  console.log("Checking DB Status...");
  let start = Date.now();
  let success = false;
  for(let i=0; i<5; i++) {
    try {
      const { count: pending } = await supabase.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'PENDING');
      const { count: processing } = await supabase.from('importer_queue').select('*', { count: 'exact', head: true }).eq('status', 'IMPORTING');
      const { data: barrier } = await supabase.from('settings').select('value').eq('key', 'publication_safety_barrier').single();
      
      console.log(`DB EXECUTION TIME: ${Date.now() - start}ms`);
      console.log(`QUEUE PENDING: ${pending}`);
      console.log(`QUEUE IMPORTING: ${processing}`);
      console.log(`CIRCUIT BREAKER: ${barrier?.value}`);
      success = true;
      break;
    } catch(e) {}
  }
  if(!success) console.log("DB FETCH FAILED DUE TO GATEWAY");
}
run();
