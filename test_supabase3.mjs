import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function runTests() {
  console.log("Running home page query...");
  let start = Date.now();
  let res = await supabase.from('works').select('*').limit(20);
  console.log(`Works fetch: ${Date.now() - start}ms | Count: ${res.data?.length}`);

  start = Date.now();
  let res2 = await supabase.from('chapters').select('*').limit(20);
  console.log(`Chapters fetch: ${Date.now() - start}ms | Count: ${res2.data?.length}`);
  
  start = Date.now();
  let res3 = await supabase.from('chapter_stages').select('*').limit(20);
  console.log(`Chapter Stages fetch: ${Date.now() - start}ms | Count: ${res3.data?.length}`);
}
runTests();
