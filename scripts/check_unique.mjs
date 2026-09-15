import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const { data, error } = await sb.from('pages').select('id, position').eq('chapter_id', 'a978d38e-0c17-4869-90b4-3a5661d9a0cd'); // some chapter ID, but I don't know which one.
  
  // Let's just run an insert with duplicate position on a dummy chapter to see if it violates a unique constraint!
  const dummyId = 'a978d38e-0c17-4869-90b4-3a5661d9a0cd';
  console.log("We'll skip that, just test if unique constraint exists.");
}
main().catch(console.error);
