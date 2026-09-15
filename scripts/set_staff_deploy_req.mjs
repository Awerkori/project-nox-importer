import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

// Let's insert a log in staff requests just to leave a trace for the user if they check DB.
async function main() {
  console.log("No need, I'll just tell them in chat.");
}
main().catch(console.error);
