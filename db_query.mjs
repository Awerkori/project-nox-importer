import { createClient } from '@supabase/supabase-js';
import fs from 'fs';
import dotenv from 'dotenv';
dotenv.config();

const supabaseUrl = process.env.VITE_SUPABASE_URL || process.env.SUPABASE_URL;
const supabaseKey = process.env.VITE_SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.VITE_SUPABASE_ANON_KEY;

const supabase = createClient(supabaseUrl, supabaseKey);

async function run() {
  // 1. Queue statuses
  const { data: queueData, error: qErr } = await supabase
    .from('importer_queue')
    .select('status');
    
  const qCounts = {};
  if (queueData) {
    queueData.forEach(r => qCounts[r.status] = (qCounts[r.status] || 0) + 1);
  } else {
    console.error("Queue query error:", qErr);
  }

  // 2. STAGED chapters count
  const { data: stagedData, error: sErr } = await supabase
    .from('chapters')
    .select('id', { count: 'exact', head: true })
    .is('published_at', null);

  // 3. Importer logs / FAILED errors
  const { data: failedJobs } = await supabase
    .from('importer_queue')
    .select('status, payload, result')
    .eq('status', 'FAILED')
    .limit(5);

  console.log("== QUEUE STATUSES ==");
  console.log(qCounts);
  console.log(`\n== STAGED CHAPTERS ==\nTotal: ${stagedData || (sErr && sErr.message)}`);
  console.log(`\n== FAILED JOBS SAMPLES ==`);
  console.log(JSON.stringify(failedJobs, null, 2));
}
run();
