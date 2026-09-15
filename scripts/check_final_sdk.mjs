import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(url, key);

async function run() {
  const { data: queueData, error: qErr } = await supabase
    .from('importer_queue')
    .select('status');
  
  if (qErr) {
    console.log("Queue Error:", qErr);
  } else {
    let qCounts = {};
    queueData.forEach(r => qCounts[r.status] = (qCounts[r.status] || 0) + 1);
    console.log("== QUEUE STATUS ==");
    console.log(qCounts);
  }
  
  const d = new Date(Date.now() - 5 * 60000).toISOString();
  const { count: pubCount, error: pErr } = await supabase
    .from('chapters')
    .select('id', { count: 'exact', head: true })
    .gt('published_at', d);
    
  if (pErr) {
    console.log("Pub Error:", pErr);
  } else {
    console.log("\n== PUBLISHED LAST 5 MIN ==");
    console.log(pubCount);
  }
  
  const { count: acqCount, error: aErr } = await supabase
    .from('importer_queue')
    .select('id', { count: 'exact', head: true })
    .eq('status', 'ACQUIRED')
    .gt('updated_at', d);
    
  if (aErr) {
    console.log("Acq Error:", aErr);
  } else {
    console.log("\n== ACQUIRED LAST 5 MIN ==");
    console.log(acqCount);
  }
}

run();
