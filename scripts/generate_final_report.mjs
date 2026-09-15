import { createClient } from '@supabase/supabase-js';
import fs from 'fs';
import dotenv from 'dotenv';
dotenv.config();

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
const sb = createClient(url, key);

async function main() {
  const now = new Date();
  const thirtyMinsAgo = new Date(now.getTime() - 30 * 60000).toISOString();
  
  // Throughput (Site-visible per minute)
  const { data: pubData } = await sb.from('chapters')
    .select('id, published_at')
    .gte('published_at', thirtyMinsAgo);
    
  const totalPub = pubData ? pubData.length : 0;
  // Calculate over the actual time span since the importer was fixed (let's say 10 mins)
  // For now we'll just log the raw count.
  console.log(`Total Published in last 30m: ${totalPub}`);

  // Gap recovery - check FAILED/RETRY jobs
  const { data: qData } = await sb.from('importer_queue')
    .select('status, task_type')
    .eq('task_type', 'IMPORT_CHAPTER')
    .in('status', ['FAILED', 'RETRY']);
    
  console.log(`Failed/Retry jobs: ${qData ? qData.length : 0}`);
}
main().catch(console.error);
