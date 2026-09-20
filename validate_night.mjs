import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function run() {
  const start = '2026-09-16T00:30:00Z';
  const end = '2026-09-16T11:25:00Z';
  
  // 1. Check real published chapters
  const { data: chapters, error } = await supabase
    .from('chapters')
    .select('id, work_id, created_at, published_at')
    .gte('published_at', start)
    .lte('published_at', end);
    
  if (error) console.error(error);
  
  const distinctWorks = new Set(chapters?.map(c => c.work_id));
  console.log(`REAL PUBLISHED: ${chapters?.length || 0}`);
  console.log(`DISTINCT WORKS: ${distinctWorks.size}`);
  
  // 2. Check duplicates (chapters with same number in same work)
  // But wait, we can just check if the queue had failures
  const { data: metrics } = await supabase
    .from('importer_job_metrics')
    .select('*')
    .gte('created_at', start)
    .lte('created_at', end);
    
  let failed = 0;
  let retries = 0;
  metrics?.forEach(m => {
    if (m.status === 'FAILED') failed++;
    if (m.attempts > 1) retries += (m.attempts - 1);
  });
  
  console.log(`FAILED JOBS: ${failed}`);
  console.log(`RETRIES: ${retries}`);
  
  // 3. Current Importer Queue Status
  const { count: pending } = await supabase.from('importer_queue').select('id', { count: 'exact', head: true }).eq('status', 'pending');
  const { count: processing } = await supabase.from('importer_queue').select('id', { count: 'exact', head: true }).eq('status', 'processing');
  
  console.log(`CURRENT DB QUEUE - Pending: ${pending}, Processing: ${processing}`);
  
  // 4. Current DB Barrier state
  const { data: barrier } = await supabase.from('settings').select('value').eq('key', 'publication_safety_barrier').single();
  console.log(`CURRENT DB BARRIER: ${barrier?.value}`);
}
run();
