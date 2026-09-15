import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const env = Object.fromEntries(readFileSync('.env', 'utf-8').split('\n').filter(l => l.includes('=') && !l.startsWith('#')).map(l => l.split('=')));
const sb = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);
async function run() {
  const { data: w } = await sb.from('works').select('id').ilike('title', '%Omniscient%').single();
  const { data: jobs } = await sb.from('importer_queue').select('id, payload, priority, chapter_sort_key, status').eq('task_type', 'IMPORT_CHAPTER').eq('payload->>workId', w.id).order('chapter_sort_key', { ascending: true });
  console.log(`Jobs for Omniscient Reader: ${jobs.length}`);
  if (jobs.length > 0) {
    console.log("First 5 jobs:");
    console.log(jobs.slice(0, 5));
    console.log("Last 5 jobs:");
    console.log(jobs.slice(-5));
    
    // Fix their priorities!
    await sb.from('importer_queue').update({ priority: 20 }).eq('task_type', 'IMPORT_CHAPTER').eq('payload->>workId', w.id).eq('priority', 35);
    console.log("Reset priorities to 20.");
  }
}
run();
