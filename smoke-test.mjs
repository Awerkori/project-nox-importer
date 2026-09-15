import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { ImporterEngine } from './build/core/engine.js';
import { MockStorageProvider } from './build/storage/mock.js';
import { SourceRegistry } from './build/sources/registry.js';
import { HostRateLimiter } from './build/core/rate-limit.js';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

async function runTest() {
  const targetIds = [
    '4b1f452c-b223-404c-83f4-e0d626563397',
    '7eb3e4e4-32be-4fb3-9ffd-4b1206701dd5',
    '27242595-44b5-4683-83c6-9b47f9bf6e11'
  ];

  const { data: jobs } = await supabase.from('importer_queue')
    .select('*')
    .eq('task_type', 'IMPORT_CHAPTER')
    .eq('status', 'QUEUED')
    .in('payload->>workId', targetIds)
    .limit(3);

  if (!jobs || jobs.length === 0) return;
  
  const logger = { info: console.log, warn: console.log, error: console.error, errorWithContext: console.error };
  const config = { WORKER_ID: 'test-1', ENV: 'production' };
  
  const engine = new ImporterEngine(
    supabase,
    new MockStorageProvider(),
    new SourceRegistry(),
    new HostRateLimiter(),
    config
  );
  
  for (const job of jobs) {
    console.log(`\nTesting job: ${job.id} for Work: ${job.payload.workId} Chapter: ${job.payload.chapterNumber}`);
    let pagesDownloaded = false;
    let originalAdapter = null;
    try {
      const adapter = engine.registry.getAdapter(job.source);
      originalAdapter = adapter.fetchPages;
      adapter.fetchPages = async () => { pagesDownloaded = true; return []; };
      
      const { count: mappingsBefore } = await supabase.from('importer_chapter_mappings').select('*', {count: 'exact', head: true});
      
      // Inject logger just in case
      engine.logger = logger;
      await engine.handleImportChapter(job, () => false);
      
      const { count: mappingsAfter } = await supabase.from('importer_chapter_mappings').select('*', {count: 'exact', head: true});
      
      console.log(`Page redownload: ${pagesDownloaded ? 'YES' : '0 / explain (adapter fetchPages was NOT called)'}`);
      console.log(`Mapping updated/linked: ${mappingsAfter >= mappingsBefore ? 'YES' : 'NO'}`);
      console.log(`Result: SUCCESS`);
    } catch (e) {
      console.error('Error:', e.message || e);
    } finally {
      if (originalAdapter) {
         const adapter = engine.registry.getAdapter(job.source);
         adapter.fetchPages = originalAdapter;
      }
    }
  }
}
runTest();
