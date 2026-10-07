import { DirectSupabaseClient } from '../build/db/direct-supabase-client.js';
import { MockStorageProvider } from '../build/storage/mock.js';
import { SourceRegistry } from '../build/sources/registry.js';
import { HostRateLimiter } from '../build/core/rate-limiter.js';
import { getConfig } from '../build/config.js';
import { ImporterEngine } from '../build/core/engine.js';
import { getYugabytePool } from '../build/db/yugabyte-direct.js';

async function main() {
  const config = getConfig();
  const pool = getYugabytePool();
  const supabase = new DirectSupabaseClient();
  const storage = new MockStorageProvider();
  const rateLimiter = new HostRateLimiter(5.0);
  const registry = new SourceRegistry(rateLimiter);
  const engine = new ImporterEngine(supabase, storage, registry, rateLimiter, config);

  const t0 = Date.now();
  console.log('Fetching SYNC_WORK job for Demon For a Night (zettahq)...');
  const jobRes = await pool.query(`
    SELECT * FROM importer_queue 
    WHERE id = '4943f258-9cf5-43c1-b39d-8a5edb6731ff'
  `);
  const job = jobRes.rows[0];
  if (!job) {
    console.error('Job not found');
    process.exit(1);
  }

  console.log('DISCOVERED AT:', job.created_at);
  console.log('Executing handleSyncWork...');
  await engine.handleSyncWork(job);

  const workRes = await pool.query(`
    SELECT w.id, w.title, w.slug, w.created_at, m.sync_status, m.updated_at as admitted_at
    FROM works w
    JOIN importer_work_mappings m ON m.work_id = w.id
    WHERE m.source = 'zettahq' AND (m.source_title ILIKE '%Demon For a Night%' OR w.title ILIKE '%Demon For a Night%')
    ORDER BY w.created_at DESC
    LIMIT 1
  `);

  const createdWork = workRes.rows[0];
  const t1 = Date.now();
  console.log('=== REAL VALIDATION RECORD ===');
  console.log('WORK TITLE:', createdWork?.title);
  console.log('SOURCE:', 'zettahq');
  console.log('DISCOVERED AT:', job.created_at);
  console.log('ADMITTED AT:', createdWork?.admitted_at || new Date().toISOString());
  console.log('CREATED AT:', createdWork?.created_at);
  console.log('TIME DISCOVERY -> CREATED:', `${Math.round((new Date(createdWork?.created_at).getTime() - new Date(job.created_at).getTime()) / 1000)}s`);

  await pool.query(`
    UPDATE importer_queue 
    SET status = 'COMPLETED', updated_at = NOW() 
    WHERE id = '4943f258-9cf5-43c1-b39d-8a5edb6731ff'
  `);

  await pool.end();
  process.exit(0);
}

main().catch(err => {
  console.error('Sync failed:', err);
  process.exit(1);
});
