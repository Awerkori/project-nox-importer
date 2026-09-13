import dotenv from 'dotenv';
dotenv.config();
import { createClient } from '@supabase/supabase-js';

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

async function queryWithTimeout(name, fn, ms = 4000) {
  const t0 = Date.now();
  const timeoutPromise = new Promise((_, reject) =>
    setTimeout(() => reject(new Error(`TIMEOUT (${ms}ms)`)), ms)
  );
  try {
    const res = await Promise.race([fn(), timeoutPromise]);
    const duration = Date.now() - t0;
    return { success: true, duration, data: res };
  } catch (err) {
    const duration = Date.now() - t0;
    return { success: false, duration, error: err.message };
  }
}

async function main() {
  console.log('=== FAST EMERGENCY SNAPSHOT ===');
  console.log(`Timestamp: ${new Date().toISOString()}\n`);

  // 1. Health RPC
  const healthRes = await queryWithTimeout('admin_get_system_health', async () => {
    const { data, error } = await sb.rpc('admin_get_system_health');
    if (error) throw error;
    return data;
  }, 5000);

  if (healthRes.success) {
    console.log('--- SYSTEM HEALTH ---');
    console.log('Database:', healthRes.data?.database);
    console.log('Counts:', healthRes.data?.counts);
    console.log('Sources:', healthRes.data?.sources);
  } else {
    console.log(`Health RPC: FAILED (${healthRes.error}) in ${healthRes.duration}ms`);
  }

  // 2. Active Connections & pg_stat_activity
  const pgStatRes = await queryWithTimeout('active_queries', async () => {
    // We can query through RPC or inspect slow queries from health
    return healthRes.data?.slow_queries || [];
  }, 2000);
  console.log('\nTop slow queries:', pgStatRes.data?.slice(0, 3));

  // 3. Queue status breakdown (using single fast query if possible)
  const queueStatuses = ['IMPORTING', 'QUEUED', 'RETRY', 'BLOCKED_BY_UPSTREAM', 'COMPLETED', 'FAILED'];
  console.log('\n--- QUEUE STATUSES (HEAD ONLY) ---');
  for (const st of queueStatuses) {
    const qRes = await queryWithTimeout(`queue_${st}`, async () => {
      const { count, error } = await sb
        .from('importer_queue')
        .select('id', { count: 'exact', head: true })
        .eq('status', st);
      if (error) throw error;
      return count;
    }, 3000);
    console.log(`  ${st.padEnd(20)}: ${qRes.success ? qRes.data : 'ERR: ' + qRes.error} (${qRes.duration}ms)`);
  }

  // 4. Staged Mappings (Backlog stored not published)
  const stagedRes = await queryWithTimeout('staged_count', async () => {
    const { count, error } = await sb
      .from('importer_chapter_mappings')
      .select('id', { count: 'exact', head: true })
      .eq('status', 'STAGED');
    if (error) throw error;
    return count;
  }, 3000);
  console.log(`\nSTAGED Mappings (Stored not published): ${stagedRes.success ? stagedRes.data : stagedRes.error} (${stagedRes.duration}ms)`);

  // 5. Last successful publication
  const lastPubRes = await queryWithTimeout('last_published', async () => {
    const { data, error } = await sb
      .from('chapters')
      .select('id, work_id, number, published_at')
      .not('published_at', 'is', null)
      .order('published_at', { ascending: false })
      .limit(3);
    if (error) throw error;
    return data;
  }, 3000);
  console.log('\nLast published chapters:', lastPubRes.success ? lastPubRes.data : lastPubRes.error);

  // 6. Last completed job
  const lastCompRes = await queryWithTimeout('last_completed_job', async () => {
    const { data, error } = await sb
      .from('importer_queue')
      .select('id, source, task_type, updated_at')
      .eq('status', 'COMPLETED')
      .order('updated_at', { ascending: false })
      .limit(3);
    if (error) throw error;
    return data;
  }, 3000);
  console.log('\nLast completed jobs:', lastCompRes.success ? lastCompRes.data : lastCompRes.error);

  // 7. Oldest and newest staged chapter
  const oldestStagedRes = await queryWithTimeout('oldest_staged', async () => {
    const { data, error } = await sb
      .from('importer_chapter_mappings')
      .select('id, work_id, chapter_number, chapter_sort_key, updated_at')
      .eq('status', 'STAGED')
      .order('updated_at', { ascending: true })
      .limit(2);
    if (error) throw error;
    return data;
  }, 3000);
  console.log('\nOldest STAGED mappings:', oldestStagedRes.success ? oldestStagedRes.data : oldestStagedRes.error);

  // 8. Test Web Latency right now
  console.log('\n--- LIVE WEB LATENCIES ---');
  const urls = [
    'https://manga.project-nox-awerkori.workers.dev/',
    'https://manga.project-nox-awerkori.workers.dev/catalogo'
  ];
  for (const u of urls) {
    const t0 = Date.now();
    try {
      const resp = await fetch(u, {
        headers: { 'User-Agent': 'Mozilla/5.0 Nox-EmergencyAudit/1.0' },
        signal: AbortSignal.timeout(4000)
      });
      console.log(`  [${resp.status}] ${u} -> ${Date.now() - t0}ms`);
    } catch (e) {
      console.log(`  [FAIL] ${u} -> ${e.message} in ${Date.now() - t0}ms`);
    }
  }
}

main().catch(console.error);
