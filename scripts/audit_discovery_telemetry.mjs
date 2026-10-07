import { getYugabytePool } from '../build/db/yugabyte-direct.js';

const globalTimer = setTimeout(() => {
  console.error('❌ [TIMEOUT] Script global timeout (45s).');
  process.exit(1);
}, 45000);
globalTimer.unref();

async function run() {
  const pool = getYugabytePool();
  try {
    console.log('=== 1. TELEMETRIA REAL DE OBRAS NOVAS ===');
    const worksRes = await pool.query(`
      SELECT id, title, slug, created_at, published, latest_chapter_published_at
      FROM works
      ORDER BY created_at DESC
      LIMIT 5;
    `);
    console.log('Últimas 5 obras criadas:', worksRes.rows);

    const timeSinceRes = await pool.query(`
      SELECT 
        NOW() as current_time,
        MAX(w.created_at) as last_work_created_at,
        ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(w.created_at))) / 60, 1) as minutes_since_last_work_created,
        MAX(c.published_at) as last_chapter_published_at,
        ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(c.published_at))) / 60, 1) as minutes_since_last_chapter_published
      FROM works w
      LEFT JOIN chapters c ON c.work_id = w.id;
    `);
    console.log('Tempo desde a última obra:', timeSinceRes.rows[0]);

    const recentWorksCount = await pool.query(`
      SELECT 
        count(*) FILTER (WHERE created_at >= NOW() - INTERVAL '2 hours') as works_created_last_2h,
        count(*) FILTER (WHERE created_at >= NOW() - INTERVAL '4 hours') as works_created_last_4h,
        count(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') as works_created_last_24h,
        count(*) as total_works
      FROM works;
    `);
    console.log('Contagem de obras criadas:', recentWorksCount.rows[0]);

    console.log('\n=== 2. MAPPINGS E CANDIDATOS ===');
    const mapStats = await pool.query(`
      SELECT 
        sync_status,
        count(*) as total,
        count(*) FILTER (WHERE created_at >= NOW() - INTERVAL '2 hours') as last_2h,
        count(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') as last_24h
      FROM importer_work_mappings
      GROUP BY sync_status;
    `);
    console.log('Work Mappings por status:', mapStats.rows);

    const recentMappings = await pool.query(`
      SELECT id, source, source_work_id, source_title, work_id, sync_status, created_at, last_synced_at
      FROM importer_work_mappings
      ORDER BY created_at DESC
      LIMIT 5;
    `);
    console.log('Últimos 5 work mappings criados:', recentMappings.rows);

    console.log('\n=== 3. SETTINGS & CONFIGURAÇÕES GLOBAIS ===');
    const settingsRes = await pool.query(`
      SELECT key, value
      FROM settings
      WHERE key IN (
        'catalog_discovery_enabled',
        'importer_protective_stop',
        'importer_barrier_state',
        'importer_paused',
        'max_active_new_works',
        'max_active_backfill_works'
      );
    `);
    console.log('Settings relevantes:', settingsRes.rows);

    console.log('\n=== 4. FONTES E STATUS DE DISCOVERY ===');
    const sourcesRes = await pool.query(`
      SELECT 
        id, 
        name, 
        enabled, 
        status, 
        catalog_discovery_enabled, 
        sync_interval_minutes,
        last_sync_at, 
        cooldown_until,
        ROUND(EXTRACT(EPOCH FROM (NOW() - last_sync_at)) / 60, 1) as minutes_since_last_sync
      FROM importer_sources
      ORDER BY id;
    `);
    console.log('Status das Fontes:', sourcesRes.rows);

    console.log('\n=== 5. QUEUE JOBS (DISCOVER_WORKS, SYNC_WORK, CHAPTERS) ===');
    const queueTypes = await pool.query(`
      SELECT 
        task_type, 
        status, 
        count(*) as count,
        min(priority) as min_prio,
        max(priority) as max_prio,
        min(next_run_at) as earliest_next_run
      FROM importer_queue
      GROUP BY task_type, status
      ORDER BY task_type, status;
    `);
    console.log('Queue breakdown:', queueTypes.rows);

    const activeDiscoveryJobs = await pool.query(`
      SELECT id, source, task_type, status, priority, attempts, last_error, created_at, next_run_at, locked_by
      FROM importer_queue
      WHERE task_type IN ('DISCOVER_WORKS', 'SYNC_WORK')
      ORDER BY created_at DESC
      LIMIT 10;
    `);
    console.log('Amostra de jobs de Discovery / Sync:', activeDiscoveryJobs.rows);

    console.log('\n=== 6. ADMISSION CONTROLLER CHECK ===');
    const p0Check = await pool.query(`
      SELECT 
        count(*) as all_queued,
        count(*) FILTER (WHERE priority >= 100) as prio_gte_100,
        count(*) FILTER (WHERE priority >= 1000) as staff_forced_cnt,
        count(*) FILTER (WHERE priority >= 100 AND priority < 1000) as real_p0_cnt
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER' AND status IN ('QUEUED', 'RETRY');
    `);
    console.log('Contagem de P0 / Staff Forced no Queue:', p0Check.rows[0]);

    const activeStaffReqs = await pool.query(`
      SELECT id, work_id, priority_boost, status, created_at
      FROM importer_staff_requests
      WHERE status = 'ACTIVE';
    `);
    console.log('Staff requests ativas:', activeStaffReqs.rows);

  } catch (err) {
    console.error('Erro na auditoria:', err);
  } finally {
    await pool.end();
    process.exit(0);
  }
}

run();
