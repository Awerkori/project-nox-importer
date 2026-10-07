import { getYugabytePool } from '../build/db/yugabyte-direct.js';

async function run() {
  const pool = getYugabytePool();
  try {
    // 1. Works created
    const lastCreatedRes = await pool.query(`
      SELECT id, title, slug, created_at, published, latest_chapter_published_at
      FROM works
      ORDER BY created_at DESC
      LIMIT 1;
    `);
    const lastCreated = lastCreatedRes.rows[0];

    // 2. Works published
    const lastPubRes = await pool.query(`
      SELECT id, title, slug, created_at, published, latest_chapter_published_at
      FROM works
      WHERE published = true AND latest_chapter_published_at IS NOT NULL
      ORDER BY latest_chapter_published_at DESC
      LIMIT 1;
    `);
    const lastPub = lastPubRes.rows[0];

    // 3. Last work mapping admitted (status = 'ACTIVE' or 'SYNCED')
    const lastAdmittedRes = await pool.query(`
      SELECT id, source, source_title, work_id, sync_status, updated_at, created_at
      FROM importer_work_mappings
      WHERE sync_status IN ('ACTIVE', 'SYNCED')
      ORDER BY updated_at DESC
      LIMIT 1;
    `);
    const lastAdmitted = lastAdmittedRes.rows[0];

    // 4. Last work discovered (most recent mapping created)
    const lastDiscoveredRes = await pool.query(`
      SELECT id, source, source_title, sync_status, created_at
      FROM importer_work_mappings
      ORDER BY created_at DESC
      LIMIT 1;
    `);
    const lastDiscovered = lastDiscoveredRes.rows[0];

    // 5. Time since last new work
    const timeSinceRes = await pool.query(`
      SELECT 
        NOW() as current_time,
        MAX(created_at) as last_work_created_at,
        ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(created_at))) / 60, 1) as minutes_since_last_created
      FROM works;
    `);
    const timeSince = timeSinceRes.rows[0];

    // 6. Mapping breakdown (waiting, rejected, duplicates/ambiguous)
    const mapStatsRes = await pool.query(`
      SELECT 
        sync_status,
        count(*) as total,
        count(*) FILTER (WHERE created_at >= NOW() - INTERVAL '2 hours') as last_2h
      FROM importer_work_mappings
      GROUP BY sync_status;
    `);
    const mapStats = Object.fromEntries(mapStatsRes.rows.map(r => [r.sync_status, { total: Number(r.total), last_2h: Number(r.last_2h) }]));

    // 7. Active scheduler state
    const stateRes = await pool.query(`
      SELECT value FROM importer_scheduler_state WHERE key = 'active_works'
    `);
    const activeWorks = stateRes.rows[0]?.value || [];
    const activeP2 = activeWorks.filter((w) => w.lane === 'P2');

    // 8. Queue P3 candidates
    const p3Res = await pool.query(`
      SELECT count(*) as p3_queue_count
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER' AND priority < 70 AND priority >= 30;
    `);

    console.log(JSON.stringify({
      LAST_NEW_WORK_DISCOVERED: lastDiscovered ? `${lastDiscovered.source_title} (${lastDiscovered.source}) em ${lastDiscovered.created_at}` : 'N/A',
      LAST_NEW_WORK_ADMITTED: lastAdmitted ? `${lastAdmitted.source_title} (${lastAdmitted.source}) em ${lastAdmitted.updated_at || lastAdmitted.created_at}` : 'N/A',
      LAST_NEW_WORK_CREATED: lastCreated ? `${lastCreated.title} (${lastCreated.slug}) em ${lastCreated.created_at}` : 'N/A',
      LAST_NEW_WORK_PUBLISHED: lastPub ? `${lastPub.title} em ${lastPub.latest_chapter_published_at}` : 'N/A',
      TIME_SINCE_LAST_NEW_WORK: `${timeSince.minutes_since_last_created} minutos`,
      DISCOVERED_CANDIDATES_LAST_2H: mapStatsRes.rows.reduce((acc, r) => acc + Number(r.last_2h), 0),
      ELIGIBLE_CANDIDATES: mapStats['WAITING_ADMISSION']?.total || 0,
      REJECTED: mapStats['IGNORED']?.total || 0,
      DUPLICATES: mapStats['AMBIGUOUS']?.total || 0,
      WAITING_ADMISSION: mapStats['WAITING_ADMISSION']?.total || 0,
      P3_CANDIDATES: Number(p3Res.rows[0]?.p3_queue_count || 0),
      ACTIVE_NEW_WORK_COHORT: activeP2.map((w) => ({
        id: w.workId,
        title: w.workTitle,
        admittedAt: w.admittedAt,
        queued: w.queuedChapters,
        pub: w.publishedChapters,
        state: w.state,
      })),
    }, null, 2));

  } catch (err) {
    console.error('Erro na telemetria:', err);
  } finally {
    await pool.end();
  }
}

run();
