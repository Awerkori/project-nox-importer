import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  await client.connect();

  console.log('--- 1. P0 Jobs Waiting ---');
  const p0 = await client.query(`
    SELECT count(*) as cnt
    FROM importer_queue
    WHERE task_type = 'IMPORT_CHAPTER'
      AND status IN ('QUEUED', 'RETRY')
      AND priority >= 100
  `);
  console.log('P0 Jobs Waiting:', p0.rows[0].cnt);

  console.log('--- 2. P1 Claimable & Available Chapters on Active Sources ---');
  const p1 = await client.query(`
    SELECT 
      COUNT(CASE WHEN q.status IN ('QUEUED', 'RETRY') AND (q.next_run_at IS NULL OR q.next_run_at <= NOW()) THEN 1 END) as claimable_cnt,
      COUNT(CASE WHEN q.status = 'PAUSED_BY_STAFF' THEN 1 END) as paused_cnt,
      COUNT(DISTINCT q.payload->>'workId') as works_cnt
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    JOIN importer_sources s ON s.id = q.source
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND q.status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF')
      AND w.published = true
      AND s.enabled = true
      AND s.status = 'ACTIVE'
      AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW());
  `);
  console.table(p1.rows);

  console.log('--- 3. Active Works in Scheduler State ---');
  const schedRes = await client.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
  const activeWorksMap = schedRes.rows[0]?.value || {};
  const activeWorks = Array.isArray(activeWorksMap) ? activeWorksMap : Object.values(activeWorksMap);
  console.log(`Total Active Works tracked: ${activeWorks.length}`);
  
  const worksBreakdown = [];
  for (const w of activeWorks) {
    // Check published count vs total count in chapters / mappings
    const pubRes = await client.query(
      "SELECT count(*) as pub_cnt FROM chapters WHERE work_id = $1::uuid AND published_at IS NOT NULL",
      [w.workId]
    );
    const pubCnt = parseInt(pubRes.rows[0]?.pub_cnt || '0', 10);

    const mapRes = await client.query(
      "SELECT count(*) as total_maps, count(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_maps FROM importer_chapter_mappings WHERE work_id = $1::uuid",
      [w.workId]
    );
    const totalMaps = parseInt(mapRes.rows[0]?.total_maps || '0', 10);
    const completedMaps = parseInt(mapRes.rows[0]?.completed_maps || '0', 10);

    const qRes = await client.query(
      "SELECT count(*) as queued_cnt FROM importer_queue WHERE (payload->>'workId') = $1 AND status IN ('QUEUED', 'RETRY')",
      [w.workId]
    );
    const queuedCnt = parseInt(qRes.rows[0]?.queued_cnt || '0', 10);

    let classification = 'JUST_STARTED';
    if (pubCnt === 0 || pubCnt <= 2) {
      classification = 'JUST_STARTED';
    } else if (queuedCnt > 0 || completedMaps < totalMaps) {
      classification = 'PARTIALLY_IMPORTED';
    } else {
      classification = 'CAUGHT_UP';
    }

    worksBreakdown.push({
      workId: w.workId,
      title: (w.workTitle || '').substring(0, 30),
      lane: w.lane,
      state: w.state,
      source: w.primarySource,
      pubCnt,
      totalMaps,
      completedMaps,
      queuedCnt,
      classification
    });
  }
  console.table(worksBreakdown);

  console.log('--- 4. Works Admitted in importer_work_mappings (Last 24 hours) ---');
  const recentMappings = await client.query(`
    SELECT m.work_id, w.title, m.source, m.sync_status, m.created_at, m.updated_at,
           COUNT(c.id) as pub_chapters
    FROM importer_work_mappings m
    LEFT JOIN works w ON w.id = m.work_id
    LEFT JOIN chapters c ON c.work_id = m.work_id AND c.published_at IS NOT NULL
    WHERE m.created_at >= NOW() - INTERVAL '24 hours'
       OR m.updated_at >= NOW() - INTERVAL '2 hours'
    GROUP BY m.work_id, w.title, m.source, m.sync_status, m.created_at, m.updated_at
    ORDER BY m.updated_at DESC
    LIMIT 20
  `);
  console.table(recentMappings.rows.map(r => ({
    workId: r.work_id,
    title: (r.title || '').substring(0, 30),
    source: r.source,
    sync_status: r.sync_status,
    pub_chapters: r.pub_chapters,
    updated_at: r.updated_at
  })));

  await client.end();
}

main().catch(console.error);
