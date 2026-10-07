import pg from 'pg';
import fs from 'fs';

const envVars = Object.fromEntries(
  fs.readFileSync('/home/awerkori/.config/project-nox/yugabyte.env', 'utf8')
    .split('\n')
    .filter(l => l.includes('=') && !l.startsWith('#'))
    .map(l => [l.split('=')[0].trim(), l.substring(l.indexOf('=') + 1).trim().replace(/^['"]|['"]$/g, '')])
);

const pool = new pg.Pool({
  host: envVars.YUGABYTE_HOST,
  port: parseInt(envVars.YUGABYTE_PORT || '5433', 10),
  user: envVars.YUGABYTE_USER,
  password: envVars.YUGABYTE_PASSWORD,
  database: envVars.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 2
});

async function main() {
  const client = await pool.connect();
  try {
    console.log('============================================================');
    console.log('1. SOURCES STATUS & WAF / ASN BLOCK AUDIT');
    console.log('============================================================');

    const sources = await client.query(`
      SELECT id, name, enabled, status, cooldown_until, blocked_reason,
             chapter_ingestion_enabled, catalog_discovery_enabled
      FROM importer_sources
      ORDER BY name;
    `);
    console.table(sources.rows);

    console.log('\n============================================================');
    console.log('2. QUEUED BACKLOG BREAKDOWN PER SOURCE');
    console.log('============================================================');

    const qPerSource = await client.query(`
      SELECT 
        COALESCE(s.name, q.source) as source_name,
        COALESCE(s.status, 'UNKNOWN') as source_status,
        COALESCE(s.enabled, false) as source_enabled,
        COALESCE(s.chapter_ingestion_enabled, false) as ingestion_enabled,
        COALESCE(s.blocked_reason, 'NONE') as blocked_reason,
        count(*) as total_queued,
        count(CASE WHEN q.attempts < q.max_attempts THEN 1 END) as claimable_attempts,
        count(CASE WHEN q.attempts >= q.max_attempts THEN 1 END) as exhausted_attempts
      FROM importer_queue q
      LEFT JOIN importer_sources s ON s.id = q.source
      WHERE q.status = 'QUEUED'
      GROUP BY COALESCE(s.name, q.source), s.status, s.enabled, s.chapter_ingestion_enabled, s.blocked_reason
      ORDER BY total_queued DESC;
    `);
    console.table(qPerSource.rows);

    console.log('\n============================================================');
    console.log('3. ACTIVE WORKS AUDIT & CLAIMABLE FRONTIER');
    console.log('============================================================');

    const activeWorksSetting = await client.query(`SELECT value FROM settings WHERE key = 'importer_active_works'`);
    let activeWorkIds = [];
    if (activeWorksSetting.rows[0]?.value) {
      const parsed = JSON.parse(activeWorksSetting.rows[0].value);
      activeWorkIds = Array.isArray(parsed) ? parsed : (parsed.workIds || []);
    }
    console.log(`Currently Admitted Active Works (${activeWorkIds.length}):`, activeWorkIds);

    const activeWorksDetails = [];
    for (const wid of activeWorkIds) {
      // Find work details
      const wRes = await client.query(`SELECT id, title, slug, primary_source FROM works WHERE id = $1`, [wid]);
      const work = wRes.rows[0];
      if (!work) {
        activeWorksDetails.push({ workId: wid, title: 'NOT_FOUND', status: 'UNKNOWN' });
        continue;
      }

      // Check max published chapter
      const maxPub = await client.query(`
        SELECT MAX(number) as max_pub 
        FROM chapters 
        WHERE work_id = $1 AND published_at IS NOT NULL
      `, [wid]);
      const frontierNumber = maxPub.rows[0]?.max_pub !== null && maxPub.rows[0]?.max_pub !== undefined 
        ? parseFloat(maxPub.rows[0].max_pub) + 1 
        : 1;

      // Check queue status for this work
      const qStatus = await client.query(`
        SELECT 
          id, task_type, status, attempts, max_attempts, chapter_sort_key,
          payload->>'chapterNumber' as chapter_num,
          payload->>'source' as job_source
        FROM importer_queue
        WHERE payload->>'workId' = $1
        ORDER BY chapter_sort_key ASC
        LIMIT 5;
      `, [wid]);

      const inflightRes = await client.query(`
        SELECT count(*) as inflight_cnt
        FROM importer_queue
        WHERE payload->>'workId' = $1 AND status = 'IMPORTING'
      `, [wid]);

      const nextJob = qStatus.rows[0];
      let claimable = false;
      let blockReason = 'NONE';

      if (!nextJob) {
        blockReason = 'NO_QUEUED_JOBS';
      } else if (nextJob.status === 'IMPORTING') {
        blockReason = 'ALREADY_IMPORTING';
      } else if (nextJob.attempts >= nextJob.max_attempts) {
        blockReason = `EXHAUSTED_ATTEMPTS (${nextJob.attempts}/${nextJob.max_attempts})`;
      } else if (nextJob.status !== 'QUEUED') {
        blockReason = `STATUS_${nextJob.status}`;
      } else {
        claimable = true;
      }

      activeWorksDetails.push({
        title: work.title.substring(0, 30),
        source: work.primary_source,
        nextRequiredChap: frontierNumber,
        nextJobChap: nextJob?.chapter_num || 'N/A',
        nextJobSortKey: nextJob?.chapter_sort_key || 'N/A',
        nextJobStatus: nextJob?.status || 'N/A',
        attempts: nextJob ? `${nextJob.attempts}/${nextJob.max_attempts}` : 'N/A',
        claimable: claimable ? 'YES' : 'NO',
        blockReason,
        inflight: inflightRes.rows[0].inflight_cnt
      });
    }

    console.table(activeWorksDetails);

    console.log('\n============================================================');
    console.log('4. WORKER EFFICIENCY (PROCESSING TIME VS SLOT OCCUPANCY)');
    console.log('============================================================');

    const recentCompleted = await client.query(`
      SELECT 
        id, 
        locked_by,
        payload->>'workId' as work_id,
        payload->>'chapterNumber' as chapter_num,
        payload->>'source' as source,
        locked_at,
        updated_at,
        round(EXTRACT(EPOCH FROM (updated_at - locked_at))::numeric, 2) as occupancy_sec
      FROM importer_queue
      WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' AND updated_at >= NOW() - INTERVAL '60 minutes'
      ORDER BY updated_at DESC
      LIMIT 15;
    `);

    console.table(recentCompleted.rows);

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
