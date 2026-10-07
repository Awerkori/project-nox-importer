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
    const res = await client.query(`SELECT value FROM importer_scheduler_state WHERE key = 'active_works'`);
    const activeWorks = res.rows[0]?.value || [];
    console.log(`Active Works Count: ${activeWorks.length}\n`);

    const sourcesRes = await client.query(`SELECT id, name, status, enabled, blocked_reason FROM importer_sources`);
    const sourceMap = new Map();
    for (const s of sourcesRes.rows) {
      sourceMap.set(s.id, s);
      sourceMap.set(s.name, s);
    }

    const table = [];
    for (const aw of activeWorks) {
      const wid = aw.workId;
      const src = sourceMap.get(aw.primarySource) || { name: aw.primarySource, status: 'UNKNOWN', enabled: false, blocked_reason: 'UNKNOWN' };

      // Query max published chapter in DB
      const maxPub = await client.query(`
        SELECT MAX(number) as max_pub 
        FROM chapters 
        WHERE work_id = $1 AND published_at IS NOT NULL
      `, [wid]);
      const lastPubNum = maxPub.rows[0]?.max_pub !== null && maxPub.rows[0]?.max_pub !== undefined
        ? parseFloat(maxPub.rows[0].max_pub)
        : 0;

      // Next required chapter (frontier)
      const nextReq = lastPubNum + 1;

      // Query next jobs in queue
      const qJobs = await client.query(`
        SELECT 
          id, task_type, status, attempts, max_attempts, chapter_sort_key,
          payload->>'chapterNumber' as chapter_num,
          payload->>'source' as job_source
        FROM importer_queue
        WHERE payload->>'workId' = $1
        ORDER BY chapter_sort_key ASC
        LIMIT 3;
      `, [wid]);

      const inflightRes = await client.query(`
        SELECT count(*) as cnt
        FROM importer_queue
        WHERE payload->>'workId' = $1 AND status = 'IMPORTING'
      `, [wid]);
      const inflight = parseInt(inflightRes.rows[0].cnt, 10);

      const nextJob = qJobs.rows[0];
      let claimable = false;
      let blockReason = 'NONE';

      if (!nextJob) {
        blockReason = 'NO_QUEUED_JOBS';
      } else if (inflight >= 2) {
        blockReason = 'MAX_INFLIGHT_REACHED (2)';
      } else if (src.status !== 'ACTIVE' && src.status !== 'DEGRADED') {
        blockReason = `SOURCE_${src.status} (${src.blocked_reason || 'N/A'})`;
      } else if (!src.enabled) {
        blockReason = 'SOURCE_DISABLED';
      } else if (nextJob.attempts >= nextJob.max_attempts) {
        blockReason = `EXHAUSTED_ATTEMPTS (${nextJob.attempts}/${nextJob.max_attempts})`;
      } else if (nextJob.status !== 'QUEUED') {
        blockReason = `JOB_STATUS_${nextJob.status}`;
      } else {
        // Check if nextJob chapter matches or is contiguous with frontier
        const jobNum = parseFloat(nextJob.chapter_num || nextJob.chapter_sort_key);
        if (jobNum > nextReq) {
          blockReason = `GAP_DETECTED (Needs Cap ${nextReq}, Next Job is Cap ${jobNum})`;
        } else {
          claimable = true;
        }
      }

      table.push({
        work: aw.workTitle ? aw.workTitle.substring(0, 25) : wid.substring(0, 8),
        workId: wid,
        source: aw.primarySource,
        lane: aw.lane,
        lastPub: lastPubNum,
        nextRequired: nextReq,
        nextJobChap: nextJob?.chapter_num || 'NONE',
        nextJobStatus: nextJob?.status || 'NONE',
        attempts: nextJob ? `${nextJob.attempts}/${nextJob.max_attempts}` : 'NONE',
        claimable: claimable ? 'YES' : 'NO',
        blockReason,
        inflight
      });
    }

    console.table(table);

    // Summary counts
    const claimableCount = table.filter(t => t.claimable === 'YES').length;
    const blockedCount = table.filter(t => t.claimable === 'NO').length;
    console.log(`\nActive Works Claimable: ${claimableCount} / ${table.length}`);
    console.log(`Active Works Blocked: ${blockedCount} / ${table.length}`);

    const reasons = {};
    for (const t of table) {
      reasons[t.blockReason] = (reasons[t.blockReason] || 0) + 1;
    }
    console.log('Block Reasons Breakdown:', reasons);

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
