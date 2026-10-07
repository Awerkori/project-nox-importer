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
    console.log('=== AUDIT DE CLAIMABLE FRONTIERS REAIS ===\n');

    // 1. Identify healthy sources vs blocked sources
    const sourcesRes = await client.query(`
      SELECT id, name, status, enabled, cooldown_until, blocked_reason
      FROM importer_sources
    `);
    const healthySourceNames = new Set();
    const blockedSourceNames = new Set();
    const sourceStatusMap = {};

    for (const s of sourcesRes.rows) {
      sourceStatusMap[s.id] = s;
      sourceStatusMap[s.name] = s;
      const isHealthy = s.enabled && (s.status === 'ACTIVE' || s.status === 'DEGRADED') && (!s.cooldown_until || new Date(s.cooldown_until) <= new Date());
      if (isHealthy) {
        healthySourceNames.add(s.id);
        healthySourceNames.add(s.name);
      } else {
        blockedSourceNames.add(s.id);
        blockedSourceNames.add(s.name);
      }
    }

    console.log(`Healthy Sources: ${healthySourceNames.size / 2} | Blocked/Cooldown Sources: ${blockedSourceNames.size / 2}`);

    // 2. Count Queued in Healthy vs Blocked sources
    const qCountRes = await client.query(`
      SELECT 
        q.source,
        count(*) as total_queued,
        count(CASE WHEN q.attempts < q.max_attempts THEN 1 END) as valid_attempts,
        count(CASE WHEN q.attempts >= q.max_attempts THEN 1 END) as exhausted_attempts
      FROM importer_queue q
      WHERE q.status = 'QUEUED'
      GROUP BY q.source;
    `);

    let queuedInHealthy = 0;
    let queuedInBlocked = 0;
    let validAttemptsHealthy = 0;
    let validAttemptsBlocked = 0;

    for (const r of qCountRes.rows) {
      const srcName = r.source;
      const isH = healthySourceNames.has(srcName);
      const cnt = parseInt(r.total_queued, 10);
      const valid = parseInt(r.valid_attempts, 10);
      if (isH) {
        queuedInHealthy += cnt;
        validAttemptsHealthy += valid;
      } else {
        queuedInBlocked += cnt;
        validAttemptsBlocked += valid;
      }
    }

    console.log(`QUEUED in Healthy Sources: ${queuedInHealthy} (Valid attempts: ${validAttemptsHealthy})`);
    console.log(`QUEUED in Blocked Sources: ${queuedInBlocked} (Valid attempts: ${validAttemptsBlocked})`);

    // 3. Find all distinct works with QUEUED jobs
    const worksInQueue = await client.query(`
      SELECT 
        (payload->>'workId') as work_id,
        count(*) as queued_count,
        min(chapter_sort_key) as min_queued_sort_key
      FROM importer_queue
      WHERE status = 'QUEUED' AND (payload->>'workId') IS NOT NULL
      GROUP BY (payload->>'workId');
    `);

    console.log(`\nDistinct Works with QUEUED jobs: ${worksInQueue.rows.length}`);

    // 4. For each work with queued jobs, find max published chapter and determine if frontier is queued
    let healthyWorksWithFrontier = 0;
    let worksWithGap = 0;
    let worksWithFrontierOnBlockedSource = 0;
    let totalClaimableFrontiers = 0;
    const healthyFrontierWorksList = [];

    // Query published chapters max for all these works
    const workIds = worksInQueue.rows.map(r => r.work_id);
    
    // Batch query published chapters
    const publishedRes = await client.query(`
      SELECT work_id, MAX(number) as max_pub
      FROM chapters
      WHERE published_at IS NOT NULL AND work_id = ANY($1::uuid[])
      GROUP BY work_id;
    `, [workIds]);
    const maxPubMap = new Map();
    for (const r of publishedRes.rows) {
      maxPubMap.set(r.work_id, parseFloat(r.max_pub));
    }

    // Now query the EARLIEST QUEUED chapter for each work
    const earliestQueuedRes = await client.query(`
      SELECT DISTINCT ON (payload->>'workId')
        (payload->>'workId') as work_id,
        q.id as job_id,
        q.source,
        q.attempts,
        q.max_attempts,
        q.chapter_sort_key,
        payload->>'chapterNumber' as chapter_num
      FROM importer_queue q
      WHERE q.status = 'QUEUED' AND (payload->>'workId') IS NOT NULL
      ORDER BY (payload->>'workId'), q.chapter_sort_key ASC;
    `);

    const earliestMap = new Map();
    for (const r of earliestQueuedRes.rows) {
      earliestMap.set(r.work_id, r);
    }

    // Also get work titles and primary sources
    const worksInfo = await client.query(`
      SELECT id, title
      FROM works
      WHERE id = ANY($1::uuid[]);
    `, [workIds]);
    const workInfoMap = new Map();
    for (const w of worksInfo.rows) {
      workInfoMap.set(w.id, w);
    }

    const healthyClaimableSourcesSet = new Set();

    for (const r of worksInQueue.rows) {
      const wid = r.work_id;
      const lastPub = maxPubMap.has(wid) ? maxPubMap.get(wid) : 0;
      const nextRequired = lastPub + 1;
      const earliest = earliestMap.get(wid);
      const workInfo = workInfoMap.get(wid);

      if (!earliest) continue;

      const jobChap = parseFloat(earliest.chapter_num || earliest.chapter_sort_key);
      const isFrontier = jobChap <= nextRequired; // either exact next or earlier unimported
      const isAttemptsOk = earliest.attempts < earliest.max_attempts;
      const isSourceHealthy = healthySourceNames.has(earliest.source);

      if (isFrontier && isAttemptsOk) {
        if (isSourceHealthy) {
          healthyWorksWithFrontier++;
          totalClaimableFrontiers++;
          healthyClaimableSourcesSet.add(earliest.source);
          healthyFrontierWorksList.push({
            title: workInfo?.title || wid,
            workId: wid,
            source: earliest.source,
            lastPub,
            nextRequired,
            jobChap,
            queuedCount: r.queued_count
          });
        } else {
          worksWithFrontierOnBlockedSource++;
        }
      } else if (!isFrontier) {
        worksWithGap++;
      }
    }

    console.log('\n============================================================');
    console.log('RESUMO DE FRONTEIRAS CLAIMÁVEIS REAIS');
    console.log('============================================================');
    console.log(`TOTAL QUEUED: ${queuedInHealthy + queuedInBlocked}`);
    console.log(`REAL CLAIMABLE FRONTIERS: ${totalClaimableFrontiers}`);
    console.log(`HEALTHY WORKS WITH CLAIMABLE FRONTIER: ${healthyWorksWithFrontier}`);
    console.log(`HEALTHY SOURCES WITH CLAIMABLE FRONTIER: ${healthyClaimableSourcesSet.size}`);
    console.log(`WORKS WITH GAP (MISSING PREDECESSOR): ${worksWithGap}`);
    console.log(`WORKS WITH FRONTIER ON BLOCKED SOURCE (WAF): ${worksWithFrontierOnBlockedSource}`);

    console.log('\nSample of Healthy Works With Claimable Frontier (top 15):');
    console.table(healthyFrontierWorksList.slice(0, 15));

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
