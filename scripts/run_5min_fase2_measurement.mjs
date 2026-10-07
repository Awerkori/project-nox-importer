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
  const DURATION_SEC = 300; // 5 minutes
  const INTERVAL_SEC = 10;
  const samples = [];

  console.log(`Starting 5-minute Fase 2 measurement (${DURATION_SEC}s)...`);

  // Initial counters
  const getCounts = async () => {
    const qRes = await client.query(`
      SELECT 
        count(CASE WHEN status = 'IMPORTING' THEN 1 END) as importing_cnt,
        count(CASE WHEN status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' THEN 1 END) as completed_cnt,
        count(CASE WHEN status = 'QUEUED' THEN 1 END) as queued_cnt
      FROM importer_queue;
    `);

    const chRes = await client.query(`
      SELECT count(*) as published_cnt
      FROM chapters
      WHERE published_at IS NOT NULL;
    `);

    const hbRes = await client.query(`SELECT value FROM settings WHERE key = 'importer_heartbeat'`);
    const hb = hbRes.rows[0]?.value ? JSON.parse(hbRes.rows[0].value) : null;

    // Active jobs details
    const activeJobsRes = await client.query(`
      SELECT id, payload->>'source' as source, payload->>'workId' as work_id,
             round(EXTRACT(EPOCH FROM (now() - locked_at))::numeric, 1) as locked_sec
      FROM importer_queue
      WHERE status = 'IMPORTING';
    `);

    return {
      time: Date.now(),
      importing: parseInt(qRes.rows[0].importing_cnt, 10),
      completed: parseInt(qRes.rows[0].completed_cnt, 10),
      queued: parseInt(qRes.rows[0].queued_cnt, 10),
      published: parseInt(chRes.rows[0].published_cnt, 10),
      rssMb: hb?.rssMb || 0,
      activeJobs: activeJobsRes.rows
    };
  };

  const initial = await getCounts();
  let prev = initial;
  const startTime = Date.now();

  for (let elapsed = INTERVAL_SEC; elapsed <= DURATION_SEC; elapsed += INTERVAL_SEC) {
    await new Promise(r => setTimeout(r, INTERVAL_SEC * 1000));
    const curr = await getCounts();

    const sample = {
      elapsed,
      timestamp: new Date().toISOString(),
      importing: curr.importing,
      completedDelta: curr.completed - prev.completed,
      publishedDelta: curr.published - prev.published,
      rssMb: curr.rssMb,
      activeSources: curr.activeJobs.map(j => j.source).filter(Boolean)
    };
    samples.push(sample);
    prev = curr;

    process.stdout.write(`[${elapsed}s/${DURATION_SEC}s] InFlight: ${curr.importing} | New Completed: ${sample.completedDelta} | New Published: ${sample.publishedDelta} | RSS: ${curr.rssMb}MB\n`);
  }

  const final = await getCounts();
  const totalDurationMin = (final.time - initial.time) / 60000;
  const totalCompleted = final.completed - initial.completed;
  const totalPublished = final.published - initial.published;

  const avgUsefulWorkers = samples.reduce((acc, s) => acc + s.importing, 0) / samples.length;

  console.log('\n============================================================');
  console.log('5-MINUTE FASE 2 SUMMARY REPORT');
  console.log('============================================================');
  console.log(`Duration: ${totalDurationMin.toFixed(2)} min`);
  console.log(`Total Chapters Completed: ${totalCompleted}`);
  console.log(`Total Chapters Published: ${totalPublished}`);
  console.log(`COMPLETED/MIN: ${(totalCompleted / totalDurationMin).toFixed(2)} cap/min`);
  console.log(`NEW VISIBLE/MIN: ${(totalPublished / totalDurationMin).toFixed(2)} cap/min`);
  console.log(`AVG USEFUL WORKERS: ${avgUsefulWorkers.toFixed(2)} / 8`);

  // Fetch recent completed chapters during this 5m test for timing analysis
  const recentTimingRes = await client.query(`
    SELECT 
      c.id, c.number, w.title, m.source,
      round(EXTRACT(EPOCH FROM (c.published_at - m.created_at))::numeric, 2) as pipeline_sec,
      c.published_at
    FROM chapters c
    JOIN works w ON w.id = c.work_id
    JOIN importer_chapter_mappings m ON m.chapter_id = c.id
    WHERE c.published_at >= NOW() - INTERVAL '5 minutes'
    ORDER BY c.published_at DESC;
  `);

  console.log(`\nChapters Published During Window (${recentTimingRes.rows.length}):`);
  console.table(recentTimingRes.rows);

  client.release();
  await pool.end();
}

main().catch(console.error);
