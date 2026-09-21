import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  database: process.env.YUGABYTE_DATABASE,
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  await client.connect();

  console.log('=== EXACT ROLLING AVERAGES ===');

  const siteWindows = [5, 15, 30, 60, 360, 1440];
  const siteResults: any = {};

  for (const m of siteWindows) {
    const res = await client.query(`
      SELECT count(*) as count
      FROM chapters
      WHERE published_at >= NOW() - ($1::text || ' minutes')::interval;
    `, [String(m)]);
    const cnt = parseInt(res.rows[0].count, 10);
    siteResults[`${m}m`] = {
      window: `${m} min`,
      publishedCount: cnt,
      capPerMin: (cnt / m).toFixed(2),
      projectedDay: Math.round((cnt / m) * 1440)
    };
  }
  console.log('\n--- SITE PUBLISHED CAP/MIN ---');
  console.table(siteResults);

  const impResults: any = {};
  for (const m of siteWindows) {
    const res = await client.query(`
      SELECT count(*) as count
      FROM importer_queue
      WHERE status = 'COMPLETED'
        AND updated_at >= NOW() - ($1::text || ' minutes')::interval;
    `, [String(m)]);
    const cnt = parseInt(res.rows[0].count, 10);
    impResults[`${m}m`] = {
      window: `${m} min`,
      completedJobs: cnt,
      capPerMin: (cnt / m).toFixed(2)
    };
  }
  console.log('\n--- IMPORTER COMPLETED CAP/MIN ---');
  console.table(impResults);

  // Exact latest publication
  const latestPub = await client.query(`
    SELECT c.id, w.title, c.number, c.published_at,
           EXTRACT(EPOCH FROM (NOW() - c.published_at))/60 as minutes_ago
    FROM chapters c
    JOIN works w ON c.work_id = w.id
    WHERE c.published_at IS NOT NULL
    ORDER BY c.published_at DESC
    LIMIT 1;
  `);
  console.log('\n--- LAST CHAPTER PUBLISHED ---');
  console.log(latestPub.rows[0]);

  // Exact latest import completed
  const latestImp = await client.query(`
    SELECT id, source, task_type, updated_at,
           EXTRACT(EPOCH FROM (NOW() - updated_at))/60 as minutes_ago
    FROM importer_queue
    WHERE status = 'COMPLETED'
    ORDER BY updated_at DESC
    LIMIT 1;
  `);
  console.log('\n--- LAST IMPORT COMPLETED ---');
  console.log(latestImp.rows[0]);

  // Eligible Queue
  const eligibleQ = await client.query(`
    SELECT count(*) as count
    FROM importer_queue q
    WHERE (
      q.status = 'QUEUED'
      OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
    )
      AND q.attempts < COALESCE(q.max_attempts, 7)
      AND q.source IN (SELECT s.id FROM importer_sources s WHERE s.enabled = true AND s.status = 'ACTIVE');
  `);
  console.log('\n--- QUEUE ELIGIBLE COUNT ---');
  console.log('Eligible:', eligibleQ.rows[0].count);

  const totalQueued = await client.query(`
    SELECT count(*) as count FROM importer_queue WHERE status = 'QUEUED';
  `);
  console.log('Total QUEUED in table:', totalQueued.rows[0].count);

  await client.end();
}

main().catch(console.error);
