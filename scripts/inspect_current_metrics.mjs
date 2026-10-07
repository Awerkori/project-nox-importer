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

async function run() {
  await client.connect();

  const bootTime = '2026-09-27 00:39:35+00';

  const chRes = await client.query(`
    SELECT 
      count(*) as total_since_boot,
      count(*) FILTER (WHERE is_fresh_release = true) as fresh,
      count(*) FILTER (WHERE is_fresh_release = false) as backfill,
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '1 minute') as pub_1m,
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '3 minute') as pub_3m,
      count(*) FILTER (WHERE published_at >= NOW() - INTERVAL '5 minute') as pub_5m
    FROM chapters
    WHERE published_at >= $1
  `, [bootTime]);

  const qRes = await client.query(`
    SELECT status, count(*) as count
    FROM importer_queue
    WHERE status IN ('PROCESSING', 'QUEUED', 'RETRY', 'FAILED', 'COMPLETED')
    GROUP BY status
  `);

  const stagedRes = await client.query(`
    SELECT count(*) as staged_count
    FROM importer_chapter_mappings
    WHERE status = 'STAGED'
  `);

  const rbRes = await client.query(`
    SELECT 
      bucket_minute,
      visible_published,
      fresh_visible,
      completed_jobs
    FROM importer_rate_buckets
    WHERE bucket_minute >= NOW() - INTERVAL '15 minute'
    ORDER BY bucket_minute DESC
  `);

  const hpRes = await client.query(`
    SELECT 
      count(*) as samples,
      round(avg(download_ms)) as avg_dl_ms,
      round(percentile_cont(0.50) within group (order by download_ms)) as p50_dl_ms,
      round(percentile_cont(0.95) within group (order by download_ms)) as p95_dl_ms,
      round(avg(upload_ms)) as avg_up_ms,
      round(percentile_cont(0.50) within group (order by upload_ms)) as p50_up_ms,
      round(percentile_cont(0.95) within group (order by upload_ms)) as p95_up_ms,
      round(avg(duration_ms)) as avg_dur_ms,
      round(percentile_cont(0.50) within group (order by duration_ms)) as p50_dur_ms,
      round(percentile_cont(0.95) within group (order by duration_ms)) as p95_dur_ms
    FROM importer_job_metrics
    WHERE created_at >= NOW() - INTERVAL '15 minute'
  `);

  console.log('=== DB CHAPTERS ===', chRes.rows[0]);
  console.log('=== STAGED BACKLOG ===', stagedRes.rows[0]);
  console.log('=== QUEUE BREAKDOWN ===', qRes.rows);
  console.log('=== RATE BUCKETS ===', rbRes.rows);
  console.log('=== HOT PATH METRICS ===', hpRes.rows[0]);

  await client.end();
}

run().catch(console.error);
