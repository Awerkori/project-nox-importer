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

const DEPLOY_START = '2026-09-26 22:09:00Z';

async function main() {
  await client.connect();

  console.log('====================================================');
  console.log('CLEAN WINDOWS TELEMETRY SNAPSHOT (SINCE DEPLOY: ' + DEPLOY_START + ')');
  console.log('CURRENT TIME:', new Date().toISOString());
  console.log('====================================================\n');

  // 1. Rate buckets since deploy
  const rbRes = await client.query(`
    SELECT 
      bucket_minute,
      visible_published,
      fresh_visible,
      (visible_published - fresh_visible) as backfill_visible,
      completed_jobs,
      updated_at
    FROM importer_rate_buckets
    WHERE bucket_minute >= $1::timestamptz
    ORDER BY bucket_minute ASC
  `, [DEPLOY_START]);
  console.log('--- RATE BUCKETS (CLEAN POST-DEPLOY) ---');
  console.table(rbRes.rows);

  // 2. Chapters in DB
  const chRes = await client.query(`
    SELECT 
      COUNT(*) as total_visible,
      COUNT(*) FILTER (WHERE is_fresh_release = true) as fresh_releases,
      COUNT(*) FILTER (WHERE is_fresh_release = false) as backfills,
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '1 minute') as visible_1m,
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '3 minute') as visible_3m,
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '5 minute') as visible_5m,
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '10 minute') as visible_10m
    FROM chapters
    WHERE published_at >= $1::timestamptz
  `, [DEPLOY_START]);
  console.log('\n--- CHAPTERS PUBLISHED IN DB (CLEAN POST-DEPLOY) ---');
  console.log(JSON.stringify(chRes.rows[0], null, 2));

  // 3. Rolling per-minute breakdown from DB
  const perMinRes = await client.query(`
    SELECT 
      date_trunc('minute', published_at) as min,
      COUNT(*) as total,
      COUNT(*) FILTER (WHERE is_fresh_release = true) as fresh,
      COUNT(*) FILTER (WHERE is_fresh_release = false) as backfill
    FROM chapters
    WHERE published_at >= $1::timestamptz
    GROUP BY 1
    ORDER BY 1 ASC
  `, [DEPLOY_START]);
  console.log('\n--- PER-MINUTE CLOSED DB PUBLICATION COUNTS ---');
  console.table(perMinRes.rows);

  // 4. Job Metrics
  const jmRes = await client.query(`
    SELECT 
      count(*) as sample_count,
      round(avg(duration_ms)) as avg_duration_ms,
      round(percentile_cont(0.50) within group (order by duration_ms)) as p50_duration_ms,
      round(percentile_cont(0.95) within group (order by duration_ms)) as p95_duration_ms,
      round(avg(download_ms)) as avg_download_wall_ms,
      round(percentile_cont(0.50) within group (order by download_ms)) as p50_download_wall_ms,
      round(percentile_cont(0.95) within group (order by download_ms)) as p95_download_wall_ms,
      round(avg(upload_ms)) as avg_upload_wall_ms,
      round(percentile_cont(0.50) within group (order by upload_ms)) as p50_upload_wall_ms,
      round(percentile_cont(0.95) within group (order by upload_ms)) as p95_upload_wall_ms,
      round(avg(db_ms)) as avg_db_ms,
      round(avg(page_count), 1) as avg_pages
    FROM importer_job_metrics
    WHERE created_at >= $1::timestamptz
  `, [DEPLOY_START]);
  console.log('\n--- HOT PATH TIMINGS (CLEAN POST-DEPLOY) ---');
  console.log(JSON.stringify(jmRes.rows[0], null, 2));

  // 5. Heartbeat & Capacity
  const hbRes = await client.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
  const hb = hbRes.rows[0]?.value ? JSON.parse(hbRes.rows[0].value) : {};
  console.log('\n--- CAPACITY & HEALTH ---');
  console.log('Status:', hb.status);
  console.log('Capacity:', JSON.stringify(hb.capacity));
  console.log('Throughput:', JSON.stringify(hb.throughput));
  console.log('RSS (MB):', hb.rssMb);
  console.log('Staged Backlog:', {
    stagedUnique: hb.stagedUnique,
    publishableStaged: hb.publishableStaged,
    waitingPredecessorStaged: hb.waitingPredecessorStaged,
    stuckStaged: hb.stuckStaged
  });

  // 6. Test Site Latencies (Home & Reader)
  console.log('\n--- SITE LATENCIES ---');
  try {
    const t0 = performance.now();
    const homeRes = await fetch('https://manga.project-nox-awerkori.workers.dev/', { headers: { 'User-Agent': 'HealthCheck/1.0' } });
    const homeMs = Math.round(performance.now() - t0);
    console.log('Home Status:', homeRes.status, 'Latency:', homeMs + 'ms');

    const t1 = performance.now();
    const readerRes = await fetch('https://manga.project-nox-awerkori.workers.dev/api/health', { headers: { 'User-Agent': 'HealthCheck/1.0' } });
    const readerMs = Math.round(performance.now() - t1);
    console.log('Reader/Health Status:', readerRes.status, 'Latency:', readerMs + 'ms');
  } catch (siteErr) {
    console.warn('Site probe error:', siteErr.message);
  }

  await client.end();
}

main().catch(console.error);
