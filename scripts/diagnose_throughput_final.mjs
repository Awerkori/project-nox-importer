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

  console.log('--- HEARTBEAT ---');
  const hbRes = await client.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'");
  const hb = hbRes.rows[0]?.value ? JSON.parse(hbRes.rows[0].value) : null;
  console.log(JSON.stringify(hb, null, 2));

  console.log('--- RATE BUCKETS (LAST 10 MINUTES) ---');
  const rbRes = await client.query("SELECT * FROM importer_rate_buckets ORDER BY bucket_minute DESC LIMIT 10");
  console.log(JSON.stringify(rbRes.rows, null, 2));

  console.log('--- CHAPTERS PUBLISHED IN DB (ROLLING WINDOWS) ---');
  const chRes = await client.query(`
    SELECT 
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '1 minute') as pub_1m,
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '3 minute') as pub_3m,
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '5 minute') as pub_5m,
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '10 minute') as pub_10m,
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '30 minute') as pub_30m
    FROM chapters
  `);
  console.log(JSON.stringify(chRes.rows[0], null, 2));

  console.log('--- CHAPTERS PER MINUTE (LAST 15 MIN BREAKDOWN) ---');
  const perMinRes = await client.query(`
    SELECT 
      date_trunc('minute', published_at) as min,
      COUNT(*) as count
    FROM chapters
    WHERE published_at >= NOW() - INTERVAL '15 minutes'
    GROUP BY 1
    ORDER BY 1 DESC
  `);
  console.log(JSON.stringify(perMinRes.rows, null, 2));

  console.log('--- QUEUE BREAKDOWN ---');
  const qRes = await client.query("SELECT status, count(*) FROM importer_queue GROUP BY status ORDER BY count DESC");
  console.log(JSON.stringify(qRes.rows, null, 2));

  console.log('--- JOB METRICS (LAST 30M) ---');
  const jmRes = await client.query(`
    SELECT 
      count(*) as sample_count,
      round(avg(duration_ms)) as avg_duration_ms,
      round(percentile_cont(0.50) within group (order by duration_ms)) as p50_duration_ms,
      round(percentile_cont(0.95) within group (order by duration_ms)) as p95_duration_ms,
      round(avg(download_ms)) as avg_download_ms,
      round(avg(upload_ms)) as avg_upload_ms,
      round(avg(db_ms)) as avg_db_ms,
      round(avg(page_count), 1) as avg_pages,
      round(avg(total_bytes) / 1024 / 1024, 2) as avg_mb
    FROM importer_job_metrics
    WHERE created_at >= NOW() - INTERVAL '30 minute'
  `);
  console.log(JSON.stringify(jmRes.rows[0], null, 2));

  console.log('--- EMERGENCY PAUSE & PROTECTIVE STOP ---');
  const stopRes = await client.query("SELECT key, value FROM settings WHERE key IN ('importer_auto_emergency_pause', 'importer_protective_stop', 'publication_safety_barrier')");
  for (const row of stopRes.rows) {
    console.log(row.key, '=>', row.value);
  }

  await client.end();
}

main().catch(console.error);
