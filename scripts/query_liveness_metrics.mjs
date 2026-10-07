import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: {
    rejectUnauthorized: true,
    ca: fs.readFileSync('./config/root.crt').toString()
  }
});

async function main() {
  const client = await pool.connect();
  try {
    console.log("=== PROJECT NOX LIVENESS TELEMETRY AUDIT ===");

    // 1. Last chapters published
    const lastChRes = await client.query(`
      SELECT c.id, c.work_id, w.title as work_title, c.number, c.published_at, c.created_at
      FROM chapters c
      LEFT JOIN works w ON c.work_id = w.id
      WHERE c.published_at IS NOT NULL
      ORDER BY c.published_at DESC
      LIMIT 5;
    `);
    console.log("\nLast 5 published chapters:", JSON.stringify(lastChRes.rows, null, 2));

    // 2. Last works created
    const lastWorksRes = await client.query(`
      SELECT id, title, created_at, updated_at
      FROM works
      ORDER BY created_at DESC
      LIMIT 5;
    `);
    console.log("\nLast 5 works created:", JSON.stringify(lastWorksRes.rows, null, 2));

    // 3. Chapter count 1h and 8h
    const countsChRes = await client.query(`
      SELECT
        COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '1 hour') as last_1h,
        COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '8 hour') as last_8h
      FROM chapters;
    `);
    console.log("\nChapter publication counts:", countsChRes.rows[0]);

    // 4. Works created 1h and 8h
    const countsWorksRes = await client.query(`
      SELECT
        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1 hour') as works_1h,
        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '8 hour') as works_8h
      FROM works;
    `);
    console.log("\nWorks created counts:", countsWorksRes.rows[0]);

    // 5. Jobs stats: total depth, active leases, priority mix
    const jobsRes = await client.query(`
      SELECT
        COUNT(*) as total_queue_depth,
        COUNT(*) FILTER (WHERE status = 'IMPORTING') as active_leases,
        COUNT(*) FILTER (WHERE status = 'QUEUED') as queued_count,
        COUNT(*) FILTER (WHERE status = 'QUEUED' AND priority <= 5) as p0_count,
        COUNT(*) FILTER (WHERE status = 'QUEUED' AND priority > 5 AND priority <= 20) as p1_count,
        COUNT(*) FILTER (WHERE status = 'QUEUED' AND priority > 20 AND priority <= 60) as p2_count,
        COUNT(*) FILTER (WHERE status = 'QUEUED' AND priority > 60) as p3_count,
        COUNT(*) FILTER (WHERE status = 'PAUSED') as paused_count
      FROM importer_queue;
    `);
    console.log("\nQueue & Priority breakdown:", jobsRes.rows[0]);

    // 6. Last successful jobs
    const lastJobRes = await client.query(`
      SELECT id, task_type, source, status, updated_at, created_at
      FROM importer_queue
      WHERE status = 'COMPLETED'
      ORDER BY updated_at DESC NULLS LAST
      LIMIT 3;
    `);
    console.log("\nLast completed jobs:", JSON.stringify(lastJobRes.rows, null, 2));

    // 7. Active DB connections
    const connRes = await client.query(`
      SELECT count(*) as active_connections
      FROM pg_stat_activity
      WHERE datname = current_database();
    `);
    console.log("\nActive DB connections:", connRes.rows[0]);

    // 8. Staged chapters awaiting barrier
    const stagedRes = await client.query(`
      SELECT count(*) as staged_awaiting_barrier
      FROM importer_chapter_manifest
      WHERE is_staged = true;
    `);
    console.log("\nStaged chapters awaiting barrier:", stagedRes.rows[0]);

    // 9. Sources status breakdown
    const srcStatusRes = await client.query(`
      SELECT status, catalog_discovery_enabled, count(*)
      FROM importer_sources
      GROUP BY status, catalog_discovery_enabled;
    `);
    console.log("\nSources status breakdown:", srcStatusRes.rows);

    // 10. Hourly progression for chapters published over last 8h
    const hourlyRes = await client.query(`
      SELECT
        date_trunc('hour', published_at) as hour,
        count(*) as published_chapters
      FROM chapters
      WHERE published_at >= NOW() - INTERVAL '8 hours'
      GROUP BY hour
      ORDER BY hour ASC;
    `);
    console.log("\nHourly chapters published (last 8h):", hourlyRes.rows);

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
