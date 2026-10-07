import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  const query = `
    SELECT q.id, q.source, (q.payload->>'workId') as work_id, q.chapter_sort_key, q.priority, w.published
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    JOIN importer_sources s ON s.id = q.source
    WHERE (
      q.status = 'QUEUED'
      OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
    )
      AND q.task_type = 'IMPORT_CHAPTER'
      AND q.attempts < COALESCE(q.max_attempts, 7)
      AND w.published = true
      AND s.enabled = true
      AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
    ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
    LIMIT 10;
  `;
  const res = await pool.query(query);
  console.log("Eligible Catalog P1 Jobs:", res.rows.length);
  console.table(res.rows);

  await pool.end();
}

main().catch(console.error);
