import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

const SOURCE_EXECUTION_ELIGIBILITY_SQL = `(
  (
    s.status = 'ACTIVE'
    AND (
      s.blocked_reason IS NULL
      OR s.blocked_details->>'probe_success' = 'true'
      OR s.blocked_details->>'recovered_at' IS NOT NULL
    )
  )
  OR (
    s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED')
    AND (
      s.blocked_reason IS NULL
      OR s.blocked_details->>'probe_success' = 'true'
      OR s.blocked_details->>'recovered_at' IS NOT NULL
    )
    AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())
  )
)`;

async function run() {
  console.log("Running P0 Canonical Query...");
  const res = await pool.query(`
      SELECT q.id, q.source, q.priority, q.chapter_sort_key
      FROM importer_queue q
      JOIN importer_sources s ON s.id = q.source
      JOIN LATERAL (
        SELECT published FROM works w WHERE w.id = (q.payload->>'workId')::uuid
      ) pub ON true
      WHERE q.task_type = 'IMPORT_CHAPTER'
        AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
        AND q.priority >= 100 AND q.priority < 1000
        AND s.enabled = true
        AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
        AND NOT EXISTS (
          SELECT 1 FROM importer_queue predecessor
          WHERE predecessor.task_type = 'IMPORT_CHAPTER'
            AND predecessor.payload->>'workId' = q.payload->>'workId'
            AND predecessor.chapter_sort_key < q.chapter_sort_key
            AND predecessor.status IN ('QUEUED', 'RETRY', 'IMPORTING')
            AND NOT EXISTS (
              SELECT 1 FROM chapters predecessor_canonical
              WHERE predecessor_canonical.work_id = (q.payload->>'workId')::uuid
                AND predecessor_canonical.published_at IS NOT NULL
                AND predecessor_canonical.sort_key = predecessor.chapter_sort_key
            )
        )
      LIMIT 10
  `);
  console.log('P0 Canonical Result:', res.rows);

  console.log("Running P1 Canonical Query...");
  const resP1 = await pool.query(`
      SELECT q.id, q.source, q.priority, q.chapter_sort_key
      FROM importer_queue q
      JOIN importer_sources s ON s.id = q.source
      JOIN LATERAL (
        SELECT published FROM works w WHERE w.id = (q.payload->>'workId')::uuid
      ) w ON true
      WHERE q.task_type = 'IMPORT_CHAPTER'
        AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
        AND q.attempts < COALESCE(q.max_attempts, 7)
        AND q.priority >= 75 AND q.priority < 100
        AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
        AND w.published IS FALSE
        AND s.enabled = true
        AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
      LIMIT 10
  `);
  console.log('P1 Canonical Result:', resP1.rows);

  await pool.end();
}
run().catch(console.error);
