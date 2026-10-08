import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

async function run() {
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  const query = `
      EXPLAIN ANALYZE WITH q_candidates AS (
        SELECT id
        FROM importer_queue
        WHERE (status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW()))
          AND task_type = 'IMPORT_CHAPTER'
          AND attempts < COALESCE(max_attempts, 7)
          AND (
            priority >= 1000
            OR (payload->>'workId') = ANY($1::text[])
          )
      )
      SELECT q.id
      FROM importer_queue q
      JOIN q_candidates qc ON q.id = qc.id
      JOIN importer_sources s ON s.id = q.source
      LEFT JOIN LATERAL (
        SELECT MAX(c.number) AS max_published
        FROM chapters c
        WHERE c.work_id = (q.payload->>'workId')::uuid
          AND c.published_at IS NOT NULL
      ) pub ON TRUE
      FOR UPDATE OF q SKIP LOCKED
  `;
  try {
    const res = await pool.query(query, [[]]);
    console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  } catch (err) {
    console.error(err);
  }
  await pool.end();
}
run();
