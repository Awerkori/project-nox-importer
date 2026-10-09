import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
});

async function main() {
  const query = `
    WITH candidate_works AS (
        SELECT w.id, w.source
        FROM works w
        WHERE w.published = true
    ),
    source_window AS (
      SELECT DISTINCT source FROM candidate_works
    )
    SELECT
      cw.source,
      candidate.id,
      candidate.priority
    FROM source_window cw
    LEFT JOIN LATERAL (
      SELECT
        q.id,
        q.priority
      FROM importer_queue q
      INNER JOIN candidate_works w ON q.payload->>'workId' = w.id::text
      WHERE (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
        AND q.source = cw.source
      ORDER BY q.priority DESC, q.created_at ASC
      LIMIT 100
    ) candidate ON true
    WHERE candidate.id IS NOT NULL;
  `;
  const res = await pool.query(query);
  console.log("Found jobs:", res.rows.length);
  console.table(res.rows.slice(0, 5));
  pool.end();
}
main().catch(console.error);
