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
      EXPLAIN ANALYZE WITH dummy_params AS (SELECT ARRAY['nexus']::text[]),
      eligible_sources AS MATERIALIZED (
        SELECT s.id
        FROM importer_sources s
        WHERE s.enabled = true
          AND s.id = ANY(ARRAY['nexus']::text[])
        ORDER BY s.id ASC
        LIMIT 5
      ),
      source_window AS MATERIALIZED (
        SELECT candidate.id
        FROM eligible_sources s
        CROSS JOIN LATERAL (
          SELECT q.id
          FROM importer_queue q
          WHERE q.source = s.id
            AND q.task_type = 'IMPORT_CHAPTER'
            AND q.status IN ('QUEUED', 'RETRY')
          ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
          LIMIT 160
        ) candidate
      )
      SELECT * FROM source_window;
  `;

  const res = await pool.query(query);
  console.log(res.rows.map(r => r['QUERY PLAN']).join('\\n'));
  await pool.end();
}
run();
