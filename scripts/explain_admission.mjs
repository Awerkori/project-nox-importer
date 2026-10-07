import fs from 'node:fs';
import pg from 'pg';
import dotenv from 'dotenv';

const envFile = '/home/awerkori/.Projects/project-nox-importer/.env';
const env = dotenv.parse(fs.readFileSync(envFile));

const pool = new pg.Pool({
  host: env.YUGABYTE_HOST, port: Number(env.YUGABYTE_PORT || 5433), user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD, database: env.YUGABYTE_DATABASE,
  ssl: {rejectUnauthorized:true,ca:fs.readFileSync(env.YUGABYTE_SSL_CERT)},
  max:1, connectionTimeoutMillis:5000, query_timeout:30000
});

async function main() {
  const query = `EXPLAIN ANALYZE
    WITH eligible_sources AS MATERIALIZED (
      SELECT s.id
      FROM importer_sources s
      WHERE s.enabled = true
        AND (
          (
            s.status = 'ACTIVE'
            AND (
              s.blocked_reason IS NULL
              OR s.blocked_details->>'probe_success' = 'true'
              OR s.blocked_details->>'recovered_at' IS NOT NULL
            )
          )
        )
    ), source_window AS MATERIALIZED (
      SELECT q.*
      FROM eligible_sources s
      CROSS JOIN LATERAL (
        SELECT q.*
        FROM importer_queue q
        WHERE q.source = s.id
          AND q.task_type = 'IMPORT_CHAPTER'
          AND (q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW()))
          AND q.attempts < COALESCE(q.max_attempts,7)
          AND q.priority >= 75 AND q.priority < 100
          AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
          AND q.payload->>'workId' IS NOT NULL
        ORDER BY q.created_at ASC NULLS LAST, q.id ASC
        LIMIT 50
      ) q
    )
    SELECT * FROM source_window
  `;
  try {
    const res = await pool.query(query);
    console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  } catch (e) {
    console.error(e);
  }
  pool.end();
}
main().catch(console.error);
