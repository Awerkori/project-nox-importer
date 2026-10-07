import fs from 'node:fs';
import pg from 'pg';
import dotenv from 'dotenv';
const envFile = '/home/awerkori/.Projects/project-nox-importer/.env';
const env = dotenv.parse(fs.readFileSync(envFile));
const pool = new pg.Pool({
  host: env.YUGABYTE_HOST, port: Number(env.YUGABYTE_PORT || 5433), user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD, database: env.YUGABYTE_DATABASE,
  ssl: {rejectUnauthorized:true,ca:fs.readFileSync(env.YUGABYTE_SSL_CERT)},
  max:1
});
async function main() {
  const query = `EXPLAIN ANALYZE
        WITH eligible_sources AS MATERIALIZED (
          SELECT s.id
          FROM importer_sources s
          WHERE s.enabled = true
            AND (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
        ), source_window AS MATERIALIZED (
          SELECT q.*
          FROM importer_queue q
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND q.status IN ('QUEUED', 'RETRY')
            AND q.priority >= 75 AND q.priority < 100
            AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
            AND q.payload->>'workId' IS NOT NULL
            AND q.source IN (SELECT id FROM eligible_sources)
          ORDER BY q.priority DESC, q.chapter_sort_key ASC
          LIMIT 1000
        ), queue_candidates AS MATERIALIZED (
          SELECT q.payload->>'workId' AS work_id, q.source, COUNT(*) AS pending_jobs,
            COUNT(*) FILTER (WHERE q.status = 'QUEUED' OR (q.status = 'RETRY' AND q.next_run_at <= NOW())) AS queued_count,
            COUNT(*) FILTER (WHERE q.status = 'PAUSED_BY_STAFF') AS paused_count,
            MIN(q.chapter_sort_key) AS min_sort_key
          FROM source_window q
          WHERE NOT EXISTS (
            SELECT 1
            FROM chapters canonical_chapter
            WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
              AND canonical_chapter.published_at IS NOT NULL
              AND (
                canonical_chapter.number = NULLIF(q.payload->>'chapterNumber', '')::numeric
                OR canonical_chapter.number = q.chapter_sort_key
              )
          )
          GROUP BY q.payload->>'workId', q.source
        )
        SELECT *
        FROM queue_candidates
        ORDER BY pending_jobs DESC
        LIMIT 10
  `;
  try {
    const res = await pool.query(query);
    console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  } catch (e) { console.error(e); }
  pool.end();
}
main().catch(console.error);
