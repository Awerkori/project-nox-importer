import fs from 'node:fs';
import pg from 'pg';
import dotenv from 'dotenv';
const envFile = '/home/awerkori/.Projects/project-nox-importer/.env';
const env = dotenv.parse(fs.readFileSync(envFile));
const pool = new pg.Pool({
  host: env.YUGABYTE_HOST, port: Number(env.YUGABYTE_PORT || 5433), user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD, database: env.YUGABYTE_DATABASE,
  ssl: {rejectUnauthorized:true,ca:fs.readFileSync(env.YUGABYTE_SSL_CERT)}, max: 1
});
async function main() {
  const query = `EXPLAIN ANALYZE
        WITH eligible_sources AS MATERIALIZED (
          SELECT s.id
          FROM importer_sources s
          WHERE s.enabled = true
            AND (s.status = 'ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success' = 'true' OR s.blocked_details->>'recovered_at' IS NOT NULL))
        ), all_candidates AS MATERIALIZED (
          SELECT q.*
          FROM importer_queue q
          WHERE q.task_type = 'IMPORT_CHAPTER'
            AND q.status IN ('QUEUED', 'RETRY')
            AND q.priority >= 75 AND q.priority < 100
            AND COALESCE(q.payload->>'staffForced', 'false') <> 'true'
            AND q.payload->>'workId' IS NOT NULL
          ORDER BY q.priority DESC, q.chapter_sort_key ASC
          LIMIT 1000
        ), source_window AS MATERIALIZED (
          SELECT q.*
          FROM (
            SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.source ORDER BY c.priority DESC, c.chapter_sort_key ASC) as rn
            FROM all_candidates c
            WHERE c.source IN (SELECT id FROM eligible_sources)
          ) q
          WHERE q.rn <= 50
        )
        SELECT q.payload->>'workId' AS work_id, q.source, COUNT(*) AS pending_jobs
        FROM source_window q
        WHERE NOT EXISTS (
          SELECT 1
          FROM chapters canonical_chapter
          WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
            AND canonical_chapter.published_at IS NOT NULL
            AND canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
        )
        GROUP BY q.payload->>'workId', q.source
  `;
  try {
    const res = await pool.query(query);
    console.log(res.rows.map(r => r['QUERY PLAN']).join('\n'));
  } catch (e) { console.error(e); }
  pool.end();
}
main().catch(console.error);
