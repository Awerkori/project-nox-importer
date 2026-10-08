import { config } from 'dotenv';
config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });
import { Client } from 'pg';
import fs from 'fs';
const db = new Client({
  host: process.env.YUGABYTE_HOST, port: parseInt(process.env.YUGABYTE_PORT),
  user: process.env.YUGABYTE_USER, password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { ca: fs.readFileSync(process.env.YUGABYTE_SSL_CERT).toString(), rejectUnauthorized: false }
});
await db.connect();

const start = Date.now();
try {
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
  await db.query(`SET statement_timeout = 10000;`); // 10 seconds
  const res = await db.query(`
        WITH eligible_sources AS MATERIALIZED (
          SELECT s.id
          FROM importer_sources s
          WHERE s.enabled = true
            AND ${SOURCE_EXECUTION_ELIGIBILITY_SQL}
            AND ($1::text[] IS NULL OR s.id = ANY($1::text[]))
            AND ($3::text[] IS NULL OR NOT (s.id = ANY($3::text[])))
            AND ($6::text[] IS NULL OR s.id = ANY($6::text[]))
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
              AND NOT ((q.payload->>'workId') = ANY($2::text[]))
              AND q.payload->>'workId' IS NOT NULL
              AND NOT ((q.payload->>'workId') = ANY($7::text[]))
            ORDER BY q.priority DESC, q.chapter_sort_key ASC
            LIMIT $5
          ) q
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
                canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
              )
          )
          GROUP BY q.payload->>'workId', q.source
        )
        SELECT * FROM (
            SELECT q.*,
              ROW_NUMBER() OVER (
                PARTITION BY source
                ORDER BY CASE
                  WHEN work_id > COALESCE($4::jsonb ->> source, '') THEN 0
                  ELSE 1
                END,
                work_id
              ) AS rotation_rank,
              ROW_NUMBER() OVER (
                PARTITION BY source
                ORDER BY min_sort_key ASC NULLS LAST, pending_jobs DESC, work_id
              ) AS frontier_rank
            FROM queue_candidates q
          ) ranked
          WHERE rotation_rank <= 4 OR frontier_rank <= 4
        LIMIT 200
  `, [
    null,
    ['00000000-0000-0000-0000-000000000000'],
    null,
    {},
    16,
    null,
    ['00000000-0000-0000-0000-000000000000']
  ]);
  console.log(`Success! Time: ${Date.now() - start}ms`);
} catch (e) {
  console.log(`Failed! Time: ${Date.now() - start}ms`);
  console.log(e.message);
}
process.exit(0);
