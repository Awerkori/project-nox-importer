import { PGlite } from '@electric-sql/pglite';
import { readFileSync } from 'fs';

async function run() {
  const db = new PGlite();
  await db.exec(`
    CREATE TABLE importer_queue (id uuid PRIMARY KEY, task_type text, status text, payload jsonb, attempts int, max_attempts int, chapter_sort_key numeric, source text, next_run_at timestamptz, priority int, dedupe_key text, locked_by text, locked_at timestamptz, lease_expires_at timestamptz, last_error text);
    CREATE TABLE chapters (work_id uuid, number numeric, published_at timestamptz);
    CREATE TABLE importer_sources (id text PRIMARY KEY, status text, cooldown_until timestamptz, enabled boolean, blocked_reason text, blocked_details jsonb);
    CREATE TABLE works (id uuid PRIMARY KEY, published boolean);
    
    INSERT INTO importer_sources VALUES ('s', 'ACTIVE', NULL, true, NULL, NULL);
    INSERT INTO works VALUES ('00000000-0000-0000-0000-000000000001', true);
    INSERT INTO importer_queue VALUES ('11111111-1111-1111-1111-111111111111', 'IMPORT_CHAPTER', 'QUEUED', '{"workId":"00000000-0000-0000-0000-000000000001","chapterNumber":1}', 0, 7, 1, 's', now(), 100, 'dedupe', NULL, NULL, NULL, NULL);
  `);
  
  const query = `
      WITH dummy_params AS (SELECT $1::text[], $2::int, $3::text, $4::numeric, $5::text, $6::int, $7::text[], $8::text[], $9::text[], $10::int),
      q_candidates AS (
        SELECT id, payload, source, chapter_sort_key, next_run_at, priority, status, task_type, attempts, max_attempts
        FROM importer_queue
        WHERE (status = 'QUEUED' OR (status = 'RETRY' AND next_run_at <= NOW()))
          AND task_type = 'IMPORT_CHAPTER'
          AND attempts < COALESCE(max_attempts, 7)
      ),
      to_lock AS (
        SELECT q_base.id
        FROM q_candidates q
        JOIN importer_queue q_base ON q_base.id = q.id
        JOIN importer_sources s ON s.id = q.source
        LEFT JOIN LATERAL (
          SELECT MAX(c.number) AS max_published
          FROM chapters c
          WHERE c.work_id = (q.payload->>'workId')::uuid
            AND c.published_at IS NOT NULL
        ) pub ON TRUE
        WHERE s.enabled = true
          AND (s.status = 'ACTIVE' OR s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED'))
          AND NOT EXISTS (
            SELECT 1
            FROM chapters canonical_chapter
            WHERE canonical_chapter.work_id = (q.payload->>'workId')::uuid
              AND canonical_chapter.published_at IS NOT NULL
              AND (
                canonical_chapter.number = COALESCE(NULLIF(q.payload->>'chapterNumber', '')::numeric, q.chapter_sort_key)
              )
          )
        ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
        FOR UPDATE OF q_base SKIP LOCKED
        LIMIT 1
      )
      UPDATE importer_queue q_base
      SET status = 'IMPORTING'
      FROM to_lock
      WHERE q_base.id = to_lock.id
      RETURNING q_base.id;
  `;
  const res = await db.query(query, [null, null, null, null, 'worker', 5, null, null, null, null]);
  console.log(res.rows);
}
run();
