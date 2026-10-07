import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: {
    rejectUnauthorized: true,
    ca: fs.readFileSync('./config/root.crt').toString()
  }
});

async function main() {
  const client = await pool.connect();
  try {
    console.log("=== EXPLAIN ANALYZE: Query 1 (Staged Unique) ===");
    const q1 = await client.query(`
      EXPLAIN ANALYZE
      SELECT count(DISTINCT (work_id || ':' || chapter_sort_key::text)) as staged_unique
      FROM importer_chapter_mappings
      WHERE status IN ('STAGED', 'WAITING_FOR_GAP');
    `);
    console.log(q1.rows.map(r => r['QUERY PLAN']).join('\n'));

    console.log("\n=== EXPLAIN ANALYZE: Query 2 (Publishable Staged) ===");
    const q2 = await client.query(`
      EXPLAIN ANALYZE
      WITH staged_works AS (
        SELECT work_id, MIN(chapter_sort_key) as min_staged
        FROM importer_chapter_mappings
        WHERE status IN ('STAGED', 'WAITING_FOR_GAP') AND work_id IS NOT NULL
        GROUP BY work_id
      ),
      publishable_works AS (
        SELECT sw.work_id, COALESCE((
          SELECT MAX(number) FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL
        ), -1) as max_published
        FROM staged_works sw
        WHERE sw.min_staged <= COALESCE((
          SELECT MAX(number) FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL
        ), -1) + 1.05
        OR NOT EXISTS (
          SELECT 1 FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL
        )
        LIMIT 40
      )
      SELECT COUNT(*) as publishable_staged
      FROM importer_chapter_mappings m
      JOIN publishable_works pw ON m.work_id = pw.work_id
      WHERE m.status IN ('STAGED', 'WAITING_FOR_GAP')
        AND (m.chapter_sort_key <= pw.max_published + 1.05 OR pw.max_published = -1);
    `);
    console.log(q2.rows.map(r => r['QUERY PLAN']).join('\n'));

    console.log("\n=== EXPLAIN ANALYZE: Query 3 (Correlated Dedupe) ===");
    const q3 = await client.query(`
      EXPLAIN ANALYZE
      WITH recent_completed_jobs AS (
        SELECT 
          q.id,
          q.source,
          q.chapter_sort_key,
          (q.payload->>'workId')::uuid as work_id,
          (q.payload->>'chapterId')::uuid as chapter_id,
          q.last_error,
          q.updated_at
        FROM importer_queue q
        WHERE q.status = 'COMPLETED'
          AND q.task_type = 'IMPORT_CHAPTER'
          AND q.updated_at >= NOW() - INTERVAL '30 minutes'
        ORDER BY q.updated_at DESC
        LIMIT 50
      ),
      correlated AS (
        SELECT DISTINCT ON (j.id)
          j.id as job_id,
          j.work_id,
          j.chapter_sort_key,
          j.source,
          CASE
            WHEN j.last_error IN ('CANONICAL_ALREADY_SATISFIED', 'ALREADY_CANONICAL') THEN 'ALREADY_CANONICAL'
            WHEN m.is_page_provider IS FALSE THEN 'DEDUPE_SOURCE'
            WHEN c.published_at IS NOT NULL AND c.is_fresh_release IS TRUE THEN 'FRESH_PUBLISHED'
            WHEN c.published_at IS NOT NULL THEN 'ALREADY_CANONICAL'
            ELSE 'FRESH_EXPECTED'
          END as classification
        FROM recent_completed_jobs j
        LEFT JOIN importer_chapter_mappings m 
          ON m.work_id = j.work_id 
         AND m.source = j.source 
         AND m.chapter_sort_key = j.chapter_sort_key
        LEFT JOIN chapters c 
          ON c.id = COALESCE(j.chapter_id, m.chapter_id)
      )
      SELECT 
        classification,
        count(*) as cnt
      FROM correlated
      GROUP BY classification;
    `);
    console.log(q3.rows.map(r => r['QUERY PLAN']).join('\n'));

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
