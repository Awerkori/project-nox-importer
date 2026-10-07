import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

const START = '2026-09-21T16:35:23.455Z';
const END = '2026-09-21T19:35:40.000Z';

async function run() {
  const client = await pool.connect();
  try {
    console.log('=== FORENSIC FUNNEL AUDIT ===');
    console.log('Window:', START, 'to', END);

    // 1. Total Completed Queue Jobs
    const qComp = await client.query(`
      SELECT count(*) as count, count(DISTINCT ((payload->>'workId') || ':' || chapter_sort_key::text)) as distinct_canonical
      FROM importer_queue
      WHERE status = 'COMPLETED' AND updated_at >= $1 AND updated_at <= $2 AND task_type = 'IMPORT_CHAPTER'
    `, [START, END]);
    const totalQueueCompleted = parseInt(qComp.rows[0].count, 10);
    const distinctCanonicalJobs = parseInt(qComp.rows[0].distinct_canonical, 10);
    const duplicateProviderJobs = totalQueueCompleted - distinctCanonicalJobs;

    console.log('1. TOTAL QUEUE COMPLETED:', totalQueueCompleted);
    console.log('   DISTINCT CANONICAL WORK+CHAPTER IN QUEUE:', distinctCanonicalJobs);
    console.log('   DUPLICATE PROVIDER INGESTIONS:', duplicateProviderJobs);

    // 2. Total Unique Chapters Published in Window
    const pubChaps = await client.query(`
      SELECT count(*) as count
      FROM chapters
      WHERE published_at >= $1 AND published_at <= $2
    `, [START, END]);
    const totalUniquePublished = parseInt(pubChaps.rows[0].count, 10);
    console.log('\n2. TOTAL UNIQUE CANONICAL CHAPTERS PUBLISHED IN WINDOW:', totalUniquePublished);

    // 3. Of the distinct canonical jobs, how many were published during the window?
    const matchedPublished = await client.query(`
      WITH comp_q AS (
        SELECT DISTINCT (payload->>'workId')::uuid as work_id, (payload->>'chapterNumber') as ch_num
        FROM importer_queue
        WHERE status = 'COMPLETED' AND updated_at >= $1 AND updated_at <= $2 AND task_type = 'IMPORT_CHAPTER'
      )
      SELECT count(*) as count
      FROM comp_q q
      JOIN chapters c ON c.work_id = q.work_id AND (c.number = q.ch_num OR c.number::numeric = q.ch_num::numeric)
      WHERE c.published_at >= $1 AND c.published_at <= $2
    `, [START, END]);
    console.log('\n3. JOBS PUBLISHED IN THIS WINDOW:', matchedPublished.rows[0].count);

    // 4. Of the distinct canonical jobs, how many were ALREADY published before start?
    const alreadyPublished = await client.query(`
      WITH comp_q AS (
        SELECT DISTINCT (payload->>'workId')::uuid as work_id, (payload->>'chapterNumber') as ch_num
        FROM importer_queue
        WHERE status = 'COMPLETED' AND updated_at >= $1 AND updated_at <= $2 AND task_type = 'IMPORT_CHAPTER'
      )
      SELECT count(*) as count
      FROM comp_q q
      JOIN chapters c ON c.work_id = q.work_id AND (c.number = q.ch_num OR c.number::numeric = q.ch_num::numeric)
      WHERE c.published_at < $1
    `, [START, END]);
    console.log('4. ALREADY PUBLISHED BEFORE START (Re-ingest/sync):', alreadyPublished.rows[0].count);

    // 5. Of the distinct canonical jobs, how many are currently STAGED?
    const currentlyStaged = await client.query(`
      WITH comp_q AS (
        SELECT DISTINCT (payload->>'workId')::uuid as work_id, chapter_sort_key
        FROM importer_queue
        WHERE status = 'COMPLETED' AND updated_at >= $1 AND updated_at <= $2 AND task_type = 'IMPORT_CHAPTER'
      )
      SELECT count(*) as count
      FROM comp_q q
      JOIN importer_chapter_mappings m ON m.work_id = q.work_id AND m.chapter_sort_key = q.chapter_sort_key
      WHERE m.status = 'STAGED'
    `, [START, END]);
    console.log('5. CURRENTLY HELD IN BUFFER (STAGED):', currentlyStaged.rows[0].count);

    // 6. Currently FAILED or RETRY in queue or mapping
    const failedOrRetry = await client.query(`
      SELECT count(*) as count
      FROM importer_queue
      WHERE updated_at >= $1 AND updated_at <= $2 AND task_type = 'IMPORT_CHAPTER' AND status IN ('FAILED', 'RETRY')
    `, [START, END]);
    console.log('6. FAILED / RETRY IN WINDOW:', failedOrRetry.rows[0].count);

  } finally {
    client.release();
    await pool.end();
  }
}

run().catch(console.error);
