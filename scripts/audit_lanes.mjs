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
    console.log('=== LANE AUDIT FOR PUBLISHED CHAPTERS ===');

    // For all chapters published in the window, find the priority of the queue job that ingested them
    const laneBreakdown = await client.query(`
      WITH pub_chaps AS (
        SELECT id, work_id, number
        FROM chapters
        WHERE published_at >= $1 AND published_at <= $2
      ),
      chap_jobs AS (
        SELECT 
          pc.id as chapter_id,
          MAX(q.priority) as max_prio
        FROM pub_chaps pc
        LEFT JOIN importer_queue q 
          ON (q.payload->>'workId')::uuid = pc.work_id 
          AND (q.payload->>'chapterNumber')::numeric = pc.number::numeric
          AND q.task_type = 'IMPORT_CHAPTER'
        GROUP BY pc.id
      )
      SELECT 
        CASE 
          WHEN max_prio >= 100 THEN 'P0'
          WHEN max_prio >= 70 AND max_prio < 100 THEN 'P1'
          WHEN max_prio >= 50 AND max_prio < 70 THEN 'P2'
          WHEN max_prio > 0 AND max_prio < 50 THEN 'P3'
          ELSE 'UNCLASSIFIED'
        END as lane,
        count(*) as count
      FROM chap_jobs
      GROUP BY 1
      ORDER BY 2 DESC
    `, [START, END]);

    console.table(laneBreakdown.rows);

    const total = laneBreakdown.rows.reduce((sum, r) => sum + parseInt(r.count, 10), 0);
    console.log('SUM OF LANES:', total);

  } finally {
    client.release();
    await pool.end();
  }
}

run().catch(console.error);
