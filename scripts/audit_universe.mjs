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
    console.log('=== UNIVERSE AND CONVERGENCE RECONCILIATION ===');

    // 1. ACTIVE WORKS (published = true)
    const activeStart = await client.query(`
      WITH w_stats AS (
        SELECT 
          w.id,
          count(c.id) FILTER (WHERE c.published_at < $1::timestamptz) as pub_before,
          count(m.id) FILTER (WHERE (m.status NOT IN ('COMPLETED', 'FAILED') OR m.updated_at >= $1::timestamptz)) as missing_before
        FROM works w
        LEFT JOIN chapters c ON c.work_id = w.id
        LEFT JOIN importer_chapter_mappings m ON m.work_id = w.id
        WHERE w.published = true
        GROUP BY w.id
      )
      SELECT 
        count(*) as total_active_works,
        count(*) FILTER (WHERE pub_before > 0 AND missing_before = 0) as caught_up_start,
        count(*) FILTER (WHERE missing_before > 0) as incomplete_start
      FROM w_stats
    `, [START]);
    console.log('ACTIVE WORKS AT START:', activeStart.rows[0]);

    const activeEnd = await client.query(`
      WITH w_stats AS (
        SELECT 
          w.id,
          count(c.id) FILTER (WHERE c.published_at <= $1::timestamptz) as pub_end,
          count(m.id) FILTER (WHERE m.status NOT IN ('COMPLETED', 'FAILED')) as missing_end
        FROM works w
        LEFT JOIN chapters c ON c.work_id = w.id
        LEFT JOIN importer_chapter_mappings m ON m.work_id = w.id
        WHERE w.published = true
        GROUP BY w.id
      )
      SELECT 
        count(*) as total_active_works,
        count(*) FILTER (WHERE pub_end > 0 AND missing_end = 0) as caught_up_end,
        count(*) FILTER (WHERE missing_end > 0) as incomplete_end
      FROM w_stats
    `, [END]);
    console.log('ACTIVE WORKS AT END:', activeEnd.rows[0]);

    // 2. Canonical missing chapters for ACTIVE works:
    const missingActiveStart = await client.query(`
      SELECT count(DISTINCT (m.work_id, m.chapter_sort_key)) as missing
      FROM importer_chapter_mappings m
      JOIN works w ON w.id = m.work_id AND w.published = true
      WHERE (m.status NOT IN ('COMPLETED', 'FAILED') OR m.updated_at >= $1::timestamptz)
    `, [START]);
    console.log('ACTIVE CANONICAL MISSING AT START:', missingActiveStart.rows[0].missing);

    const missingActiveEnd = await client.query(`
      SELECT count(DISTINCT (m.work_id, m.chapter_sort_key)) as missing
      FROM importer_chapter_mappings m
      JOIN works w ON w.id = m.work_id AND w.published = true
      WHERE m.status NOT IN ('COMPLETED', 'FAILED')
    `);
    console.log('ACTIVE CANONICAL MISSING AT END:', missingActiveEnd.rows[0].missing);

    // 3. ALL WORKS (Entire database: 6720 works)
    const allStart = await client.query(`
      WITH w_stats AS (
        SELECT 
          w.id,
          count(c.id) FILTER (WHERE c.published_at < $1::timestamptz) as pub_before,
          count(m.id) FILTER (WHERE (m.status NOT IN ('COMPLETED', 'FAILED') OR m.updated_at >= $1::timestamptz)) as missing_before
        FROM works w
        LEFT JOIN chapters c ON c.work_id = w.id
        LEFT JOIN importer_chapter_mappings m ON m.work_id = w.id
        GROUP BY w.id
      )
      SELECT 
        count(*) as total_all_works,
        count(*) FILTER (WHERE pub_before > 0 AND missing_before = 0) as all_caught_up_start,
        count(*) FILTER (WHERE missing_before > 0) as all_incomplete_start
      FROM w_stats
    `, [START]);
    console.log('\nALL WORKS AT START:', allStart.rows[0]);

    const allEnd = await client.query(`
      WITH w_stats AS (
        SELECT 
          w.id,
          count(c.id) FILTER (WHERE c.published_at <= $1::timestamptz) as pub_end,
          count(m.id) FILTER (WHERE m.status NOT IN ('COMPLETED', 'FAILED')) as missing_end
        FROM works w
        LEFT JOIN chapters c ON c.work_id = w.id
        LEFT JOIN importer_chapter_mappings m ON m.work_id = w.id
        GROUP BY w.id
      )
      SELECT 
        count(*) as total_all_works,
        count(*) FILTER (WHERE pub_end > 0 AND missing_end = 0) as all_caught_up_end,
        count(*) FILTER (WHERE missing_end > 0) as all_incomplete_end
      FROM w_stats
    `, [END]);
    console.log('ALL WORKS AT END:', allEnd.rows[0]);

    // 4. Canonical missing ALL works:
    const missingAllStart = await client.query(`
      SELECT count(DISTINCT (m.work_id, m.chapter_sort_key)) as missing
      FROM importer_chapter_mappings m
      WHERE (m.status NOT IN ('COMPLETED', 'FAILED') OR m.updated_at >= $1::timestamptz)
    `, [START]);
    console.log('ALL CANONICAL MISSING AT START:', missingAllStart.rows[0].missing);

    const missingAllEnd = await client.query(`
      SELECT count(DISTINCT (m.work_id, m.chapter_sort_key)) as missing
      FROM importer_chapter_mappings m
      WHERE m.status NOT IN ('COMPLETED', 'FAILED')
    `);
    console.log('ALL CANONICAL MISSING AT END:', missingAllEnd.rows[0].missing);

  } finally {
    client.release();
    await pool.end();
  }
}

run().catch(console.error);
