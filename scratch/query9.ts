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

  const resP1 = await pool.query(`
      SELECT q.id, q.source, q.chapter_sort_key, q.payload->>'chapterNumber' as chapter_number
      FROM importer_queue q
      WHERE q.id = '38fe7d1c-473b-4b08-9aaa-b09833c4361c'
  `);
  console.log('Job:', resP1.rows[0]);

  const resChap = await pool.query(`
      SELECT id, number, published_at
      FROM chapters
      WHERE work_id = '106b4c9b-2455-4c37-8723-1f5d7e7fac62'
        AND number = 29
  `);
  console.log('Chapter:', resChap.rows);
  await pool.end();
}
run().catch(console.error);
