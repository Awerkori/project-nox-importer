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
  ssl: { rejectUnauthorized: true, ca: fs.readFileSync('./config/root.crt').toString() }
});

async function run() {
  const client = await pool.connect();
  try {
    const activeHealthyWorks = [
      'Imperador Mágico',
      'Com Essa Cara, Você Quer Conquistar Alguém?',
      'O Paraíso de Valentina',
      'Wireless Onahole',
      "Stepmother's Friends",
      'A Loja do Prazer',
      'The Seed of Destiny',
      'Teacher’s Efforts',
      'Maid Rehabilitation',
      'Kangcheol’s Bosses'
    ];
    
    for (const title of activeHealthyWorks) {
      const wRes = await client.query('SELECT id, title FROM works WHERE title = $1', [title]);
      if (wRes.rows.length === 0) continue;
      const work = wRes.rows[0];
      
      const qRes = await client.query(`
        SELECT status, count(*), min(chapter_sort_key) as min_sort, max(chapter_sort_key) as max_sort
        FROM importer_queue
        WHERE (payload->>'workId') = $1
        GROUP BY status;
      `, [work.id]);
      
      const mapRes = await client.query(`
        SELECT status, count(*)
        FROM importer_chapter_mappings
        WHERE work_id = $1::uuid
        GROUP BY status;
      `, [work.id]);

      const chRes = await client.query(`
        SELECT count(*) as published_count, max(number) as max_num
        FROM chapters
        WHERE work_id = $1::uuid AND published_at IS NOT NULL;
      `, [work.id]);

      console.log(`\n=== ${work.title} (${work.id}) ===`);
      console.log('Published:', chRes.rows[0]);
      console.log('Queue:', qRes.rows);
      console.log('Mappings:', mapRes.rows);
    }
  } finally {
    client.release();
    await pool.end();
  }
}
run().catch(console.error);
