import dotenv from 'dotenv';
import pg from 'pg';
import fs from 'fs';
dotenv.config();

async function main() {
  const client = new pg.Client({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: true, ca: fs.readFileSync('./config/root.crt').toString() }
  });
  await client.connect();

  const windowStartIso = '2026-09-25T04:42:20.627Z';
  const windowEndIso = '2026-09-25T04:47:58.000Z';

  const res = await client.query(`
    SELECT 
      c.id, 
      c.work_id, 
      c.number, 
      c.published_at,
      q.id as queue_id,
      q.updated_at as queue_completed_at
    FROM chapters c
    JOIN importer_chapter_mappings m ON m.chapter_id = c.id
    LEFT JOIN importer_queue q ON q.source = m.source AND (q.payload->>'sourceChapterId') = m.source_chapter_id AND q.status = 'COMPLETED'
    WHERE c.published_at >= $1 AND c.published_at <= $2
    ORDER BY c.published_at
  `, [windowStartIso, windowEndIso]);

  console.log('Published rows:', res.rows.length);
  // Dedup by chapter id
  const seen = new Set();
  let freshCount = 0;
  let cascadeCount = 0;
  for (const r of res.rows) {
    if (seen.has(r.id)) continue;
    seen.add(r.id);
    const isFresh = r.queue_completed_at && new Date(r.queue_completed_at) >= new Date(windowStartIso) && new Date(r.queue_completed_at) <= new Date(windowEndIso);
    if (isFresh) freshCount++; else cascadeCount++;
    console.log(`Ch ${r.number} | pub: ${r.published_at.toISOString()} | queue_comp: ${r.queue_completed_at ? r.queue_completed_at.toISOString() : 'NULL'} | ${isFresh ? 'FRESH' : 'CASCADE'}`);
  }
  console.log({ totalUniquePublished: seen.size, freshCount, cascadeCount });

  await client.end();
}

main().catch(console.error);
