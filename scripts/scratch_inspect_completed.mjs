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
    SELECT q.id, q.source, q.chapter_sort_key, q.updated_at, c.published_at, c.number
    FROM importer_queue q
    LEFT JOIN importer_chapter_mappings m ON m.source = q.source AND m.source_chapter_id = (q.payload->>'sourceChapterId')
    LEFT JOIN chapters c ON c.id = m.chapter_id
    WHERE q.status = 'COMPLETED' AND q.task_type = 'IMPORT_CHAPTER' AND q.updated_at >= $1 AND q.updated_at <= $2
    ORDER BY q.updated_at
  `, [windowStartIso, windowEndIso]);

  console.log('Completed jobs count:', res.rows.length);
  for (const r of res.rows.slice(0, 15)) {
    const pubStatus = r.published_at ? r.published_at.toISOString() : 'NOT_PUBLISHED (STAGED)';
    console.log(`Ch ${r.chapter_sort_key} | src: ${r.source} | comp: ${r.updated_at.toISOString()} | pub: ${pubStatus}`);
  }

  await client.end();
}

main().catch(console.error);
