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
  const windowMin = (new Date(windowEndIso).getTime() - new Date(windowStartIso).getTime()) / 60000;

  const compRes = await client.query(`
    SELECT count(*) as count 
    FROM importer_queue 
    WHERE status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' AND updated_at >= $1 AND updated_at <= $2
  `, [windowStartIso, windowEndIso]);

  const pubRes = await client.query(`
    SELECT count(*) as count 
    FROM chapters 
    WHERE published_at >= $1 AND published_at <= $2
  `, [windowStartIso, windowEndIso]);

  const cascadeRes = await client.query(`
    SELECT count(DISTINCT c.id) as count 
    FROM chapters c 
    JOIN importer_chapter_mappings m ON m.chapter_id = c.id 
    WHERE c.published_at >= $1 AND c.published_at <= $2 AND m.created_at < $1
  `, [windowStartIso, windowEndIso]);

  const pubChapters = await client.query(`
    SELECT c.id, c.work_id, c.number, c.published_at, min(m.created_at) as earliest_mapping
    FROM chapters c 
    LEFT JOIN importer_chapter_mappings m ON m.chapter_id = c.id 
    WHERE c.published_at >= $1 AND c.published_at <= $2 
    GROUP BY c.id, c.work_id, c.number, c.published_at
    ORDER BY c.published_at
  `, [windowStartIso, windowEndIso]);

  const stagedRes = await client.query(`
    SELECT count(*) as count 
    FROM importer_chapter_mappings 
    WHERE created_at >= $1 AND created_at <= $2
  `, [windowStartIso, windowEndIso]);

  const startedRes = await client.query(`
    SELECT count(*) as count 
    FROM importer_queue 
    WHERE task_type = 'IMPORT_CHAPTER' AND ((locked_at >= $1 AND locked_at <= $2) OR (status IN ('IMPORTING', 'COMPLETED') AND updated_at >= $1 AND updated_at <= $2))
  `, [windowStartIso, windowEndIso]);

  const integrityRes = await client.query(`
    SELECT 
      count(CASE WHEN c.published_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pages p WHERE p.chapter_id = c.id) THEN 1 END) as empty_pages, 
      count(c.id) - count(DISTINCT (c.work_id || ':' || c.number::text)) as duplicate_chapters 
    FROM chapters c 
    WHERE c.published_at >= $1 AND c.published_at <= $2
  `, [windowStartIso, windowEndIso]);

  console.log({
    windowMin,
    completed: parseInt(compRes.rows[0].count, 10),
    published: parseInt(pubRes.rows[0].count, 10),
    cascade: parseInt(cascadeRes.rows[0].count, 10),
    freshNewVisible: Math.max(0, parseInt(pubRes.rows[0].count, 10) - parseInt(cascadeRes.rows[0].count, 10)),
    staged: parseInt(stagedRes.rows[0].count, 10),
    started: parseInt(startedRes.rows[0].count, 10),
    integrity: integrityRes.rows[0]
  });

  console.log('\nPublished chapters detail:');
  for (const row of pubChapters.rows) {
    const isCascade = row.earliest_mapping && new Date(row.earliest_mapping) < new Date(windowStartIso);
    console.log(`Ch ${row.number} | pub: ${row.published_at.toISOString()} | map: ${row.earliest_mapping ? row.earliest_mapping.toISOString() : 'none'} | cascade: ${isCascade}`);
  }

  await client.end();
}

main().catch(console.error);
