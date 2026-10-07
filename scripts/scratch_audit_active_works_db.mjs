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

  const res = await client.query("SELECT key, value FROM importer_scheduler_state WHERE key = 'active_works'");
  const works = res.rows[0]?.value || [];
  console.log('Total active works:', works.length);
  for (const w of works) {
    console.log(`Work: ${w.workTitle} (${w.workId.slice(0, 8)}) | lane: ${w.lane} | state: ${w.state} | src: ${w.primarySource} | queued: ${w.queuedChapters} | inflight: ${w.inFlightChapters} | critGap: ${w.criticalGapSortKey}`);
  }

  // Check actual queue state for each active work
  console.log('\n--- Real Queue State for Active Works ---');
  for (const w of works) {
    const qRes = await client.query(`
      SELECT 
        COUNT(CASE WHEN status = 'QUEUED' THEN 1 END) as queued_cnt,
        COUNT(CASE WHEN status = 'IMPORTING' THEN 1 END) as importing_cnt,
        MIN(CASE WHEN status = 'QUEUED' THEN chapter_sort_key END) as min_queued
      FROM importer_queue 
      WHERE (payload->>'workId') = $1
    `, [w.workId]);
    console.log(`${w.workTitle} (${w.workId.slice(0, 8)}) | DB queued: ${qRes.rows[0].queued_cnt} | DB importing: ${qRes.rows[0].importing_cnt} | min_queued: ${qRes.rows[0].min_queued}`);
  }

  await client.end();
}

main().catch(console.error);
