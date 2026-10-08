import { config } from 'dotenv';
config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });
import { Client } from 'pg';
import fs from 'fs';

async function main() {
  const db = new Client({
    host: process.env.YUGABYTE_HOST, port: parseInt(process.env.YUGABYTE_PORT),
    user: process.env.YUGABYTE_USER, password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { ca: fs.readFileSync(process.env.YUGABYTE_SSL_CERT).toString(), rejectUnauthorized: false }
  });
  await db.connect();

  const p1Works = await db.query(`
    SELECT DISTINCT q.payload->>'workId' as work_id
    FROM importer_queue q
    WHERE q.task_type = 'IMPORT_CHAPTER' AND q.status = 'QUEUED' AND q.priority >= 75 AND q.priority < 100
  `);
  
  console.log(`Total P1 Works: ${p1Works.rows.length}`);
  
  let blockedByGap = 0;
  let executable = 0;

  for (const row of p1Works.rows) {
    const workId = row.work_id;
    const minQ = await db.query(`SELECT MIN(chapter_sort_key) as min_q FROM importer_queue WHERE payload->>'workId' = $1 AND status = 'QUEUED'`, [workId]);
    const maxP = await db.query(`SELECT COALESCE(MAX(number), -1) as max_p FROM chapters WHERE work_id = $1 AND published_at IS NOT NULL`, [workId]);
    
    const minQueueSort = parseFloat(minQ.rows[0].min_q);
    const maxPub = parseFloat(maxP.rows[0].max_p);
    
    const gapStart = maxPub >= 0 ? maxPub + 1 : 1;
    const gapEnd = minQueueSort - 1;
    
    let hasGap = false;
    if (minQueueSort > (maxPub === -1 ? 1.5 : maxPub + 1.5)) {
      const gaps = await db.query(`SELECT 1 FROM importer_confirmed_gaps WHERE work_id = $1 AND start_sort_key <= $2 AND end_sort_key >= $3`, [workId, gapStart, gapEnd]);
      if (gaps.rows.length === 0) {
         hasGap = true;
      }
    }
    
    if (hasGap) {
      blockedByGap++;
    } else {
      executable++;
    }
  }
  
  console.log(`Blocked by unconfirmed Gap: ${blockedByGap}`);
  console.log(`Potentially Executable: ${executable}`);
  process.exit(0);
}
main().catch(console.error);
