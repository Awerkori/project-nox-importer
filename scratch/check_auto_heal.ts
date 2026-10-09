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

  const t0 = Date.now();
  console.log("Querying pipeline queue metrics...");
  const queueRes = await pool.query(`
    SELECT
      (SELECT COUNT(*) FROM importer_queue WHERE status = 'IMPORTING') as importing_cnt,
      (SELECT COUNT(*) FROM importer_queue WHERE status = 'QUEUED' AND task_type = 'IMPORT_CHAPTER') as eligible_cnt,
      (SELECT COUNT(*) FROM importer_queue WHERE status = 'RETRY' AND task_type = 'IMPORT_CHAPTER') as retry_cnt
  `);
  console.log("Queue metrics:", queueRes.rows[0], "Time:", Date.now() - t0);

  const t1 = Date.now();
  console.log("Querying mapping counts...");
  const mappingRes = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE status = 'STAGED' AND work_id IS NOT NULL) as staged_cnt,
      COUNT(DISTINCT work_id) FILTER (WHERE status = 'STAGED' AND work_id IS NOT NULL) as staged_unique_cnt,
      COUNT(*) FILTER (WHERE status = 'WAITING_FOR_GAP' AND work_id IS NOT NULL) as gap_cnt,
      COUNT(*) FILTER (WHERE status = 'STAGED' AND (classification_state = 'FAILED' OR classification_state = 'UNCERTAIN')) as stuck_cnt,
      COUNT(*) FILTER (WHERE status = 'STAGED' AND classification_state = 'CLASSIFIED') as classified_cnt,
      COUNT(*) FILTER (WHERE status = 'STAGED' AND classification_state = 'UNCLASSIFIED') as unclassified_cnt
    FROM importer_chapter_mappings
  `);
  console.log("Mapping metrics:", mappingRes.rows[0], "Time:", Date.now() - t1);

  await pool.end();
}
run();
