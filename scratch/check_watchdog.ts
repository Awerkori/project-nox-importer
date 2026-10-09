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

  console.log("Querying stale importing jobs...");
  const t0 = Date.now();
  const staleRes = await pool.query(
    `SELECT COUNT(*) as stale_importing_cnt
     FROM importer_queue 
     WHERE status = 'IMPORTING'
       AND (
         lease_expires_at <= NOW()
         OR (lease_expires_at IS NULL AND (locked_at <= NOW() - INTERVAL '5 minutes' OR updated_at <= NOW() - INTERVAL '5 minutes'))
         OR (locked_at <= NOW() - INTERVAL '15 minutes' AND updated_at <= NOW() - INTERVAL '15 minutes')
       )`
  );
  console.log("Stale query:", staleRes.rows[0], "Time:", Date.now() - t0);

  console.log("Querying new work pipeline metrics...");
  const t1 = Date.now();
  const nwRes = await pool.query(`
    SELECT 
      (SELECT MAX(created_at) FROM works) as last_created,
      (SELECT MAX(updated_at) FROM importer_work_mappings WHERE sync_status = 'ACTIVE') as last_admitted,
      (SELECT COUNT(*) FROM importer_work_mappings WHERE sync_status = 'WAITING_ADMISSION') as waiting_admission_cnt
  `);
  console.log("NW query:", nwRes.rows[0], "Time:", Date.now() - t1);

  await pool.end();
}
run();
