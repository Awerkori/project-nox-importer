import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
});

async function main() {
  const query = `
    SELECT q.payload->>'workId' AS work_id
    FROM importer_queue q
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND q.status IN ('QUEUED', 'RETRY')
      AND q.priority >= 100 AND q.priority < 1000
    GROUP BY q.payload->>'workId'
  `;
  const res = await pool.query(query);
  console.log(`Found ${res.rows.length} works in queue.`);

  for (const row of res.rows) {
    const workId = row.work_id;
    console.log(`Triggering reconciliation for ${workId}`);
    await pool.query(`
      INSERT INTO importer_work_health (work_id, health_status)
      VALUES ($1, 'RECONCILING')
      ON CONFLICT (work_id) DO UPDATE SET health_status = 'RECONCILING'
    `, [workId]);
  }
  console.log('Inserted health requests for reconciliation.');
  pool.end();
}
main().catch(console.error);
