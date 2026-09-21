import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;
const client = new Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  await client.connect();

  // 1. Total QUEUED jobs
  const totalQueuedRes = await client.query("SELECT count(*) as count FROM importer_queue WHERE status = 'QUEUED'");
  console.log(`TOTAL QUEUED JOBS: ${totalQueuedRes.rows[0].count}`);

  // 2. By task_type
  const byTaskType = await client.query(`
    SELECT task_type, count(*) as count
    FROM importer_queue
    WHERE status = 'QUEUED'
    GROUP BY task_type
    ORDER BY count DESC
  `);
  console.log('\n--- BY TASK_TYPE (QUEUED) ---');
  console.table(byTaskType.rows);

  // 3. By source
  const bySource = await client.query(`
    SELECT source, count(*) as count
    FROM importer_queue
    WHERE status = 'QUEUED'
    GROUP BY source
    ORDER BY count DESC
  `);
  console.log('\n--- BY SOURCE (QUEUED) ---');
  console.table(bySource.rows);

  // 4. Matrix: task_type x source
  const matrix = await client.query(`
    SELECT task_type, source, count(*) as count
    FROM importer_queue
    WHERE status = 'QUEUED'
    GROUP BY task_type, source
    ORDER BY task_type, count DESC
  `);
  console.log('\n--- TASK_TYPE x SOURCE (QUEUED) ---');
  console.table(matrix.rows);

  // 5. Check if any jobs are currently IMPORTING or RETRY
  const otherActive = await client.query(`
    SELECT id, task_type, source, status, locked_by, locked_at, lease_expires_at, progress_current, progress_total
    FROM importer_queue
    WHERE status IN ('IMPORTING', 'RETRY')
    ORDER BY status, updated_at DESC
  `);
  console.log('\n--- JOBS IN IMPORTING OR RETRY ---');
  console.table(otherActive.rows);

  // 6. Check importer_sources status
  const sourcesRes = await client.query(`
    SELECT id, name, status, enabled, sync_interval_minutes, last_sync_at
    FROM importer_sources
    ORDER BY id
  `);
  console.log('\n--- IMPORTER SOURCES STATUS ---');
  console.table(sourcesRes.rows);

  await client.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
