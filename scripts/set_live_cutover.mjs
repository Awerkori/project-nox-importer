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

async function run() {
  await client.connect();
  const resetPayload = JSON.stringify({
    active: false,
    resumed_at: new Date().toISOString(),
    resumed_by: 'staff_cutover'
  });

  await client.query("UPDATE settings SET value = $1 WHERE key = 'importer_protective_stop'", [resetPayload]);
  await client.query("UPDATE settings SET value = 'true' WHERE key = 'work_affinity_scheduler_enabled'");
  await client.query("UPDATE settings SET value = 'false' WHERE key = 'work_affinity_scheduler_shadow'");

  console.log('Settings updated cleanly for live cutover:');
  const res = await client.query("SELECT key, value FROM settings WHERE key IN ('work_affinity_scheduler_enabled', 'work_affinity_scheduler_shadow', 'importer_protective_stop')");
  console.log(res.rows);
  await client.end();
}

run().catch(console.error);
