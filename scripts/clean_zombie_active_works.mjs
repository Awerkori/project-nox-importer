import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  await client.connect();

  // Reset active_works to empty object/array so AdmissionController freshly populates on deploy
  await client.query("UPDATE importer_scheduler_state SET value = '[]'::jsonb, updated_at = NOW() WHERE key = 'active_works'");
  console.log('Reset active_works to [] in importer_scheduler_state');

  // Verify
  const sRes = await client.query("SELECT key, value FROM importer_scheduler_state WHERE key = 'active_works'");
  console.log('Current active_works:', sRes.rows[0]?.value);

  await client.end();
}

main().catch(console.error);
