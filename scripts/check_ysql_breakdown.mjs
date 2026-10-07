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

async function run() {
  await client.connect();
  const res = await client.query(`
    SELECT 
      count(*) as total,
      count(*) FILTER (WHERE state = 'active') as active,
      count(*) FILTER (WHERE state = 'idle') as idle,
      count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx,
      count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
      count(*) FILTER (WHERE application_name = 'project-nox-importer-direct') as direct_importer
    FROM pg_stat_activity
  `);
  console.log(JSON.stringify(res.rows[0]));
  await client.end();
}
run().catch(console.error);
