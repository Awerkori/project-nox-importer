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
  const shardsRes = await client.query(`SELECT count(*) as total, count(*) filter (where enabled = true) as enabled FROM storage_shards WHERE pool_id = '9ad5dac9-c8f7-4774-b488-59837fcef9c3'`);
  const queueRes = await client.query(`SELECT status, count(*) FROM importer_queue GROUP BY status`);
  const connsRes = await client.query(`SELECT state, count(*) FROM pg_stat_activity GROUP BY state`);
  
  console.log('SHARDS:', shardsRes.rows[0]);
  console.log('QUEUE:', queueRes.rows);
  console.log('CONNECTIONS:', connsRes.rows);
  await client.end();
}

run().catch(console.error);
