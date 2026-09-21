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

  console.log('Restoring discovery sources to ACTIVE...');
  const resSources = await client.query(`
    UPDATE importer_sources
    SET status = 'ACTIVE', updated_at = NOW()
    WHERE id IN ('fleurblanche', 'hanamiheaven', 'mangalivreto', 'manhastro', 'nexus', 'taimumangas', 'vegitoons')
    RETURNING id
  `);
  console.log(`Reactivated sources: ${resSources.rows.map(r => r.id).join(', ')}`);

  console.log('Restoring paused discovery queue jobs to QUEUED...');
  const resQueue = await client.query(`
    UPDATE importer_queue
    SET status = 'QUEUED',
        pause_reason = NULL,
        paused_by = NULL,
        paused_at = NULL,
        updated_at = NOW()
    WHERE pause_reason = 'PAUSED_FOR_5W_BENCHMARK'
    RETURNING id, task_type, source
  `);
  console.log(`Restored ${resQueue.rowCount} discovery jobs back to QUEUED.`);

  await client.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
