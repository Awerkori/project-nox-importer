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

  const res = await client.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
  const works = res.rows[0]?.value || [];

  console.log('=== REAL TIME ACTIVE WORKS STATUS ===');
  for (const w of works) {
    const qRes = await client.query("SELECT count(*)::int as cnt FROM importer_queue WHERE status = 'QUEUED' AND payload->>'workId' = $1", [w.workId]);
    const pRes = await client.query("SELECT count(*)::int as cnt FROM importer_queue WHERE status = 'PAUSED_BY_STAFF' AND payload->>'workId' = $1", [w.workId]);
    const pubRes = await client.query("SELECT count(*)::int as cnt FROM chapters WHERE work_id = $1", [w.workId]);

    const remaining = qRes.rows[0].cnt + pRes.rows[0].cnt;
    console.log(`Work: ${w.workTitle}`);
    console.log(`  ID: ${w.workId}`);
    console.log(`  Source: ${w.primarySource}`);
    console.log(`  Published: ${pubRes.rows[0].cnt} / Total: ${w.totalChapters}`);
    console.log(`  Remaining in Queue (Queued=${qRes.rows[0].cnt}, Paused=${pRes.rows[0].cnt}): ${remaining}`);
    console.log(`  State: ${w.state}`);
  }

  await client.end();
}

run().catch(console.error);
