import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

async function check() {
  const client = new pg.Client({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });
  await client.connect();
  const activeWorks = await client.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
  const works = activeWorks.rows[0]?.value || {};
  const list = Array.isArray(works) ? works : Object.values(works);
  console.log('TOTAL ACTIVE WORKS IN STATE:', list.length);
  for (const w of list) {
    const infCount = await client.query(
      "SELECT count(*) FROM importer_queue WHERE payload->>'workId' = $1 AND status = 'IMPORTING'",
      [w.workId]
    );
    const qSample = await client.query(
      "SELECT source, count(*), min(next_run_at) as min_next_run FROM importer_queue WHERE payload->>'workId' = $1 AND status = 'QUEUED' GROUP BY source",
      [w.workId]
    );
    const details = qSample.rows.map(r => `${r.source}: ${r.count} (min_run: ${r.min_next_run})`).join(', ');
    console.log(`Work: ${w.workId} | State: ${w.state} | InFlight: ${infCount.rows[0].count} | Queued: [${details}]`);
  }
  await client.end();
}
check().catch(console.error);
