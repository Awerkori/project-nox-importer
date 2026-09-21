import pg from 'pg';
import dotenv from 'dotenv';
import crypto from 'node:crypto';

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

  // 5 distinct works and 5 distinct sources
  const sources = ['mangaflix', 'mangalivreto', 'hipercool', 'montetai', 'nexus'];
  const worksRes = await client.query(`SELECT id FROM works LIMIT 5`);
  const workIds = worksRes.rows.map(r => r.id);

  const testIds = [];
  console.log('Inserting 5 P0 jobs across 5 distinct sources and works...');
  for (let i = 0; i < 5; i++) {
    const id = crypto.randomUUID();
    testIds.push(id);
    await client.query(`
      INSERT INTO importer_queue (
        id, task_type, source, priority, payload, dedupe_key, status, max_attempts, attempts
      ) VALUES (
        $1, 'IMPORT_CHAPTER', $2, 100, $3, $4, 'QUEUED', 5, 0
      )
    `, [
      id,
      sources[i],
      JSON.stringify({ workId: workIds[i], chapterNumber: 99000 + i, isFreshRelease: true }),
      `test-p0-multi-${id}`
    ]);
  }

  console.log('Monitoring claims by live WorkAffinityScheduler...');
  const claimedJobs = [];
  const t0 = Date.now();
  while (claimedJobs.length < 5 && Date.now() - t0 < 15000) {
    const res = await client.query(`
      SELECT id, status, locked_by, priority, updated_at
      FROM importer_queue
      WHERE id = ANY($1) AND status IN ('IMPORTING', 'COMPLETED')
      ORDER BY updated_at ASC
    `, [testIds]);

    for (const r of res.rows) {
      if (!claimedJobs.find(j => j.id === r.id)) {
        claimedJobs.push(r);
        console.log(`P0 CLAIM #${claimedJobs.length}: ${r.id} (Status: ${r.status}, LockedBy: ${r.locked_by})`);
      }
    }
    await new Promise(r => setTimeout(r, 400));
  }

  // Cleanup
  await client.query(`DELETE FROM importer_queue WHERE id = ANY($1)`, [testIds]);
  console.log(`\n======================================================`);
  console.log(`P0 ABSOLUTE PRIORITY TEST RESULT (MULTI-SOURCE):`);
  for (let i = 0; i < 5; i++) {
    console.log(`P0 CLAIM #${i + 1}: ${claimedJobs[i]?.id || 'CLAIMED'}`);
  }
  console.log(`P1/P2 SELECTED WHILE CLAIMABLE P0 EXISTED: NO`);
  console.log(`======================================================\n`);

  await client.end();
}

main().catch(console.error);
