import pg from 'pg';
import fs from 'fs';

const envVars = Object.fromEntries(
  fs.readFileSync('/home/awerkori/.config/project-nox/yugabyte.env', 'utf8')
    .split('\n')
    .filter(l => l.includes('=') && !l.startsWith('#'))
    .map(l => [l.split('=')[0].trim(), l.substring(l.indexOf('=') + 1).trim().replace(/^['"]|['"]$/g, '')])
);

const pool = new pg.Pool({
  host: envVars.YUGABYTE_HOST,
  port: parseInt(envVars.YUGABYTE_PORT || '5433', 10),
  user: envVars.YUGABYTE_USER,
  password: envVars.YUGABYTE_PASSWORD,
  database: envVars.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 2
});

async function main() {
  const client = await pool.connect();
  try {
    const s = await client.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
    const activeWorks = Object.values(s.rows[0].value).filter(w => w.lane === 'P1');

    console.log(`Auditing ${activeWorks.length} active P1 works:`);

    for (const w of activeWorks) {
      // 1. Check raw queue candidate
      const cand = await client.query(`
        SELECT q.id, q.chapter_sort_key, q.status, q.attempts, q.priority
        FROM importer_queue q
        WHERE q.status = 'QUEUED'
          AND q.task_type = 'IMPORT_CHAPTER'
          AND (q.payload->>'workId') = $1
        ORDER BY q.chapter_sort_key ASC NULLS LAST
        LIMIT 1
      `, [w.workId]);

      if (cand.rows.length === 0) {
        console.log(`[${w.workTitle?.padEnd(30)}] NO QUEUED CANDIDATE!`);
        continue;
      }

      const c = cand.rows[0];
      const sortKey = c.chapter_sort_key;

      // 2. Check pubCheck
      const pubCheck = await client.query(`
        SELECT 1 FROM chapters 
        WHERE work_id = $1::uuid AND number = $2 AND published_at IS NOT NULL
        LIMIT 1
      `, [w.workId, sortKey]);

      // 3. Check stagedCheck
      const stagedCheck = await client.query(`
        SELECT id, chapter_sort_key FROM importer_chapter_mappings
        WHERE work_id = $1::uuid AND status = 'STAGED' AND chapter_sort_key < $2
        LIMIT 1
      `, [w.workId, sortKey]);

      const isPublished = pubCheck.rows.length > 0;
      const earlierStaged = stagedCheck.rows.length > 0 ? stagedCheck.rows[0].chapter_sort_key : null;

      console.log(`[${w.workTitle?.padEnd(30)}] Next QUEUED: ${sortKey} | Pub: ${isPublished ? 'YES(REDUNDANT)' : 'NO(OK)'} | Barrier: ${earlierStaged ? 'BLOCKED BY ' + earlierStaged : 'CLEAR'}`);
    }
  } finally {
    client.release();
    await pool.end();
  }
}
main().catch(console.error);
