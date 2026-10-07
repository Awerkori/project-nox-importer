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
    const activeWorks = Object.values(s.rows[0].value);

    for (const w of activeWorks) {
      const q = await client.query(`
        SELECT status, count(*) as cnt, min(chapter_sort_key) as min_k, max(chapter_sort_key) as max_k
        FROM importer_queue
        WHERE (payload->>'workId') = $1
        GROUP BY status
      `, [w.workId]);

      const sourceInfo = await client.query(`
        SELECT id, enabled, status, cooldown_until 
        FROM importer_sources 
        WHERE id = $1
      `, [w.primarySource]);

      const src = sourceInfo.rows[0];
      const counts = Object.fromEntries(q.rows.map(r => [r.status, r.cnt]));
      console.log(`[${w.lane}] ${w.workTitle?.padEnd(32)} | Source: ${w.primarySource?.padEnd(14)} (${src?.status}, enabled=${src?.enabled}) | Queue: ${JSON.stringify(counts)}`);
    }
  } finally {
    client.release();
    await pool.end();
  }
}
main().catch(console.error);
