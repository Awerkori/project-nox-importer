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
  max: 2,
  connectionTimeoutMillis: 10000
});

async function main() {
  const client = await pool.connect();
  const report = {};

  try {
    console.log('=== 1. MEASURING CLAIM QUERY LATENCIES (NEW INDEXED SCAN) ===');

    // Test specific work claim (P1 / P2)
    const specificQuery = `
      SELECT q.id, q.payload->>'workId' as work_id, q.chapter_sort_key
      FROM importer_queue q
      JOIN importer_sources s ON s.id = q.source
      WHERE (
        q.status = 'QUEUED'
        OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
      )
        AND q.task_type = 'IMPORT_CHAPTER'
        AND q.attempts < COALESCE(q.max_attempts, 7)
        AND s.enabled = true
        AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
        AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
        AND (q.payload->>'workId') = $2::text
      ORDER BY q.chapter_sort_key ASC NULLS LAST
      LIMIT 1;
    `;

    const specificTimes = [];
    for (let i = 0; i < 10; i++) {
      const t0 = performance.now();
      await client.query(specificQuery, [null, '6708016d-6011-4618-8053-a1062d7ae7ad']);
      specificTimes.push(performance.now() - t0);
    }
    specificTimes.sort((a,b) => a-b);
    report.claim_p50 = specificTimes[Math.floor(specificTimes.length * 0.5)].toFixed(1);
    report.claim_p95 = specificTimes[Math.floor(specificTimes.length * 0.95)].toFixed(1);

    // Test generic claim
    const genericQuery = `
      SELECT q.id, q.payload->>'workId' as work_id, q.chapter_sort_key
      FROM importer_queue q
      JOIN importer_sources s ON s.id = q.source
      WHERE (
        q.status = 'QUEUED'
        OR (q.status = 'RETRY' AND q.next_run_at <= NOW())
      )
        AND q.task_type = 'IMPORT_CHAPTER'
        AND q.attempts < COALESCE(q.max_attempts, 7)
        AND s.enabled = true
        AND (s.status = 'ACTIVE' OR (s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED') AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())))
        AND ($1::text[] IS NULL OR q.source = ANY($1::text[]))
      ORDER BY q.priority DESC, q.chapter_sort_key ASC NULLS LAST, q.next_run_at ASC
      LIMIT 1;
    `;
    const genericTimes = [];
    for (let i = 0; i < 5; i++) {
      const t0 = performance.now();
      await client.query(genericQuery, [['mangaflix', 'manhastro', 'ninjascan', 'taimumangas']]);
      genericTimes.push(performance.now() - t0);
    }
    genericTimes.sort((a,b) => a-b);
    report.generic_claim_p50 = genericTimes[Math.floor(genericTimes.length * 0.5)].toFixed(1);
    report.generic_claim_p95 = genericTimes[Math.floor(genericTimes.length * 0.95)].toFixed(1);

    console.log(`Specific Claim: P50=${report.claim_p50}ms | P95=${report.claim_p95}ms`);
    console.log(`Generic Claim: P50=${report.generic_claim_p50}ms | P95=${report.generic_claim_p95}ms`);

    // 2. Database connections & pool status
    const connRes = await client.query(`
      SELECT count(*) as total,
             count(CASE WHEN state = 'active' THEN 1 END) as active,
             count(CASE WHEN state = 'idle' THEN 1 END) as idle,
             count(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_tx
      FROM pg_stat_activity
      WHERE datname = current_database();
    `);
    report.ysql_connections = connRes.rows[0];

    // 3. Importer Heartbeat
    const hbRes = await client.query(`SELECT value FROM settings WHERE key = 'importer_heartbeat'`);
    report.heartbeat = hbRes.rows[0]?.value ? JSON.parse(hbRes.rows[0].value) : null;

    // 4. Site latencies
    console.log('\n=== 2. MEASURING SITE LATENCIES ===');
    const siteRoutes = [
      { name: 'HOME', url: 'https://manga.project-nox-awerkori.workers.dev/' },
      { name: 'READER', url: 'https://manga.project-nox-awerkori.workers.dev/ler/b797b415-84e1-46ca-bd77-300364e16257' }
    ];

    report.site = {};
    for (const r of siteRoutes) {
      const latencies = [];
      for (let i = 0; i < 4; i++) {
        const t0 = performance.now();
        try {
          const res = await fetch(r.url, { redirect: 'manual' });
          await res.text();
          latencies.push(performance.now() - t0);
        } catch {
          latencies.push(9999);
        }
        await new Promise(res => setTimeout(res, 50));
      }
      latencies.sort((a,b) => a-b);
      report.site[r.name] = {
        p50: latencies[1]?.toFixed(1),
        p95: latencies[3]?.toFixed(1)
      };
      console.log(`${r.name} P50: ${report.site[r.name].p50}ms | P95: ${report.site[r.name].p95}ms`);
    }

    // 5. Recent publications in DB
    const pubRes = await client.query(`
      SELECT count(*) as pub_5m
      FROM chapters
      WHERE published_at >= NOW() - INTERVAL '5 minutes';
    `);
    report.pub_5m = pubRes.rows[0].pub_5m;

    console.log('\nFULL VALIDATION REPORT:');
    console.log(JSON.stringify(report, null, 2));

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
