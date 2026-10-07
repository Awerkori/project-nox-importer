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

async function probeUrl(url) {
  const start = performance.now();
  try {
    const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 NoxScorecard/1.0' } });
    await res.text();
    const duration = performance.now() - start;
    return { ok: res.ok, status: res.status, durationMs: Math.round(duration) };
  } catch (err) {
    return { ok: false, error: err.message, durationMs: Math.round(performance.now() - start) };
  }
}

async function main() {
  const client = await pool.connect();
  try {
    console.log('=== NOX PRODUCTION SCORECARD LIVE PROBE ===');

    // 1. Site latencies (P50 of 3 samples each)
    const homeSamples = [];
    const readerSamples = [];
    for (let i = 0; i < 3; i++) {
      homeSamples.push((await probeUrl('https://nox.awerkori.com')).durationMs);
      readerSamples.push((await probeUrl('https://nox.awerkori.com/obras')).durationMs);
    }
    homeSamples.sort((a, b) => a - b);
    readerSamples.sort((a, b) => a - b);
    const homeP50 = homeSamples[1];
    const readerP50 = readerSamples[1];

    // 2. YSQL connections
    const connRes = await client.query(`
      SELECT 
        count(*) as total,
        count(CASE WHEN state = 'active' THEN 1 END) as active,
        count(CASE WHEN state = 'idle' THEN 1 END) as idle,
        count(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_tx
      FROM pg_stat_activity 
      WHERE datname = current_database();
    `);

    // 3. Current in-flight jobs
    const inflightRes = await client.query(`
      SELECT q.id, q.source, q.task_type, q.attempts,
             round(EXTRACT(EPOCH FROM (now() - q.locked_at))) as running_sec,
             q.payload->>'chapterNumber' as ch,
             w.title
      FROM importer_queue q
      LEFT JOIN works w ON w.id = (q.payload->>'workId')::uuid
      WHERE q.status = 'IMPORTING'
      ORDER BY q.locked_at ASC;
    `);

    // 4. Queued jobs breakdown
    const queueRes = await client.query(`
      SELECT status, count(*) 
      FROM importer_queue 
      GROUP BY status;
    `);

    // 5. Recent completed jobs (last 10m)
    const completed10m = await client.query(`
      SELECT count(*) as count,
             round(percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (updated_at - locked_at)))) as p50_sec,
             round(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (updated_at - locked_at)))) as p95_sec
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER'
        AND status = 'COMPLETED'
        AND updated_at >= NOW() - INTERVAL '10 minutes'
        AND locked_at IS NOT NULL;
    `);

    // 6. Publications (last 5m and 10m)
    const pub5m = await client.query(`
      SELECT count(*) as count FROM chapters WHERE published_at >= NOW() - INTERVAL '5 minutes';
    `);
    const pub10m = await client.query(`
      SELECT count(*) as count FROM chapters WHERE published_at >= NOW() - INTERVAL '10 minutes';
    `);

    // 7. Timeouts in last 10m
    const timeouts10m = await client.query(`
      SELECT count(*) as count
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER'
        AND updated_at >= NOW() - INTERVAL '10 minutes'
        AND (last_error ILIKE '%timeout%' OR last_error ILIKE '%lease%expired%');
    `);

    // 8. Gap count
    const gapsRes = await client.query(`
      SELECT count(*) as count FROM importer_chapter_mappings WHERE is_gap = true;
    `);

    console.log(`HOME P50: ${homeP50}ms`);
    console.log(`READER P50: ${readerP50}ms`);
    console.log(`YSQL CONNECTIONS: ${connRes.rows[0].total} total | ${connRes.rows[0].active} active | ${connRes.rows[0].idle} idle | ${connRes.rows[0].idle_in_tx} idle in tx`);
    console.log(`IN-FLIGHT IMPORTING JOBS: ${inflightRes.rows.length}`);
    inflightRes.rows.forEach(r => console.log(`  - [${r.source}] ${r.title} ch ${r.ch} (running: ${r.running_sec}s, attempt: ${r.attempts})`));
    console.log(`QUEUE COUNTS:`, queueRes.rows);
    console.log(`COMPLETED LAST 10M: ${completed10m.rows[0].count} (P50: ${completed10m.rows[0].p50_sec}s, P95: ${completed10m.rows[0].p95_sec}s)`);
    console.log(`PUBLISHED LAST 5M: ${pub5m.rows[0].count} (${(parseInt(pub5m.rows[0].count)/5).toFixed(2)}/min)`);
    console.log(`PUBLISHED LAST 10M: ${pub10m.rows[0].count} (${(parseInt(pub10m.rows[0].count)/10).toFixed(2)}/min)`);
    console.log(`TIMEOUTS / LEASE EXPIRED (LAST 10M): ${timeouts10m.rows[0].count}`);
    console.log(`TOTAL IS_GAP=TRUE MAPPINGS IN DB: ${gapsRes.rows[0].count}`);

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
