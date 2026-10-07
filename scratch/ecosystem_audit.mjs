import pg from 'pg';
import fs from 'fs';
import dotenv from 'dotenv';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: true, ca: fs.readFileSync('./config/root.crt').toString() },
  max: 2
});

const SITE_URL = 'https://manga.project-nox-awerkori.workers.dev';

async function fetchHttp(path) {
  const url = SITE_URL + path;
  const t0 = performance.now();
  try {
    const res = await fetch(url, { headers: { 'User-Agent': 'NoxAuditor/1.0' }, signal: AbortSignal.timeout(10000) });
    const text = await res.text();
    return {
      status: res.status,
      ok: res.ok,
      ttfb: Math.round(performance.now() - t0),
      cfCache: res.headers.get('cf-cache-status'),
      len: text.length,
      text
    };
  } catch (e) {
    return { status: 0, ok: false, error: e.message, ttfb: Math.round(performance.now() - t0) };
  }
}

async function main() {
  console.log('=== AUDITORIA COMPLETA DO ECOSSISTEMA PROJECT NOX ===');
  const client = await pool.connect();

  // 1. HOME AUDIT
  console.log('\n--- 1. AUDITORIA HOME ---');
  const homeRes = await fetchHttp('/');
  console.log('Home HTTP Status:', homeRes.status, '| TTFB:', homeRes.ttfb, 'ms | Cache:', homeRes.cfCache);
  const hasNovasObras = homeRes.text?.includes('Novas Obras') || homeRes.text?.includes('Lançamentos') || homeRes.text?.includes('Recém');
  const hasMaisLidos = homeRes.text?.includes('Mais Lidos') || homeRes.text?.includes('Populares');
  console.log('Home sections: Novas Obras/Lançamentos =', hasNovasObras, '| Mais Lidos/Populares =', hasMaisLidos);

  // 2. WORK & READER AUDIT
  console.log('\n--- 2. AUDITORIA WORK & READER ---');
  const latestChRes = await client.query(`
    SELECT c.id as chapter_id, c.work_id, c.number, c.published_at, w.title as work_title, w.slug as work_slug, w.cover_id
    FROM chapters c
    JOIN works w ON w.id = c.work_id
    WHERE c.published_at IS NOT NULL AND w.published = true
    ORDER BY c.published_at DESC LIMIT 1
  `);
  const lat = latestChRes.rows[0];
  console.log('Latest published chapter:', lat ? `${lat.work_title} #${lat.number} (id: ${lat.chapter_id}, slug: ${lat.work_slug})` : 'NONE');

  if (lat) {
    const workRes = await fetchHttp('/obra/' + lat.work_slug);
    console.log('Work page status:', workRes.status, '| TTFB:', workRes.ttfb, 'ms');

    const readerRes = await fetchHttp('/ler/' + lat.chapter_id);
    console.log('Reader page status:', readerRes.status, '| TTFB:', readerRes.ttfb, 'ms');

    const pagesRes = await client.query('SELECT media_id FROM pages WHERE chapter_id = $1', [lat.chapter_id]);
    console.log('Pages registered in DB for latest chapter:', pagesRes.rows.length);

    const mediaRes = await client.query(`
      SELECT count(*) as total,
             count(CASE WHEN m.storage_ready = true THEN 1 END) as ready,
             count(CASE WHEN m.bytes > 1000 THEN 1 END) as valid_bytes
      FROM pages p
      JOIN media m ON m.id = p.media_id
      WHERE p.chapter_id = $1
    `, [lat.chapter_id]);
    console.log('Media integrity for chapter:', mediaRes.rows[0]);
  }

  // 3. DATA INTEGRITY AUDIT
  console.log('\n--- 3. AUDITORIA DE INTEGRIDADE DE DADOS ---');
  const emptyChRes = await client.query(`
    SELECT count(*) as count 
    FROM chapters c 
    WHERE published_at IS NOT NULL 
      AND NOT EXISTS (SELECT 1 FROM pages p WHERE p.chapter_id = c.id)
  `);
  console.log('Published chapters with 0 pages:', emptyChRes.rows[0].count);

  const dupRes = await client.query(`
    SELECT count(id) - count(DISTINCT (work_id || ':' || number::text)) as duplicate_chapters
    FROM chapters
  `);
  console.log('Duplicate chapters in DB:', dupRes.rows[0].duplicate_chapters);

  const brokenCoversRes = await client.query(`
    SELECT count(*) as count
    FROM works w
    WHERE w.published = true
      AND (w.cover_id IS NULL OR NOT EXISTS (SELECT 1 FROM media m WHERE m.id = w.cover_id AND m.storage_ready = true))
  `);
  console.log('Published works with missing/broken covers:', brokenCoversRes.rows[0].count);

  // 4. IMPORTER QUEUE & SCHEDULER HEALTH
  console.log('\n--- 4. AUDITORIA DO IMPORTER ---');
  const qRes = await client.query(`
    SELECT 
      count(CASE WHEN status = 'IMPORTING' THEN 1 END) as importing,
      count(CASE WHEN status = 'QUEUED' THEN 1 END) as queued,
      count(CASE WHEN status = 'RETRY' THEN 1 END) as retry,
      count(CASE WHEN status = 'FAILED' THEN 1 END) as failed,
      count(CASE WHEN status = 'COMPLETED' AND task_type = 'IMPORT_CHAPTER' AND updated_at >= NOW() - INTERVAL '1 hour' THEN 1 END) as completed_1h
    FROM importer_queue
  `);
  console.log('Queue stats:', qRes.rows[0]);

  const mapRes = await client.query(`
    SELECT status, count(*) as count 
    FROM importer_chapter_mappings 
    WHERE status IN ('STAGED', 'WAITING_FOR_GAP', 'PENDING')
    GROUP BY status
  `);
  console.log('Mappings status:', mapRes.rows);

  const actRes = await client.query("SELECT value FROM importer_scheduler_state WHERE key = 'active_works'");
  let zombies = 0;
  let activeWorks = actRes.rows[0]?.value || [];
  for (const w of activeWorks) {
    if (w.state === 'FILLING' && (w.queuedChapters || 0) === 0 && (w.inFlightChapters || 0) === 0) {
      zombies++;
    }
  }
  console.log('Active works:', activeWorks.length, '| Zombies:', zombies);

  // 5. YUGABYTE HEALTH
  console.log('\n--- 5. AUDITORIA YUGABYTE ---');
  const connRes = await client.query(`
    SELECT 
      count(*) as total,
      count(CASE WHEN state = 'active' THEN 1 END) as active,
      count(CASE WHEN state = 'idle' THEN 1 END) as idle,
      count(CASE WHEN state = 'idle in transaction' THEN 1 END) as idle_in_tx
    FROM pg_stat_activity WHERE datname = current_database()
  `);
  console.log('Connections:', connRes.rows[0]);

  const ybMetrics = await client.query('SELECT metrics FROM yb_servers_metrics LIMIT 1');
  const m = ybMetrics.rows[0]?.metrics || {};
  console.log('YB CPU user/system:', m.cpu_usage_user, '/', m.cpu_usage_system);

  await client.end();
  await pool.end();
}

main().catch(console.error);
