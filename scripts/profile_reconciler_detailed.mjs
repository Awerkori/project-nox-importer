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

async function measureQuery(name, sql, params = [], iterations = 10) {
  const times = [];
  let explainOutput = [];
  let rowCount = 0;

  // Run once with EXPLAIN (ANALYZE, BUFFERS)
  const explainRes = await client.query(`EXPLAIN (ANALYZE, BUFFERS) ${sql}`, params);
  explainOutput = explainRes.rows.map(r => r['QUERY PLAN']);

  // Run multiple iterations to get stats
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    const res = await client.query(sql, params);
    const t1 = performance.now();
    times.push(t1 - t0);
    rowCount = res.rowCount;
  }

  times.sort((a, b) => a - b);
  const p50 = times[Math.floor(times.length * 0.5)].toFixed(2);
  const p95 = times[Math.floor(times.length * 0.95)].toFixed(2);
  const max = times[times.length - 1].toFixed(2);
  const total = times.reduce((a, b) => a + b, 0).toFixed(2);
  const avg = (total / times.length).toFixed(2);

  return {
    name,
    calls: iterations,
    rowCount,
    avg,
    p50,
    p95,
    max,
    total,
    plan: explainOutput
  };
}

async function main() {
  await client.connect();

  const sampleMap = await client.query(`
    SELECT work_id, source, source_work_id 
    FROM importer_work_mappings 
    WHERE sync_status = 'SYNCED' AND work_id IS NOT NULL 
    LIMIT 1
  `);
  const sampleWorkId = sampleMap.rows[0]?.work_id;
  console.log(`Sample workId: ${sampleWorkId}`);

  // Check locks currently active
  const locksRes = await client.query(`
    SELECT locktype, mode, granted, count(*) 
    FROM pg_locks 
    GROUP BY locktype, mode, granted
  `);
  console.log('Current locks:', JSON.stringify(locksRes.rows));

  // 1. Leitura de works
  const q1 = await measureQuery(
    '1. Leitura de works (importer_work_mappings JOIN works)',
    `SELECT m.id, m.work_id, m.source, m.source_work_id, m.updated_at, w.id, w.title, w.slug, w.published, w.updated_at
     FROM importer_work_mappings m
     INNER JOIN works w ON w.id = m.work_id
     WHERE m.sync_status = 'SYNCED' AND m.work_id IS NOT NULL
     ORDER BY m.updated_at DESC
     LIMIT 60`
  );

  // 2. Capítulos publicados
  const q2 = await measureQuery(
    '2. Capítulos publicados da obra (chapters)',
    `SELECT id, number, published_at 
     FROM chapters 
     WHERE work_id = $1::uuid AND published_at IS NOT NULL`,
    [sampleWorkId]
  );

  // 3. Chapter mappings da obra
  const q3 = await measureQuery(
    '3. Chapter mappings da obra (importer_chapter_mappings)',
    `SELECT chapter_sort_key, chapter_number, status 
     FROM importer_chapter_mappings 
     WHERE work_id = $1::uuid`,
    [sampleWorkId]
  );

  // 4. Active queue por payload JSON
  const q4 = await measureQuery(
    '4. Active queue (importer_queue by payload->>workId)',
    `SELECT chapter_sort_key, status, payload 
     FROM importer_queue 
     WHERE task_type = 'IMPORT_CHAPTER' 
       AND (payload->>'workId') = $1::text 
       AND status IN ('QUEUED', 'IMPORTING', 'RETRY')`,
    [sampleWorkId]
  );

  console.log(JSON.stringify({ q1, q2, q3, q4 }, null, 2));

  await client.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
