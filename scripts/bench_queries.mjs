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

  const sampleMap = await client.query(`
    SELECT work_id, source, source_work_id 
    FROM importer_work_mappings 
    WHERE sync_status = 'SYNCED' AND work_id IS NOT NULL 
    LIMIT 1
  `);
  const sampleWorkId = sampleMap.rows[0]?.work_id;

  // Let's get 5 different work_ids for a realistic batch test
  const sampleWorks = await client.query(`
    SELECT DISTINCT work_id 
    FROM importer_work_mappings 
    WHERE sync_status = 'SYNCED' AND work_id IS NOT NULL 
    LIMIT 5
  `);
  const workIds = sampleWorks.rows.map(r => r.work_id);

  console.log(`Testing with ${workIds.length} works:`, workIds);

  const queries = [
    {
      step: 'Leitura de works (JOIN)',
      table: 'importer_work_mappings + works',
      sql: `SELECT m.id, m.work_id, m.source, m.source_work_id, m.updated_at, w.id, w.title, w.slug, w.published, w.updated_at
            FROM importer_work_mappings m
            INNER JOIN works w ON w.id = m.work_id
            WHERE m.sync_status = 'SYNCED' AND m.work_id IS NOT NULL
            ORDER BY m.updated_at DESC
            LIMIT 15`,
      params: []
    },
    {
      step: 'Leitura de capítulos publicados',
      table: 'chapters',
      sql: `SELECT id, number, published_at 
            FROM chapters 
            WHERE work_id = $1::uuid AND published_at IS NOT NULL`,
      params: [sampleWorkId]
    },
    {
      step: 'Leitura de chapter mappings',
      table: 'importer_chapter_mappings',
      sql: `SELECT chapter_sort_key, chapter_number, status 
            FROM importer_chapter_mappings 
            WHERE work_id = $1::uuid`,
      params: [sampleWorkId]
    },
    {
      step: 'Verificação de fila ativa',
      table: 'importer_queue',
      sql: `SELECT chapter_sort_key, status, payload 
            FROM importer_queue 
            WHERE task_type = 'IMPORT_CHAPTER' 
              AND (payload->>'workId') = $1::text 
              AND status IN ('QUEUED', 'IMPORTING', 'RETRY')`,
      params: [sampleWorkId]
    }
  ];

  const results = [];

  for (const q of queries) {
    const times = [];
    let explainRows = [];
    const explainRes = await client.query(`EXPLAIN (ANALYZE, BUFFERS) ${q.sql}`, q.params);
    explainRows = explainRes.rows.map(r => r['QUERY PLAN']);

    let totalRows = 0;
    for (let i = 0; i < 20; i++) {
      const t0 = performance.now();
      const res = await client.query(q.sql, q.params);
      const t1 = performance.now();
      times.push(t1 - t0);
      totalRows = res.rowCount;
    }

    times.sort((a, b) => a - b);
    const p50 = times[Math.floor(times.length * 0.5)];
    const p95 = times[Math.floor(times.length * 0.95)];
    const max = times[times.length - 1];
    const total = times.reduce((a, b) => a + b, 0);
    const avg = total / times.length;

    // Check plan for scan type
    const planStr = explainRows.join('\n');
    const isIndexScan = planStr.includes('Index Scan') || planStr.includes('Index Only Scan');
    const isSeqScan = planStr.includes('Seq Scan');

    results.push({
      step: q.step,
      table: q.table,
      calls: times.length,
      rowsRead: totalRows,
      rowsWritten: 0,
      roundTrips: 1,
      scanType: isIndexScan ? 'Index Scan' : (isSeqScan ? 'Seq Scan' : 'Other'),
      p50_ms: parseFloat(p50.toFixed(2)),
      p95_ms: parseFloat(p95.toFixed(2)),
      max_ms: parseFloat(max.toFixed(2)),
      avg_ms: parseFloat(avg.toFixed(2)),
      total_exec_time_ms: parseFloat(total.toFixed(2)),
      plan: explainRows
    });
  }

  // Check locks
  const locks = await client.query(`
    SELECT locktype, mode, granted, count(*) 
    FROM pg_locks 
    GROUP BY locktype, mode, granted
  `);

  console.log(JSON.stringify({ results, locks: locks.rows }, null, 2));

  await client.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
