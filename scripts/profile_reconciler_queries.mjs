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

async function explainQuery(name, sql, params = []) {
  console.log(`\n======================================================================`);
  console.log(`QUERY: ${name}`);
  console.log(`SQL: ${sql.slice(0, 150)}...`);
  console.log(`======================================================================`);
  
  const t0 = performance.now();
  try {
    const res = await client.query(`EXPLAIN (ANALYZE, BUFFERS, COSTS) ${sql}`, params);
    const duration = (performance.now() - t0).toFixed(2);
    console.log(`Planning & Execution time: ${duration}ms`);
    console.log('PLAN:');
    res.rows.forEach(r => console.log('  ', r['QUERY PLAN']));
  } catch (err) {
    console.error(`Error explaining ${name}:`, err.message);
  }
}

async function main() {
  await client.connect();

  // 1. Get a sample work_id and source_work_id to test realistic parameters
  const sampleMap = await client.query(`
    SELECT work_id, source, source_work_id 
    FROM importer_work_mappings 
    WHERE sync_status = 'SYNCED' AND work_id IS NOT NULL 
    LIMIT 1
  `);
  const sampleWorkId = sampleMap.rows[0]?.work_id || '00000000-0000-0000-0000-000000000000';
  console.log(`Using sample workId: ${sampleWorkId}`);

  // Query 1: Selection of works for reconciliation
  await explainQuery(
    '1. Seleção de obras (importer_work_mappings JOIN works)',
    `SELECT m.id, m.work_id, m.source, m.source_work_id, m.updated_at, w.id, w.title, w.slug, w.published, w.updated_at
     FROM importer_work_mappings m
     INNER JOIN works w ON w.id = m.work_id
     WHERE m.sync_status = 'SYNCED' AND m.work_id IS NOT NULL
     ORDER BY m.updated_at DESC
     LIMIT 60`
  );

  // Query 2: Published chapters for work
  await explainQuery(
    '2. Capítulos publicados da obra (chapters)',
    `SELECT id, number, published_at 
     FROM chapters 
     WHERE work_id = $1::uuid AND published_at IS NOT NULL`,
    [sampleWorkId]
  );

  // Query 3: Chapter mappings for work
  await explainQuery(
    '3. Chapter mappings da obra (importer_chapter_mappings)',
    `SELECT chapter_sort_key, chapter_number, status 
     FROM importer_chapter_mappings 
     WHERE work_id = $1::uuid`,
    [sampleWorkId]
  );

  // Query 4: Active queue check by JSON payload (payload->>'workId')
  await explainQuery(
    '4. Active queue por payload JSON (payload->>workId) [POTENCIAL SEQ SCAN!]',
    `SELECT chapter_sort_key, status, payload 
     FROM importer_queue 
     WHERE task_type = 'IMPORT_CHAPTER' 
       AND (payload->>'workId') = $1::text 
       AND status IN ('QUEUED', 'IMPORTING', 'RETRY')`,
    [sampleWorkId]
  );

  // Query 5: Upsert importer_chapter_mappings single row
  await explainQuery(
    '5. Upsert importer_chapter_mappings',
    `INSERT INTO importer_chapter_mappings 
       (source, source_chapter_id, work_id, work_mapping_id, chapter_number, chapter_sort_key, page_count, is_page_provider, status, is_gap, last_error)
     VALUES 
       ('hanamiheaven', 'mock-chap-1', $1::uuid, '00000000-0000-0000-0000-000000000000'::uuid, 1, 1000, 10, true, 'PENDING', false, null)
     ON CONFLICT (source, source_chapter_id) DO UPDATE 
     SET work_id = EXCLUDED.work_id`,
    [sampleWorkId]
  );

  // Query 6: Check existing indexes on these tables
  console.log(`\n======================================================================`);
  console.log('INDEXES ON KEY TABLES:');
  console.log(`======================================================================`);
  const indexesRes = await client.query(`
    SELECT tablename, indexname, indexdef 
    FROM pg_indexes 
    WHERE tablename IN ('importer_queue', 'importer_chapter_mappings', 'importer_work_mappings', 'chapters', 'works')
    ORDER BY tablename, indexname
  `);
  indexesRes.rows.forEach(r => console.log(`[${r.tablename}] ${r.indexname} -> ${r.indexdef}`));

  await client.end();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
