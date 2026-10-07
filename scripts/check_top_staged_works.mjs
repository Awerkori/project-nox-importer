import pg from 'pg';
import fs from 'fs';

const { Pool } = pg;
const envVars = Object.fromEntries(fs.readFileSync('/home/awerkori/.config/project-nox/yugabyte.env','utf8').split('\n').filter(l=>l.includes('=')&&!l.startsWith('#')).map(l=>[l.split('=')[0].trim(), l.substring(l.indexOf('=')+1).trim().replace(/^["']|["']$/g, '')]));
const pool = new Pool({
  host: envVars.YUGABYTE_HOST,
  port: parseInt(envVars.YUGABYTE_PORT || '5433', 10),
  user: envVars.YUGABYTE_USER,
  password: envVars.YUGABYTE_PASSWORD,
  database: envVars.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 2,
  connectionTimeoutMillis: 10000,
  statement_timeout: 15000
});

async function check(workId, workTitle) {
  console.log(`\n=== CHECKING BARRIER FOR "${workTitle}" (${workId}) ===`);
  const pubRes = await pool.query(`
    SELECT COALESCE(MAX(number), -1) as max_published
    FROM chapters
    WHERE work_id = $1::uuid AND published_at IS NOT NULL
  `, [workId]);
  const maxPublished = parseFloat(pubRes.rows[0]?.max_published ?? '-1');
  console.log(`Max published chapter: ${maxPublished}`);

  const stagedCh = await pool.query(`
    SELECT chapter_id, chapter_number, chapter_sort_key
    FROM importer_chapter_mappings
    WHERE work_id = $1::uuid AND status = 'STAGED'
    ORDER BY chapter_sort_key ASC
    LIMIT 5;
  `, [workId]);

  console.log('Lowest STAGED chapters:');
  for (const ch of stagedCh.rows) {
    const targetSortKey = Number(ch.chapter_sort_key);
    const step = targetSortKey - maxPublished;

    // Check gaps
    const gapCheck = await pool.query(`
      SELECT DISTINCT m.chapter_sort_key, m.status, m.is_gap
      FROM importer_chapter_mappings m
      WHERE m.work_id = $1::uuid
        AND m.chapter_sort_key > $2::numeric
        AND m.chapter_sort_key < $3::numeric
        AND m.is_gap IS NOT TRUE
        AND NOT EXISTS (
          SELECT 1 FROM chapters c
          WHERE c.work_id = m.work_id AND c.number = m.chapter_sort_key AND c.published_at IS NOT NULL
        )
        AND NOT EXISTS (
          SELECT 1 FROM importer_chapter_mappings m2
          WHERE m2.work_id = m.work_id AND m2.chapter_sort_key = m.chapter_sort_key AND m2.status = 'COMPLETED'
        )
      ORDER BY m.chapter_sort_key ASC
      LIMIT 10;
    `, [workId, maxPublished, targetSortKey]);

    const canPublish = targetSortKey <= maxPublished || step <= 1.05 || gapCheck.rows.length === 0;
    console.log(`  Ch ${ch.chapter_number} (sort: ${ch.chapter_sort_key}) -> step: ${step.toFixed(2)} | blocking gaps: ${gapCheck.rows.length} | canPublish: ${canPublish}`);
    if (gapCheck.rows.length > 0) {
      console.log('    Blocking gap keys:', gapCheck.rows.map(r => r.chapter_sort_key));
    }
  }
}

async function main() {
  try {
    await check('d8b402cd-b42a-4e08-98d4-1eace5976642', 'Berserk');
    await check('bd883348-98b9-48da-a2e8-c3a277da5d5d', 'A Vingança do Cão de Caça dos Baskerville');
    await check('0f5cc9e2-8d56-4fae-af23-4e9301c3d19d', 'O imperador está grávido');
    await check('623c6884-749c-4ebf-94ea-3fe9f9fc501c', 'Mago Infinito');
  } finally {
    await pool.end();
  }
}

main().catch(console.error);
