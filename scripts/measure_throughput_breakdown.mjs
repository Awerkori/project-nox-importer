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

async function run() {
  const client = await pool.connect();
  try {
    // 1. Every chapter published in last 15 minutes, deduplicated by c.id
    const resPub = await client.query(`
      SELECT c.id as chapter_id, c.work_id, c.number, c.published_at, c.created_at as ch_created_at,
             MAX(q.updated_at) as latest_queue_completed_at,
             MIN(m.created_at) as earliest_mapping_created_at,
             MAX(m.updated_at) as latest_mapping_updated_at
      FROM chapters c
      LEFT JOIN importer_chapter_mappings m ON m.chapter_id = c.id
      LEFT JOIN importer_queue q ON q.task_type = 'IMPORT_CHAPTER'
        AND (q.payload->>'workId') = c.work_id::text
        AND (q.payload->>'chapterNumber')::numeric = c.number
        AND q.status = 'COMPLETED'
      WHERE c.published_at >= NOW() - INTERVAL '15 minutes'
      GROUP BY c.id, c.work_id, c.number, c.published_at, c.created_at
      ORDER BY c.published_at DESC;
    `);

    let newImport15 = 0;
    let cascade15 = 0;
    let newImport5 = 0;
    let cascade5 = 0;

    const fiveMinAgo = Date.now() - 5 * 60 * 1000;
    const details = [];

    for (const r of resPub.rows) {
      const pubTime = new Date(r.published_at).getTime();
      const qTime = r.latest_queue_completed_at ? new Date(r.latest_queue_completed_at).getTime() : 0;
      
      // If queue job was completed within 180s of published_at, it was freshly imported and published immediately!
      // If queue job completed earlier, it sat in STAGED and was released by cascade.
      const diffSec = qTime > 0 ? Math.abs(pubTime - qTime) / 1000 : 999999;
      const isNew = diffSec <= 180;

      if (isNew) {
        newImport15++;
        if (pubTime >= fiveMinAgo) newImport5++;
      } else {
        cascade15++;
        if (pubTime >= fiveMinAgo) cascade5++;
      }

      details.push({
        work_id: r.work_id.slice(0, 8),
        ch: r.number,
        pub: r.published_at.toISOString().slice(11, 19),
        q_comp: r.latest_queue_completed_at ? r.latest_queue_completed_at.toISOString().slice(11, 19) : 'none',
        diff_s: qTime > 0 ? Math.round(diffSec) : 'N/A',
        type: isNew ? 'NEW_IMPORT' : 'CASCADE_STAGED'
      });
    }

    // Additional pipeline metrics in last 15m and 5m:
    // 1. CLAIMED: jobs that moved into IMPORTING or COMPLETED in window
    const claimed15 = await client.query(`
      SELECT count(*) as count
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER'
        AND updated_at >= NOW() - INTERVAL '15 minutes'
        AND status IN ('IMPORTING', 'COMPLETED');
    `);
    const claimed5 = await client.query(`
      SELECT count(*) as count
      FROM importer_queue
      WHERE task_type = 'IMPORT_CHAPTER'
        AND updated_at >= NOW() - INTERVAL '5 minutes'
        AND status IN ('IMPORTING', 'COMPLETED');
    `);

    // 2. STORED / DOWNLOADED: distinct chapters created in chapters table
    const stored15 = await client.query(`
      SELECT count(*) as count
      FROM chapters
      WHERE created_at >= NOW() - INTERVAL '15 minutes';
    `);
    const stored5 = await client.query(`
      SELECT count(*) as count
      FROM chapters
      WHERE created_at >= NOW() - INTERVAL '5 minutes';
    `);

    // 3. NEW STAGED: mappings set to STAGED in window
    const staged15 = await client.query(`
      SELECT count(*) as count
      FROM importer_chapter_mappings
      WHERE status = 'STAGED' AND updated_at >= NOW() - INTERVAL '15 minutes';
    `);
    const staged5 = await client.query(`
      SELECT count(*) as count
      FROM importer_chapter_mappings
      WHERE status = 'STAGED' AND updated_at >= NOW() - INTERVAL '5 minutes';
    `);

    console.log('=== PIPELINE BREAKDOWN (LAST 15 MINUTES) ===');
    console.log(`CLAIMED/MIN:      ${(parseInt(claimed15.rows[0].count) / 15).toFixed(2)} (${claimed15.rows[0].count} total)`);
    console.log(`DOWNLOADED/MIN:   ${(parseInt(stored15.rows[0].count) / 15).toFixed(2)} (${stored15.rows[0].count} total stored)`);
    console.log(`STORED/MIN:       ${(parseInt(stored15.rows[0].count) / 15).toFixed(2)}`);
    console.log(`NEW STAGED/MIN:   ${(parseInt(staged15.rows[0].count) / 15).toFixed(2)} (${staged15.rows[0].count} total)`);
    console.log(`NEW PUBLISHED/MIN: ${(newImport15 / 15).toFixed(2)} (${newImport15} total)`);
    console.log(`NEW VISIBLE/MIN:   ${(newImport15 / 15).toFixed(2)}`);
    console.log(`CASCADE PUBLISHED/MIN: ${(cascade15 / 15).toFixed(2)} (${cascade15} total)`);
    console.log(`TOTAL VISIBLE/MIN: ${(resPub.rows.length / 15).toFixed(2)} (${resPub.rows.length} total)`);

    const total5 = newImport5 + cascade5;
    console.log('\n=== PIPELINE BREAKDOWN (LAST 5 MINUTES) ===');
    console.log(`CLAIMED/MIN:      ${(parseInt(claimed5.rows[0].count) / 5).toFixed(2)} (${claimed5.rows[0].count} total)`);
    console.log(`DOWNLOADED/MIN:   ${(parseInt(stored5.rows[0].count) / 5).toFixed(2)} (${stored5.rows[0].count} total stored)`);
    console.log(`STORED/MIN:       ${(parseInt(stored5.rows[0].count) / 5).toFixed(2)}`);
    console.log(`NEW STAGED/MIN:   ${(parseInt(staged5.rows[0].count) / 5).toFixed(2)} (${staged5.rows[0].count} total)`);
    console.log(`NEW PUBLISHED/MIN: ${(newImport5 / 5).toFixed(2)} (${newImport5} total)`);
    console.log(`NEW VISIBLE/MIN:   ${(newImport5 / 5).toFixed(2)}`);
    console.log(`CASCADE PUBLISHED/MIN: ${(cascade5 / 5).toFixed(2)} (${cascade5} total)`);
    console.log(`TOTAL VISIBLE/MIN: ${(total5 / 5).toFixed(2)} (${total5} total)`);

    console.log('\n--- SAMPLE DETAILS (FIRST 15) ---');
    console.table(details.slice(0, 15));

  } finally {
    client.release();
    await pool.end();
  }
}

run().catch(console.error);
