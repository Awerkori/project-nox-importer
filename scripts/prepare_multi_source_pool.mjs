import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

const ACTIVE_SOURCES = [
  'fleurblanche',
  'hanamiheaven',
  'mangalivreto',
  'manhastro',
  'taimumangas',
  'montetai',
  'mangaflix',
  'hotcabaretscan',
  'mangaonlinetv',
  'ninjascan',
  'mrtenzus',
  'nebulosascan'
];

async function main() {
  await client.connect();
  console.log('=== PREPARING MULTI-SOURCE POOL FOR SCALABILITY BENCHMARKS ===');

  // 1. Ensure global catalog discovery is DISABLED
  await client.query(`
    INSERT INTO settings (key, value)
    VALUES ('catalog_discovery_enabled', 'DISABLED')
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
  `);

  // 2. Activate sources in importer_sources
  console.log(`Activating ${ACTIVE_SOURCES.length} sources in importer_sources...`);
  for (const s of ACTIVE_SOURCES) {
    await client.query(`
      UPDATE importer_sources
      SET status = 'ACTIVE',
          chapter_ingestion_enabled = true,
          catalog_discovery_enabled = false,
          updated_at = NOW()
      WHERE id = $1
    `, [s]);
  }

  // 3. For each source, ensure at least 80-100 QUEUED jobs are available
  for (const s of ACTIVE_SOURCES) {
    const qCountRes = await client.query(`
      SELECT count(*) as count
      FROM importer_queue
      WHERE source = $1 AND status = 'QUEUED' AND task_type = 'IMPORT_CHAPTER'
    `, [s]);
    const currentQueued = parseInt(qCountRes.rows[0].count, 10);

    if (currentQueued < 80) {
      const needed = 100 - currentQueued;
      const unpauseRes = await client.query(`
        UPDATE importer_queue
        SET status = 'QUEUED', updated_at = NOW(), attempts = 0, last_error = NULL
        WHERE id IN (
          SELECT id FROM importer_queue
          WHERE source = $1 AND status = 'PAUSED_BY_STAFF' AND task_type = 'IMPORT_CHAPTER'
          ORDER BY chapter_sort_key ASC NULLS LAST, id ASC
          LIMIT $2
        )
        RETURNING id
      `, [s, needed]);
      console.log(`  Source [${s}]: unpaused ${unpauseRes.rowCount} jobs (was ${currentQueued}, now ${currentQueued + unpauseRes.rowCount})`);
    } else {
      console.log(`  Source [${s}]: already has ${currentQueued} QUEUED jobs`);
    }
  }

  // 4. Report summary of ready pool
  console.log('\n--- READY MULTI-SOURCE POOL SUMMARY ---');
  const poolSummary = await client.query(`
    SELECT source, count(*) as queued_jobs
    FROM importer_queue
    WHERE status = 'QUEUED' AND task_type = 'IMPORT_CHAPTER'
    GROUP BY source
    ORDER BY queued_jobs DESC
  `);
  console.table(poolSummary.rows);

  const totalQueued = poolSummary.rows.reduce((a, b) => a + parseInt(b.queued_jobs, 10), 0);
  console.log(`Total QUEUED chapters ready across all sources: ${totalQueued}`);

  await client.end();
}

main().catch(err => {
  console.error('Failed to prepare multi-source pool:', err);
  process.exit(1);
});
