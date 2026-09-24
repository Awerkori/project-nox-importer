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
  try {
    console.log('=== STARTING SURGICAL GAP RESOLUTION & UNBLOCKER BOOST ===');

    // 1. Berserk (171 staged chapters blocked by 126, 127, 128)
    const berserkId = 'd8b402cd-b42a-4e08-98d4-1eace5976642';
    console.log('\n--- 1. Resolving Berserk gaps (126, 127, 128) ---');
    await client.query(`
      UPDATE importer_chapter_mappings
      SET is_gap = true, status = 'COMPLETED', updated_at = NOW(),
          last_error = 'EXPLICIT_PERMANENT_GAP_RESOLVED'
      WHERE work_id = $1 AND chapter_sort_key IN (126, 127, 128);
    `, [berserkId]);
    await client.query(`
      UPDATE importer_queue
      SET status = 'COMPLETED', updated_at = NOW(), last_error = 'EXPLICIT_PERMANENT_GAP_RESOLVED'
      WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1 AND chapter_sort_key IN (126, 127, 128);
    `, [berserkId]);
    console.log('Berserk gaps 126-128 marked as resolved.');

    // 2. Baskerville (107 staged chapters blocked by missing 1-86)
    const baskervilleId = 'bd883348-98b9-48da-a2e8-c3a277da5d5d';
    console.log('\n--- 2. Resolving Baskerville missing catalog gap (1 to 86) ---');
    const wmRes = await client.query('SELECT id, source FROM importer_work_mappings WHERE work_id = $1 LIMIT 1', [baskervilleId]);
    const wm = wmRes.rows[0];
    if (wm) {
      for (let ch = 1; ch <= 86; ch++) {
        await client.query(`
          INSERT INTO importer_chapter_mappings (
            source, source_chapter_id, work_id, work_mapping_id, chapter_number, chapter_sort_key,
            page_count, is_page_provider, status, is_gap, updated_at
          ) VALUES (
            $1, $2, $3, $4, $5, $5, 0, false, 'COMPLETED', true, NOW()
          )
          ON CONFLICT (source, source_chapter_id) DO UPDATE
          SET is_gap = true, status = 'COMPLETED', updated_at = NOW();
        `, [wm.source, `gap_${baskervilleId}_${ch}`, baskervilleId, wm.id, ch]);
      }
      console.log('Baskerville gaps 1-86 registered cleanly.');
    }

    // 3. Eu sou o Vilão Predestinado (46 staged chapters blocked by 75, 76)
    const vilaoId = '8c870b74-8bd3-448e-96b4-5e28e7626e2e';
    console.log('\n--- 3. Resolving Vilão Predestinado gaps (75, 76) ---');
    await client.query(`
      UPDATE importer_chapter_mappings
      SET is_gap = true, status = 'COMPLETED', updated_at = NOW(),
          last_error = 'EXPLICIT_PERMANENT_GAP_RESOLVED'
      WHERE work_id = $1 AND chapter_sort_key IN (75, 76);
    `, [vilaoId]);
    await client.query(`
      UPDATE importer_queue
      SET status = 'COMPLETED', updated_at = NOW(), last_error = 'EXPLICIT_PERMANENT_GAP_RESOLVED'
      WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1 AND chapter_sort_key IN (75, 76);
    `, [vilaoId]);
    console.log('Vilão Predestinado gaps 75-76 marked as resolved.');

    // 4. My Dragon System (70 staged chapters blocked by 90)
    const dragonId = 'a66d42a1-cf7a-4481-9c8d-a88f2072704f';
    console.log('\n--- 4. Resolving My Dragon System gap (90) ---');
    await client.query(`
      UPDATE importer_chapter_mappings
      SET is_gap = true, status = 'COMPLETED', updated_at = NOW(),
          last_error = 'EXPLICIT_PERMANENT_GAP_RESOLVED'
      WHERE work_id = $1 AND chapter_sort_key = 90;
    `, [dragonId]);
    await client.query(`
      UPDATE importer_queue
      SET status = 'COMPLETED', updated_at = NOW(), last_error = 'EXPLICIT_PERMANENT_GAP_RESOLVED'
      WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1 AND chapter_sort_key = 90;
    `, [dragonId]);
    console.log('My Dragon System gap 90 marked as resolved.');

    // 5. The Demon of Vengeance (29 staged chapters blocked by 3)
    const demonId = '29ab3c9b-9716-48a5-b08c-bcb333b4c210';
    console.log('\n--- 5. Resolving The Demon of Vengeance gap (3) ---');
    await client.query(`
      UPDATE importer_chapter_mappings
      SET is_gap = true, status = 'COMPLETED', updated_at = NOW(),
          last_error = 'EXPLICIT_PERMANENT_GAP_RESOLVED'
      WHERE work_id = $1 AND chapter_sort_key = 3;
    `, [demonId]);
    await client.query(`
      UPDATE importer_queue
      SET status = 'COMPLETED', updated_at = NOW(), last_error = 'EXPLICIT_PERMANENT_GAP_RESOLVED'
      WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1 AND chapter_sort_key = 3;
    `, [demonId]);
    console.log('The Demon of Vengeance gap 3 marked as resolved.');

    // 6. Top Tier Providence (28 staged chapters blocked by 183)
    const ttpId = 'd8f8e05d-a103-400f-b963-7c8c7453aa67';
    console.log('\n--- 6. Resolving Top Tier Providence gap (183) ---');
    await client.query(`
      UPDATE importer_chapter_mappings
      SET is_gap = true, status = 'COMPLETED', updated_at = NOW(),
          last_error = 'EXPLICIT_PERMANENT_GAP_RESOLVED'
      WHERE work_id = $1 AND chapter_sort_key = 183;
    `, [ttpId]);
    await client.query(`
      UPDATE importer_queue
      SET status = 'COMPLETED', updated_at = NOW(), last_error = 'EXPLICIT_PERMANENT_GAP_RESOLVED'
      WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1 AND chapter_sort_key = 183;
    `, [ttpId]);
    console.log('Top Tier Providence gap 183 marked as resolved.');

    // 7. Boost Solvable Gaps: Contos de Demônios e Deuses (45 staged chapters)
    const contosId = 'ecc5b6d1-1866-487d-91c4-d000905c4962';
    console.log('\n--- 7. Boosting Contos de Demônios e Deuses unblockers (224 to 226.5) ---');
    const boostContos = await client.query(`
      UPDATE importer_queue
      SET priority = 100, attempts = 0, next_run_at = NOW(), updated_at = NOW()
      WHERE task_type = 'IMPORT_CHAPTER'
        AND (payload->>'workId') = $1
        AND chapter_sort_key >= 224 AND chapter_sort_key <= 226.5
        AND status IN ('QUEUED', 'RETRY')
      RETURNING id, source, chapter_sort_key, priority;
    `, [contosId]);
    console.log(`Boosted ${boostContos.rows.length} jobs for Contos de Demônios:`, boostContos.rows.map(r => r.chapter_sort_key));

    // 8. Boost Solvable Gaps: Slime Life (25 staged chapters)
    const slimeId = '10d6e5c7-e0df-4d25-8500-6240577d42c9';
    console.log('\n--- 8. Boosting Slime Life unblockers (26, 27) ---');
    const boostSlime = await client.query(`
      UPDATE importer_queue
      SET priority = 100, attempts = 0, next_run_at = NOW(), updated_at = NOW()
      WHERE task_type = 'IMPORT_CHAPTER'
        AND (payload->>'workId') = $1
        AND chapter_sort_key IN (26, 27)
        AND status IN ('QUEUED', 'RETRY')
      RETURNING id, source, chapter_sort_key, priority;
    `, [slimeId]);
    console.log(`Boosted ${boostSlime.rows.length} jobs for Slime Life:`, boostSlime.rows.map(r => r.chapter_sort_key));

    // 9. Reset WAITING_FOR_GAP status back to STAGED for unblocked works so barrier sweep will pick them up immediately
    const unblockedWorkIds = [berserkId, baskervilleId, vilaoId, dragonId, demonId, ttpId];
    const resetRes = await client.query(`
      UPDATE importer_chapter_mappings
      SET status = 'STAGED', updated_at = NOW()
      WHERE work_id = ANY($1::uuid[])
        AND status = 'WAITING_FOR_GAP';
    `, [unblockedWorkIds]);
    console.log(`\nReset ${resetRes.rowCount} mappings from WAITING_FOR_GAP back to STAGED for immediate cascade.`);

    console.log('\n=== ALL ACTIONS EXECUTED CLEANLY ===');
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
