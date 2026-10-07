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
    console.log('=== REVERTING INCORRECT GAPS AND RESTORING NORMAL ATTEMPTS ===\n');

    // 1. Berserk #126-128
    const berserkId = 'd8b402cd-b42a-4e08-98d4-1eace5976642';
    console.log('1. Reverting Berserk #126-128...');
    const bMap = await client.query(`
      UPDATE importer_chapter_mappings
      SET is_gap = false, status = 'PENDING', last_error = 'REVERTED_TRANSIENT_WAF_GAP', updated_at = NOW()
      WHERE work_id = $1 AND chapter_sort_key IN (126, 127, 128)
      RETURNING id, chapter_sort_key, source;
    `, [berserkId]);
    const bQ = await client.query(`
      UPDATE importer_queue
      SET status = 'QUEUED', attempts = 0, next_run_at = NOW(), priority = 50, last_error = null, updated_at = NOW()
      WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1 AND chapter_sort_key IN (126, 127, 128)
      RETURNING id, chapter_sort_key, source;
    `, [berserkId]);
    console.log(`Berserk: reverted ${bMap.rows.length} mappings and queued ${bQ.rows.length} jobs.`);

    // 2. Baskerville #1-86: Delete synthetic gap rows
    const baskervilleId = 'bd883348-98b9-48da-a2e8-c3a277da5d5d';
    console.log('\n2. Reverting Baskerville #1-86 (removing synthetic gap entries)...');
    const delBaskerville = await client.query(`
      DELETE FROM importer_chapter_mappings
      WHERE work_id = $1 AND source_chapter_id LIKE 'gap_' || $1 || '_%'
      RETURNING id, chapter_sort_key;
    `, [baskervilleId]);
    console.log(`Baskerville: deleted ${delBaskerville.rows.length} synthetic gap mappings.`);

    // 3. Eu sou o Vilão Predestinado #75-76
    const vilaoId = '8c870b74-8bd3-448e-96b4-5e28e7626e2e';
    console.log('\n3. Reverting Eu sou o Vilão Predestinado #75-76...');
    const vMap = await client.query(`
      UPDATE importer_chapter_mappings
      SET is_gap = false, status = 'PENDING', last_error = 'REVERTED_TRANSIENT_CDN_403_GAP', updated_at = NOW()
      WHERE work_id = $1 AND chapter_sort_key IN (75, 76)
      RETURNING id, chapter_sort_key, source;
    `, [vilaoId]);
    const vQ = await client.query(`
      UPDATE importer_queue
      SET status = 'QUEUED', attempts = 0, next_run_at = NOW(), priority = 50, last_error = null, updated_at = NOW()
      WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1 AND chapter_sort_key IN (75, 76)
      RETURNING id, chapter_sort_key, source;
    `, [vilaoId]);
    console.log(`Vilão Predestinado: reverted ${vMap.rows.length} mappings and queued ${vQ.rows.length} jobs.`);

    // 4. My Dragon System #90
    const dragonId = 'a66d42a1-cf7a-4481-9c8d-a88f2072704f';
    console.log('\n4. Reverting My Dragon System #90...');
    const dMap = await client.query(`
      UPDATE importer_chapter_mappings
      SET is_gap = false, status = 'PENDING', last_error = 'REVERTED_TRANSIENT_TIMEOUT_GAP', updated_at = NOW()
      WHERE work_id = $1 AND chapter_sort_key = 90
      RETURNING id, chapter_sort_key, source;
    `, [dragonId]);
    const dQ = await client.query(`
      UPDATE importer_queue
      SET status = 'QUEUED', attempts = 0, next_run_at = NOW(), priority = 50, last_error = null, updated_at = NOW()
      WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1 AND chapter_sort_key = 90
      RETURNING id, chapter_sort_key, source;
    `, [dragonId]);
    console.log(`My Dragon System: reverted ${dMap.rows.length} mappings and queued ${dQ.rows.length} jobs.`);

    // 5. The Demon of Vengeance #3
    const demonId = '29ab3c9b-9716-48a5-b08c-bcb333b4c210';
    console.log('\n5. Reverting The Demon of Vengeance #3...');
    const dmMap = await client.query(`
      UPDATE importer_chapter_mappings
      SET is_gap = false, status = 'PENDING', last_error = 'REVERTED_TRANSIENT_TIMEOUT_GAP', updated_at = NOW()
      WHERE work_id = $1 AND chapter_sort_key = 3
      RETURNING id, chapter_sort_key, source;
    `, [demonId]);
    const dmQ = await client.query(`
      UPDATE importer_queue
      SET status = 'QUEUED', attempts = 0, next_run_at = NOW(), priority = 50, last_error = null, updated_at = NOW()
      WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1 AND chapter_sort_key = 3
      RETURNING id, chapter_sort_key, source;
    `, [demonId]);
    console.log(`The Demon of Vengeance: reverted ${dmMap.rows.length} mappings and queued ${dmQ.rows.length} jobs.`);

    // 6. Top Tier Providence #183
    const ttpId = 'd8f8e05d-a103-400f-b963-7c8c7453aa67';
    console.log('\n6. Reverting Top Tier Providence #183...');
    const tMap = await client.query(`
      UPDATE importer_chapter_mappings
      SET is_gap = false, status = 'PENDING', last_error = 'REVERTED_TRANSIENT_TIMEOUT_GAP', updated_at = NOW()
      WHERE work_id = $1 AND chapter_sort_key = 183
      RETURNING id, chapter_sort_key, source;
    `, [ttpId]);
    const tQ = await client.query(`
      UPDATE importer_queue
      SET status = 'QUEUED', attempts = 0, next_run_at = NOW(), priority = 50, last_error = null, updated_at = NOW()
      WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1 AND chapter_sort_key = 183
      RETURNING id, chapter_sort_key, source;
    `, [ttpId]);
    console.log(`Top Tier Providence: reverted ${tMap.rows.length} mappings and queued ${tQ.rows.length} jobs.`);

    console.log('\n✅ All incorrect gaps successfully reverted and queued for normal retry!');
  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
