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

const MANGA_URL = process.env.NOX_MANGA_URL || 'https://manga.project-nox-awerkori.workers.dev';

async function main() {
  await client.connect();
  console.log('Connected to DB. Selecting sample media from existing shards 1-9...');

  const { rows } = await client.query(`
    SELECT m.id, m.provider_key, m.bot_reference, m.storage_shard_id, m.mime, m.bytes, s.display_name
    FROM media m
    LEFT JOIN storage_shards s ON m.storage_shard_id = s.id
    WHERE m.storage_ready = true
      AND m.status IS DISTINCT FROM 'DELETED'
      AND m.provider = 'telegram'
      AND m.bytes > 1000
    ORDER BY m.created_at DESC
    LIMIT 15;
  `);

  console.log(`Found ${rows.length} test candidates.`);
  let passed = 0;
  let failed = 0;

  for (const row of rows) {
    const mediaUrl = `${MANGA_URL}/media/${row.id}`;
    const start = Date.now();
    try {
      const res = await fetch(mediaUrl, {
        headers: {
          'User-Agent': 'ProjectNox-Verification/1.0'
        },
        signal: AbortSignal.timeout(15000)
      });
      const latency = Date.now() - start;
      if (res.status === 200) {
        const buf = await res.arrayBuffer();
        const ct = res.headers.get('content-type');
        console.log(`[PASS] ${row.id} (${row.display_name || row.bot_reference}) -> HTTP 200 | size: ${buf.byteLength} bytes | mime: ${ct} | latency: ${latency}ms`);
        passed++;
      } else {
        console.error(`[FAIL] ${row.id} -> HTTP ${res.status} | latency: ${latency}ms`);
        failed++;
      }
    } catch (err) {
      console.error(`[ERROR] ${row.id} -> ${err.message}`);
      failed++;
    }
  }

  console.log('\n========================================');
  console.log(`OLD MEDIA VALIDATION RESULT: ${failed === 0 && passed > 0 ? 'PASS' : 'FAIL'}`);
  console.log(`Passed: ${passed} | Failed: ${failed}`);
  console.log('========================================');

  await client.end();
  process.exit(failed === 0 && passed > 0 ? 0 : 1);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
