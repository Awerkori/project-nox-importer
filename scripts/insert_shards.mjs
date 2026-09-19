import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const cp = JSON.parse(fs.readFileSync('/home/awerkori/.config/project-nox/storage_checkpoint.json', 'utf8'));

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

const poolId = '9ad5dac9-c8f7-4774-b488-59837fcef9c3'; // Manga Pages & Chapters Storage

async function main() {
  await client.connect();
  console.log('Connected to YugabyteDB successfully.');

  for (let i = 10; i <= 21; i++) {
    const skey = 'SHARD_' + String(i).padStart(2, '0');
    const sdata = cp.shards[skey];
    const shardId = sdata.shardId;
    const cid = String(sdata.channel_id);
    const dname = 'Nox Manga Storage ' + String(i).padStart(3, '0');

    let botRef = 'MANGA_STORAGE_03';
    if (i >= 13 && i <= 15) botRef = 'MANGA_STORAGE_04';
    if (i >= 16 && i <= 18) botRef = 'MANGA_STORAGE_05';
    if (i >= 19 && i <= 21) botRef = 'MANGA_STORAGE_06';

    const check = await client.query('SELECT id FROM storage_shards WHERE id = $1 OR channel_id = $2', [shardId, cid]);
    if (check.rows.length === 0) {
      await client.query(`
        INSERT INTO storage_shards (
          id, pool_id, backend, bot_reference, channel_id, display_name,
          enabled, reserved, write_status, read_status, weight,
          active_uploads, queue_depth, recent_successes, recent_failures,
          latency_ms, throughput, error_rate, created_at, updated_at,
          assigned_chapters_count, assigned_pages_count
        ) VALUES (
          $1, $2, 'TELEGRAM', $3, $4, $5,
          true, false, 'HEALTHY', 'HEALTHY', 100,
          0, 0, 10, 0,
          0, 0, 0, NOW(), NOW(),
          0, 0
        )
      `, [shardId, poolId, botRef, cid, dname]);
      console.log(`[INSERTED] ${skey}: ${shardId} -> ${dname} (${cid})`);
    } else {
      console.log(`[EXISTS] ${skey}: ${check.rows[0].id}`);
    }
  }

  const res = await client.query('SELECT count(*) FROM storage_shards WHERE pool_id = $1', [poolId]);
  console.log(`\nTotal Manga storage shards in DB: ${res.rows[0].count} / 21`);

  const allManga = await client.query(`
    SELECT display_name, bot_reference, channel_id, write_status, enabled
    FROM storage_shards
    WHERE pool_id = $1
    ORDER BY display_name
  `, [poolId]);
  console.table(allManga.rows);

  await client.end();
}

main().catch(err => {
  console.error('Error inserting shards:', err);
  process.exit(1);
});
