import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'node:fs';
import crypto from 'node:crypto';
import { DirectTelegramStorageProvider } from '../src/storage/direct-telegram.ts';
import { inspectImage, calculateSha256 } from '../src/storage/media.ts';

dotenv.config();

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  database: process.env.YUGABYTE_DATABASE,
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  ssl: { rejectUnauthorized: false }
});

const BOT_USER_ID = process.env.IMPORTER_USER_ID || '732fbe87-5040-41fb-9983-0aedb2af44c8';
const NOX_MANGA_URL = process.env.NOX_MANGA_URL || 'https://manga.project-nox-awerkori.workers.dev';
const BRIDGE_TOKEN = process.env.NOX_STORAGE_BRIDGE_TOKEN || '';

const TARGETS = [
  {
    slug: 'amigo-de-infancia-do-zenite',
    title: 'Amigo de Infância do Zênite',
    filePath: '/home/awerkori/.gemini/antigravity-cli/brain/24c478a8-bdc4-481b-bbd1-28a99f5f3a37/scratch/amigo.webp'
  },
  {
    slug: 'como-sobreviver-sendo-um-cavaleiro-renegado',
    title: 'Como Sobreviver Sendo Um Cavaleiro Renegado',
    filePath: '/home/awerkori/.gemini/antigravity-cli/brain/24c478a8-bdc4-481b-bbd1-28a99f5f3a37/scratch/como.webp'
  },
  {
    slug: 'o-glutao',
    title: 'O Retorno do Devorador – O Glutão',
    filePath: '/home/awerkori/.gemini/antigravity-cli/brain/24c478a8-bdc4-481b-bbd1-28a99f5f3a37/scratch/glutao.webp'
  }
];

async function invalidateSiteCache(slug: string) {
  try {
    const res = await fetch(`${NOX_MANGA_URL}/api/internal/cache/invalidate`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${BRIDGE_TOKEN}`
      },
      body: JSON.stringify({
        tags: [`obra:${slug}`, 'home', 'releases']
      }),
      signal: AbortSignal.timeout(5000)
    });
    return res.status;
  } catch (e: any) {
    return `error: ${e.message}`;
  }
}

async function main() {
  await client.connect();
  const storage = new DirectTelegramStorageProvider();

  console.log('=== UPLOADING RECONSTRUCTED OFFICIAL WEBP COVERS ===');

  for (const t of TARGETS) {
    console.log(`\nProcessing work: "${t.title}" (${t.slug})`);
    const fileBytes = fs.readFileSync(t.filePath);
    const uint8 = new Uint8Array(fileBytes);
    const info = inspectImage(uint8);
    const sha256 = calculateSha256(uint8);

    console.log(`  Inspected image: ${info.width}x${info.height} (${info.mime}, ${uint8.length} bytes)`);

    // Fetch work id
    const workRes = await client.query('SELECT id, cover_id FROM works WHERE slug = $1', [t.slug]);
    if (workRes.rows.length === 0) {
      console.error(`  ERROR: Work with slug "${t.slug}" not found in DB!`);
      continue;
    }
    const workId = workRes.rows[0].id;
    console.log(`  Found workId: ${workId}`);

    const newMediaId = crypto.randomUUID();
    console.log(`  Uploading to Telegram Storage (media_id: ${newMediaId})...`);

    const providerKey = await storage.upload(uint8, info.mime, newMediaId);
    const botRef = storage.getLastBotReference(newMediaId);
    const shardId = storage.getLastShardId(newMediaId);

    console.log(`  Telegram upload OK: providerKey=${providerKey}, botRef=${botRef}, shardId=${shardId}`);

    // Insert into media
    await client.query(`
      INSERT INTO media (
        id, provider, provider_key, mime, width, height, bytes, sha256,
        created_by, created_at, storage_ready, purpose, storage_shard_id,
        bot_reference, access_class, status, chapter_id
      ) VALUES (
        $1, 'telegram', $2, $3, $4, $5, $6, $7,
        $8, NOW(), true, 'editorial', $9,
        $10, 'PUBLIC', 'READY', NULL
      );
    `, [
      newMediaId,
      providerKey,
      info.mime,
      info.width,
      info.height,
      uint8.length,
      sha256,
      BOT_USER_ID,
      shardId,
      botRef
    ]);

    // Update works table
    await client.query(`UPDATE works SET cover_id = $1, updated_at = NOW() WHERE id = $2`, [newMediaId, workId]);
    console.log(`  Updated works.cover_id = ${newMediaId}`);

    const status = await invalidateSiteCache(t.slug);
    console.log(`  Invalidated cache: HTTP ${status}`);
  }

  // Verification
  console.log('\n=== VERIFICATION ===');
  const verifyRes = await client.query(`
    SELECT w.slug, w.title, w.cover_id, m.mime, m.width, m.height, m.bytes, m.purpose, m.chapter_id
    FROM works w
    LEFT JOIN media m ON w.cover_id = m.id
    WHERE w.slug IN ('amigo-de-infancia-do-zenite', 'como-sobreviver-sendo-um-cavaleiro-renegado', 'o-glutao');
  `);
  console.log(JSON.stringify(verifyRes.rows, null, 2));

  const countBad = await client.query(`
    SELECT count(*) as bad_count
    FROM works w
    JOIN media m ON w.cover_id = m.id
    WHERE m.chapter_id IS NOT NULL OR m.purpose = 'chapter_page';
  `);
  console.log(`Contaminated covers in entire DB: ${countBad.rows[0].bad_count}`);

  const countNull = await client.query(`
    SELECT count(*) as null_count
    FROM works
    WHERE cover_id IS NULL;
  `);
  console.log(`Total works with cover_id = NULL in entire DB: ${countNull.rows[0].null_count}`);

  await client.end();
}

main().catch(console.error);
