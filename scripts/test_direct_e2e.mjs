import crypto from 'crypto';
import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { DirectTelegramStorageProvider } = await import('../build/storage/direct-telegram.js');

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  await client.connect();
  const provider = new DirectTelegramStorageProvider();

  const testId = crypto.randomUUID();
  const dummyWebp = Buffer.concat([
    Buffer.from('RIFF', 'ascii'),
    Buffer.from('20000000', 'hex'),
    Buffer.from('WEBPVP8 ', 'ascii'),
    Buffer.from('140000003001009d012a010001000002003425a400037000fefbfd5000', 'hex')
  ]);
  const sha256 = crypto.createHash('sha256').update(dummyWebp).digest('hex');

  console.log('Uploading test image directly to Telegram...');
  const fileId = await provider.upload(dummyWebp, 'image/webp', testId);
  const botRef = provider.getLastBotReference(testId);
  const shardId = provider.getLastShardId(testId);
  console.log('Upload successful! fileId:', fileId, 'botRef:', botRef, 'shardId:', shardId);

  // Register in DB
  await client.query(`
    INSERT INTO media (
      id, provider, provider_key, bot_reference, storage_shard_id,
      mime, width, height, bytes, sha256, purpose, access_class,
      created_by, storage_ready, created_at
    ) VALUES (
      $1, 'telegram', $2, $3, $4,
      'image/webp', 1, 1, $5, $6, 'editorial', 'PUBLIC',
      '00000000-0000-0000-0000-000000000001', true, NOW()
    );
  `, [testId, fileId, botRef, shardId, dummyWebp.length, sha256]);
  console.log('Media registered in Yugabyte DB!');

  // Test reading via Reader endpoint
  const url = 'https://manga.project-nox-awerkori.workers.dev/media/' + testId;
  console.log('Testing Reader GET:', url);
  const res = await fetch(url, { headers: { 'User-Agent': 'VerificationTest/1.0' } });
  console.log('Reader status:', res.status, 'Content-Type:', res.headers.get('content-type'));

  const fetchedBytes = Buffer.from(await res.arrayBuffer());
  const match = fetchedBytes.equals(dummyWebp);
  console.log('Bytes match perfectly:', match, fetchedBytes.length, 'vs', dummyWebp.length);

  // Clean up
  await client.query('DELETE FROM media WHERE id = $1', [testId]);
  console.log('Test record cleaned up from DB.');

  await client.end();
  process.exit(match && res.status === 200 ? 0 : 1);
}

main().catch(err => {
  console.error('Test error:', err);
  process.exit(1);
});
