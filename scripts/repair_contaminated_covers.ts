import pg from 'pg';
import dotenv from 'dotenv';
import { HostRateLimiter } from '../src/core/rate-limiter.ts';
import { SourceRegistry } from '../src/sources/registry.ts';
import { DirectTelegramStorageProvider } from '../src/storage/direct-telegram.ts';
import { inspectImage, calculateSha256 } from '../src/storage/media.ts';
import crypto from 'node:crypto';

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

interface WorkRepairReport {
  workId: string;
  slug: string;
  title: string;
  source: string;
  before: {
    coverId: string;
    width: number;
    height: number;
    bytes: number;
  };
  after: {
    coverId: string | null;
    width?: number;
    height?: number;
    bytes?: number;
    status: 'RECOVERED' | 'FALLBACK_PLACEHOLDER';
    reason?: string;
  };
}

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

async function run() {
  console.log('=== STARTING REPAIR OF CONTAMINATED COVERS ===');
  await client.connect();

  const res = await client.query(`
    SELECT DISTINCT ON (w.id)
      w.id as work_id,
      w.slug,
      w.title,
      w.cover_id,
      m.width as bad_w,
      m.height as bad_h,
      m.bytes as bad_bytes,
      wm.source,
      wm.source_work_id
    FROM works w
    JOIN media m ON w.cover_id = m.id
    LEFT JOIN importer_work_mappings wm ON w.id = wm.work_id
    WHERE m.chapter_id IS NOT NULL OR m.purpose = 'chapter_page'
    ORDER BY w.id, wm.created_at ASC NULLS LAST;
  `);

  const works = res.rows;
  console.log(`Found ${works.length} contaminated works to repair.\n`);

  const rateLimiter = new HostRateLimiter(5.0);
  const registry = new SourceRegistry(rateLimiter, BRIDGE_TOKEN, NOX_MANGA_URL);
  const storage = new DirectTelegramStorageProvider();

  const reports: WorkRepairReport[] = [];

  for (let i = 0; i < works.length; i++) {
    const row = works[i];
    console.log(`--------------------------------------------------------------------------------`);
    console.log(`[${i + 1}/${works.length}] Processing: "${row.title}" (slug: ${row.slug})`);
    console.log(`  Source: ${row.source} | SourceWorkId: ${row.source_work_id}`);
    console.log(`  Contaminated Cover: ${row.bad_w}x${row.bad_h} (${row.bad_bytes} bytes)`);

    const report: WorkRepairReport = {
      workId: row.work_id,
      slug: row.slug,
      title: row.title,
      source: row.source || 'NONE',
      before: {
        coverId: row.cover_id,
        width: row.bad_w,
        height: row.bad_h,
        bytes: row.bad_bytes,
      },
      after: {
        coverId: null,
        status: 'FALLBACK_PLACEHOLDER',
      }
    };

    let recoveredCover: {
      bytes: Uint8Array;
      info: { mime: string; width: number; height: number };
    } | null = null;

    if (row.source && row.source_work_id) {
      const adapter = registry.get(row.source);
      if (adapter) {
        try {
          console.log(`  Fetching work details from ${row.source}...`);
          const details = await Promise.race([
            adapter.fetchWorkDetails(row.source_work_id),
            new Promise<any>((_, reject) => setTimeout(() => reject(new Error('Timeout fetching details')), 10000))
          ]);

          if (details?.coverUrl) {
            const isChapterPattern = /\/(chapter|capitulo|reader|leitor|page|pagina|paginas)\b|_page_\d+/i.test(details.coverUrl);
            if (isChapterPattern) {
              console.log(`  Rejected coverUrl matching chapter pattern: ${details.coverUrl}`);
            } else {
              console.log(`  Candidate cover URL: ${details.coverUrl}`);
              const imgRes = await fetch(details.coverUrl, {
                headers: {
                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                  'Referer': new URL(details.coverUrl).origin + '/'
                },
                signal: AbortSignal.timeout(8000)
              });

              if (imgRes.ok) {
                const cLen = parseInt(imgRes.headers.get('content-length') || '0', 10);
                if (cLen > 4_000_000) {
                  console.log(`  Rejected cover due to excessive size: ${cLen} bytes`);
                } else {
                  const buf = await imgRes.arrayBuffer();
                  const bytes = new Uint8Array(buf);
                  if (bytes.length >= 1500 && bytes.length <= 4_000_000) {
                    try {
                      const info = inspectImage(bytes);
                      const ratio = info.height / info.width;
                      const invRatio = info.width / info.height;
                      if (info.mime === 'image/gif') {
                        console.log(`  Rejected animated gif as cover.`);
                      } else if (info.width < 100 || info.height < 140) {
                        console.log(`  Rejected small cover: ${info.width}x${info.height}`);
                      } else if (ratio > 2.5 || invRatio > 2.5) {
                        console.log(`  Rejected extreme aspect ratio: ${info.width}x${info.height} (ratio=${ratio.toFixed(2)})`);
                      } else {
                        recoveredCover = { bytes, info };
                        console.log(`  VALID COVER CONFIRMED: ${info.width}x${info.height} (${bytes.length} bytes, ratio=${ratio.toFixed(2)})`);
                      }
                    } catch (inspectErr: any) {
                      console.log(`  inspectImage error: ${inspectErr.message}`);
                    }
                  } else {
                    console.log(`  Rejected byte length: ${bytes.length}`);
                  }
                }
              } else {
                console.log(`  HTTP error fetching cover: ${imgRes.status}`);
              }
            }
          } else {
            console.log(`  No coverUrl in work details.`);
          }
        } catch (fetchErr: any) {
          console.log(`  Failed to fetch work details: ${fetchErr.message}`);
        }
      }
    }

    if (recoveredCover) {
      try {
        const newMediaId = crypto.randomUUID();
        console.log(`  Uploading new cover to Telegram Storage (ID: ${newMediaId})...`);
        const providerKey = await storage.upload(recoveredCover.bytes, recoveredCover.info.mime, newMediaId);
        const botRef = storage.getLastBotReference(newMediaId);
        const shardId = storage.getLastShardId(newMediaId);
        const sha256 = calculateSha256(recoveredCover.bytes);

        // Insert new media record into public.media
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
          recoveredCover.info.mime,
          recoveredCover.info.width,
          recoveredCover.info.height,
          recoveredCover.bytes.length,
          sha256,
          BOT_USER_ID,
          shardId,
          botRef
        ]);

        // Update works table with new cover_id
        await client.query(`
          UPDATE works 
          SET cover_id = $1, updated_at = NOW() 
          WHERE id = $2;
        `, [newMediaId, row.work_id]);

        report.after = {
          coverId: newMediaId,
          width: recoveredCover.info.width,
          height: recoveredCover.info.height,
          bytes: recoveredCover.bytes.length,
          status: 'RECOVERED',
        };
        console.log(`  SUCCESS: Updated work with clean official cover!`);
      } catch (uploadErr: any) {
        console.error(`  Upload/DB error: ${uploadErr.message}. Falling back to null.`);
        await client.query(`UPDATE works SET cover_id = NULL, updated_at = NOW() WHERE id = $1;`, [row.work_id]);
        report.after = {
          coverId: null,
          status: 'FALLBACK_PLACEHOLDER',
          reason: uploadErr.message,
        };
      }
    } else {
      console.log(`  No valid official cover recoverable. Resetting cover_id to NULL (clean brand placeholder fallback).`);
      await client.query(`UPDATE works SET cover_id = NULL, updated_at = NOW() WHERE id = $1;`, [row.work_id]);
      report.after = {
        coverId: null,
        status: 'FALLBACK_PLACEHOLDER',
        reason: 'No valid official cover available from upstream source',
      };
    }

    // Invalidate site cache for this work
    const cacheStatus = await invalidateSiteCache(row.slug);
    console.log(`  Cache invalidated: ${cacheStatus}`);

    reports.push(report);
  }

  await client.end();

  console.log('\n================================================================================');
  console.log('REPAIR COMPLETED. SUMMARY:');
  console.log('================================================================================');
  const recoveredCount = reports.filter(r => r.after.status === 'RECOVERED').length;
  const fallbackCount = reports.filter(r => r.after.status === 'FALLBACK_PLACEHOLDER').length;
  console.log(`Total Processed: ${reports.length}`);
  console.log(`Successfully Recovered: ${recoveredCount}`);
  console.log(`Fallback to Brand Placeholder: ${fallbackCount}`);
  console.log('================================================================================');

  import('node:fs').then(fs => {
    fs.writeFileSync('cover_repair_report.json', JSON.stringify(reports, null, 2));
    console.log('Saved cover_repair_report.json');
  });
}

run().catch(err => {
  console.error('Fatal error running repair:', err);
  process.exit(1);
});
