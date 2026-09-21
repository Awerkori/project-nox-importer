import pg from 'pg';
import dotenv from 'dotenv';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { HostRateLimiter } from '../src/core/rate-limiter.ts';
import { SourceRegistry } from '../src/sources/registry.ts';
import { DirectTelegramStorageProvider } from '../src/storage/direct-telegram.ts';
import { inspectImage, calculateSha256 } from '../src/storage/media.ts';
import { CloudflareClassifier } from '../src/core/cloudflare-classifier.ts';

dotenv.config();

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  database: process.env.YUGABYTE_DATABASE,
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  ssl: { rejectUnauthorized: false },
});

const BOT_USER_ID = process.env.IMPORTER_USER_ID || '732fbe87-5040-41fb-9983-0aedb2af44c8';
const NOX_MANGA_URL = (process.env.NOX_MANGA_URL || 'https://manga.project-nox-awerkori.workers.dev').replace(/\/$/, '');
const BRIDGE_TOKEN = process.env.NOX_STORAGE_BRIDGE_TOKEN || '';

interface RepairReportItem {
  workId: string;
  slug: string;
  title: string;
  published: boolean;
  previousCoverId: string | null;
  issue: string;
  status: 'REPAIRED' | 'NO_VALID_SOURCE' | 'FAILED';
  newCoverId?: string;
  newMime?: string;
  newWidth?: number;
  newHeight?: number;
  newBytes?: number;
  resolvedSource?: string;
  resolvedUrl?: string;
  error?: string;
}

async function invalidateSiteCache(slug: string) {
  if (!BRIDGE_TOKEN) return 'skipped (no token)';
  try {
    const res = await fetch(`${NOX_MANGA_URL}/api/internal/cache/invalidate`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${BRIDGE_TOKEN}`,
      },
      body: JSON.stringify({
        tags: [`obra:${slug}`, 'home', 'releases'],
      }),
      signal: AbortSignal.timeout(5000),
    });
    return `HTTP ${res.status}`;
  } catch (e: any) {
    return `error: ${e.message}`;
  }
}

async function fetchCandidateBytes(
  url: string,
  source: string,
  registry: SourceRegistry,
  rateLimiter: HostRateLimiter
): Promise<Uint8Array> {
  const parsed = new URL(url);
  await rateLimiter.acquire(parsed.host);

  let customHeaders: Record<string, string> = {};
  if (source && source !== 'unknown') {
    try {
      const adapter = registry.get(source);
      if (adapter && typeof adapter.getImageHeaders === 'function') {
        const h = await adapter.getImageHeaders(url);
        if (h) customHeaders = h;
      }
    } catch {
      // Non-blocking
    }
  }

  const isKuro = parsed.host.includes('kuromangas.com');
  const referer = isKuro ? 'https://kuromangas.com/' : `${parsed.origin}/`;

  const requestHeaders: Record<string, string> = {
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    Referer: referer,
    ...customHeaders,
  };

  let res: Response | null = null;
  let fetchError: any = null;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      res = await fetch(url, {
        headers: requestHeaders,
        signal: AbortSignal.timeout(15000),
      });
      if (res.ok) break;
      if (res.status >= 500 && attempt < 3) {
        await new Promise((r) => setTimeout(r, 500 * attempt));
        continue;
      }
      break;
    } catch (err: any) {
      fetchError = err;
      if (attempt < 3) {
        await new Promise((r) => setTimeout(r, 500 * attempt));
        continue;
      }
    }
  }

  if (fetchError) throw fetchError;
  if (!res || !res.ok) {
    throw new Error(`HTTP ${res?.status || 500}`);
  }

  const buf = await res.arrayBuffer();
  const bytes = new Uint8Array(buf);

  // Magic bytes / challenge check
  const snippet = new TextDecoder('utf-8', { fatal: false }).decode(bytes.slice(0, 4096));
  const insp = CloudflareClassifier.inspect(res.status, res.headers, snippet, {
    url,
    expectedType: 'image',
    buffer: bytes,
  });

  if (!insp.isValidImage || insp.isBlocked || insp.isChallenge) {
    throw new Error(`Invalid image response (${insp.classification || 'BLOCKED'})`);
  }

  return bytes;
}

async function run() {
  console.log('============================================================');
  console.log('PROJECT NOX — PRODUCTION COVER AUDIT & REPAIR PIPELINE');
  console.log('============================================================\n');

  await client.connect();

  const rateLimiter = new HostRateLimiter(5.0);
  const registry = new SourceRegistry(rateLimiter, BRIDGE_TOKEN, NOX_MANGA_URL);
  const storage = new DirectTelegramStorageProvider();

  // Find all works with broken or missing covers
  const auditRes = await client.query(`
    SELECT 
      w.id as work_id,
      w.slug,
      w.title,
      w.published,
      w.cover_id,
      w.metadata_provenance,
      m.id as media_id,
      m.storage_ready,
      m.bytes,
      m.width,
      m.height,
      m.purpose,
      m.chapter_id,
      CASE
        WHEN w.cover_id IS NULL THEN 'COVER_NULL'
        WHEN m.id IS NULL THEN 'DANGLING_MEDIA'
        WHEN m.storage_ready IS NOT TRUE THEN 'STORAGE_NOT_READY'
        WHEN m.bytes < 1500 THEN 'TINY_BYTES'
        WHEN m.purpose = 'chapter_page' OR m.chapter_id IS NOT NULL THEN 'CONTAMINATED_PAGE'
        WHEN m.width < 100 OR m.height < 140 THEN 'SMALL_DIMENSIONS'
        ELSE 'UNKNOWN_DEFECT'
      END as issue
    FROM works w
    LEFT JOIN media m ON w.cover_id = m.id
    WHERE w.cover_id IS NULL
       OR m.id IS NULL
       OR m.storage_ready IS NOT TRUE
       OR m.bytes < 1500
       OR m.purpose = 'chapter_page'
       OR m.chapter_id IS NOT NULL
       OR m.width < 100
       OR m.height < 140
    ORDER BY w.published DESC, w.created_at DESC;
  `);

  const brokenWorks = auditRes.rows;
  const publishedCount = brokenWorks.filter((w) => w.published).length;
  const unpublishedCount = brokenWorks.filter((w) => !w.published).length;

  console.log(`Total works requiring cover repair: ${brokenWorks.length}`);
  console.log(`  - Published works missing/broken covers: ${publishedCount}`);
  console.log(`  - Unpublished works missing/broken covers: ${unpublishedCount}\n`);

  const reports: RepairReportItem[] = [];

  // Parse limit from command line args (default: repair all published, plus up to N unpublished)
  const argLimit = process.argv[2] ? parseInt(process.argv[2], 10) : brokenWorks.length;
  const worksToProcess = brokenWorks.slice(0, argLimit);

  console.log(`Processing up to ${worksToProcess.length} works in this run...\n`);

  for (let i = 0; i < worksToProcess.length; i++) {
    const row = worksToProcess[i];
    const reportItem: RepairReportItem = {
      workId: row.work_id,
      slug: row.slug,
      title: row.title,
      published: row.published,
      previousCoverId: row.cover_id,
      issue: row.issue,
      status: 'FAILED',
    };

    console.log(`[${i + 1}/${worksToProcess.length}] ${row.published ? '★ PUBLISHED' : '○'} "${row.title}" (${row.slug})`);
    console.log(`  Issue: ${row.issue} | Previous cover_id: ${row.cover_id || 'NULL'}`);

    // Fetch all mappings for this work
    const mapRes = await client.query(
      `SELECT source, source_work_id, source_slug, metadata FROM importer_work_mappings WHERE work_id = $1 ORDER BY created_at ASC`,
      [row.work_id]
    );
    const mappings = mapRes.rows;

    let candidateUrls: Array<{ url: string; source: string }> = [];

    // Collect candidate URLs from mappings metadata
    for (const m of mappings) {
      const meta = m.metadata || {};
      const urls = [
        meta?.poster?.default_url,
        meta?.poster?.large_url,
        meta?.poster?.url,
        meta?.coverUrl,
        meta?.cover_url,
        meta?.imagem ? (meta.imagem.startsWith('http') ? meta.imagem : `https://cdn.mangotoons.com/${meta.imagem.replace(/^\//, '')}`) : null,
        meta?.image ? (meta.image.startsWith('http') ? meta.image : `https://cdn.mangotoons.com/${meta.image.replace(/^\//, '')}`) : null,
        meta?.thumbnail,
        meta?.thumbnail_url,
        meta?.banner_imagem,
      ].filter((u): u is string => typeof u === 'string' && u.length > 5 && u.startsWith('http'));

      for (const u of urls) {
        if (!candidateUrls.some((c) => c.url === u)) {
          candidateUrls.push({ url: u, source: m.source });
        }
      }
    }

    // If no candidate URLs found in metadata, try calling adapter.fetchWorkDetails
    if (candidateUrls.length === 0) {
      for (const m of mappings) {
        try {
          const adapter = registry.get(m.source);
          if (adapter && typeof adapter.fetchWorkDetails === 'function') {
            const details = await Promise.race([
              adapter.fetchWorkDetails(m.source_work_id),
              new Promise<any>((_, reject) => setTimeout(() => reject(new Error('Timeout')), 8000)),
            ]);
            if (details?.coverUrl) {
              candidateUrls.push({ url: details.coverUrl, source: m.source });
            }
          }
        } catch {
          // Non-blocking
        }
      }
    }

    console.log(`  Found ${candidateUrls.length} candidate cover URLs across ${mappings.length} mapping(s).`);

    let recovered: {
      bytes: Uint8Array;
      info: { mime: string; width: number; height: number };
      source: string;
      url: string;
    } | null = null;

    for (const cand of candidateUrls) {
      // Chapter pattern rejection
      const isChapterPattern = /\/(chapter|capitulo|reader|leitor|page|pagina|paginas)\b|_page_\d+/i.test(cand.url);
      if (isChapterPattern) {
        console.log(`    ↳ Skipped: matches chapter page pattern (${cand.url})`);
        continue;
      }

      try {
        const bytes = await fetchCandidateBytes(cand.url, cand.source, registry, rateLimiter);
        if (bytes.length < 1500) {
          console.log(`    ↳ Rejected: bytes too small (${bytes.length}) from ${cand.url}`);
          continue;
        }
        if (bytes.length > 4_000_000) {
          console.log(`    ↳ Rejected: image exceeds 4MB (${bytes.length}) from ${cand.url}`);
          continue;
        }

        const info = inspectImage(bytes);
        if (info.mime === 'image/gif') {
          console.log(`    ↳ Rejected: animated GIF from ${cand.url}`);
          continue;
        }
        if (info.width < 100 || info.height < 140) {
          console.log(`    ↳ Rejected: dimensions too small (${info.width}x${info.height}) from ${cand.url}`);
          continue;
        }

        const ratio = info.height / info.width;
        const invRatio = info.width / info.height;
        if (ratio > 2.5 || invRatio > 2.5) {
          console.log(`    ↳ Rejected: aspect ratio too extreme (${info.width}x${info.height}, ratio=${ratio.toFixed(2)})`);
          continue;
        }

        // Passed all checks!
        recovered = {
          bytes,
          info,
          source: cand.source,
          url: cand.url,
        };
        console.log(`    ✓ Valid cover verified: ${info.width}x${info.height} (${bytes.length} bytes, ratio=${ratio.toFixed(2)}) from [${cand.source}]`);
        break;
      } catch (err: any) {
        console.log(`    ↳ Download failed (${err.message}) for ${cand.url}`);
      }
    }

    if (recovered) {
      try {
        const newMediaId = crypto.randomUUID();
        const providerKey = await storage.upload(recovered.bytes, recovered.info.mime, newMediaId);
        const botRef = storage.getLastBotReference?.(newMediaId) || 'MANGA_STORAGE_01';
        const shardId = storage.getLastShardId?.(newMediaId) || null;
        const sha256 = calculateSha256(recovered.bytes);

        // Insert new media row
        await client.query(`
          INSERT INTO media (
            id, created_by, provider, provider_key, mime, width, height, bytes, sha256,
            created_at, storage_ready, purpose, storage_shard_id, bot_reference,
            access_class, status, chapter_id
          ) VALUES (
            $1, $2, 'telegram', $3, $4, $5, $6, $7, $8,
            NOW(), true, 'editorial', $9, $10,
            'PUBLIC', 'READY', NULL
          );
        `, [
          newMediaId,
          BOT_USER_ID,
          providerKey,
          recovered.info.mime,
          recovered.info.width,
          recovered.info.height,
          recovered.bytes.length,
          sha256,
          shardId,
          botRef,
        ]);

        // Update works table
        const prov = {
          ...(row.metadata_provenance || {}),
          cover: { source: recovered.source, updated_at: new Date().toISOString() },
        };

        await client.query(`
          UPDATE works
          SET cover_id = $1, metadata_provenance = $2, updated_at = NOW()
          WHERE id = $3;
        `, [newMediaId, JSON.stringify(prov), row.work_id]);

        // Invalidate site cache
        const cacheResult = await invalidateSiteCache(row.slug);

        reportItem.status = 'REPAIRED';
        reportItem.newCoverId = newMediaId;
        reportItem.newMime = recovered.info.mime;
        reportItem.newWidth = recovered.info.width;
        reportItem.newHeight = recovered.info.height;
        reportItem.newBytes = recovered.bytes.length;
        reportItem.resolvedSource = recovered.source;
        reportItem.resolvedUrl = recovered.url;

        console.log(`  ★ SUCCESS: Repaired cover with ID ${newMediaId}. Cache invalidate: ${cacheResult}\n`);
      } catch (uploadErr: any) {
        reportItem.status = 'FAILED';
        reportItem.error = uploadErr.message;
        console.error(`  ✗ Upload failed: ${uploadErr.message}\n`);
      }
    } else {
      reportItem.status = 'NO_VALID_SOURCE';
      console.log(`  ○ No valid cover recoverable among candidate sources.\n`);
    }

    reports.push(reportItem);

    // Controlled rate limit between uploads (250ms)
    await new Promise((r) => setTimeout(r, 250));
  }

  await client.end();

  console.log('============================================================');
  console.log('COVER REPAIR RUN COMPLETED');
  console.log('============================================================');
  const repairedCount = reports.filter((r) => r.status === 'REPAIRED').length;
  const noSourceCount = reports.filter((r) => r.status === 'NO_VALID_SOURCE').length;
  const failedCount = reports.filter((r) => r.status === 'FAILED').length;
  const publishedRepaired = reports.filter((r) => r.published && r.status === 'REPAIRED').length;

  console.log(`Total Audited & Processed: ${reports.length}`);
  console.log(`Successfully Repaired:     ${repairedCount}`);
  console.log(`  - Published Repaired:     ${publishedRepaired}`);
  console.log(`No Viable Source:          ${noSourceCount}`);
  console.log(`Failed:                    ${failedCount}`);
  console.log('============================================================\n');

  fs.writeFileSync('cover_repair_production_report.json', JSON.stringify(reports, null, 2));
  console.log('Detailed report written to cover_repair_production_report.json');
}

run().catch((err) => {
  console.error('Fatal error in cover repair script:', err);
  process.exit(1);
});
