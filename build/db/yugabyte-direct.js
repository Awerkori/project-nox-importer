import pg from 'pg';
import fs from 'fs';
import crypto from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import { getConfig } from '../config.js';
import { Logger } from '../core/logger.js';
import { computeCanonicalChapterKey } from '../core/deduplication.js';
import { telemetryCollector } from '../core/telemetry-collector.js';
import { performance } from 'node:perf_hooks';
const logger = new Logger('YugabyteDirect');
let pool = null;
const EMBEDDED_YUGABYTE_CA = `-----BEGIN CERTIFICATE-----
MIIGxDCCBKygAwIBAgITF8MQH/VpOvqeGxybP3vBoEPj0TANBgkqhkiG9w0BAQsF
ADCBhDELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMRIwEAYDVQQHEwlTdW5ueXZh
bGUxFTATBgNVBAoTDFl1Z2FieXRlIEluYzEXMBUGA1UECxMOWXVnYWJ5dGUgQ2xv
dWQxJDAiBgNVBAMTG1l1Z2FieXRlIENsb3VkIFJvb3QgQ0EgcHJvZDAeFw0yNTA5
MTgxNjIzMjBaFw0zMDA5MTcxNjIzMTlaMIGWMQswCQYDVQQGEwJVUzELMAkGA1UE
CBMCQ0ExEjAQBgNVBAcTCVN1bm55dmFsZTEXMBUGA1UEChMOWXVnYWJ5dGVEQiBJ
bmMxIDAeBgNVBAsTF1l1Z2FieXRlREIgQWVvbiBNYW5hZ2VkMSswKQYDVQQDEyJZ
dWdhYnl0ZSBDbG91ZCBTdWJvcmRpbmF0ZSBDQSBwcm9kMIIBojANBgkqhkiG9w0B
AQEFAAOCAY8AMIIBigKCAYEAl1CO4UpzZYYDqVuzLUyhNEah0a0VMNzgYQkCxXGN
QOqA0sp60bfJiAaUe1fO1A/VYSur3kpuxWk3/CxBrR6RTsZKpFy1XgeK6McecWUV
ACOtINMwrmHYg0hb032bPSvwDweTddkG9wzuD4Md9+5FK6ebjDhq/kPwSQHhG1lS
qygAcwRJgHcb3Ad6ZrtG5hYNG2Ikt86ipIV/6zgwHYoaDjI4u5Nl5Gqblkea2yn6
M+Z0AM0u+k6FsMkqu9IMXYC466ZTNXHB4pMLVgRkiUvLnUnA/nROGTHmi1ivLGDO
TvKPlEkVvbdZYbutG9isGUg61Fhqb1hRISPdC1uxD4RBoooIBHoOP27gWcdVa+qZ
Q/N6oYy6Yw3R58d0JJrjE5BwppAK9q4EMlRIj9JEbQIyBhtbkqPn+PwwvUHRFavY
s3BKmx2BSuYWaDCSmbDSOLxuf2bzaHIwrhlBgDhWwtVl4F5W1tzDV7FPl19vkl77
UPQo87d3C55rofMJ9BRsdqHbAgMBAAGjggGZMIIBlTAOBgNVHQ8BAf8EBAMCAQYw
HQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMA8GA1UdEwEB/wQFMAMBAf8w
HQYDVR0OBBYEFLnilBT2qOKcpt+IED4inSpiiLCnMB8GA1UdIwQYMBaAFGCPfEqV
jRfctFj3jSsv+QkB/hI7MIGNBggrBgEFBQcBAQSBgDB+MHwGCCsGAQUFBzAChnBo
dHRwOi8vcHJpdmF0ZWNhLWNvbnRlbnQtNjExZTIyZjYtMDAwMC0yMGM5LWE2NTAt
ZDRmNTQ3ZjdlZTFjLnN0b3JhZ2UuZ29vZ2xlYXBpcy5jb20vYWExNDA4ODNkNGE3
YTJjMDBhMDkvY2EuY3J0MIGCBgNVHR8EezB5MHegdaBzhnFodHRwOi8vcHJpdmF0
ZWNhLWNvbnRlbnQtNjExZTIyZjYtMDAwMC0yMGM5LWE2NTAtZDRmNTQ3ZjdlZTFj
LnN0b3JhZ2UuZ29vZ2xlYXBpcy5jb20vYWExNDA4ODNkNGE3YTJjMDBhMDkvY3Js
LmNybDANBgkqhkiG9w0BAQsFAAOCAgEAt9HGIX1Btl0Fb70GpySsxv94632Kvs6G
1zTL21lpAnLmngezaGOuOzR/swwCIfHj3rf+wFKY6HQ371pLgEUvfe3Ve5aVKIs/
krvGzUIJybaMIZ2rtO88C+X+a+LInRJLJMhJ366X+jTNURLV2S2twCQYDnodyYWA
6z+Fq5fPs+O5khj6qCork6tyEgV9F1bLJNRatNRIYBOhif6GeR3bFOBoSb++d1KB
OrqB5fMIXnqcwV7Zjz8goaWQgeWf1VTyAeavUfKLmZl/SzmKfOGUeux5ezZk77KA
beXE/wMCCARVeV4LPf4nOIcJIciF94A2kZuFpyJmM0VVKbcGuSQe58HxVS3iwmN6
GHGegOqDy8yWknbrjADDyYzJvhUJciKN/Nu0XarXvCSR84BnNfKLB0zBNIUAlAUq
iT3NnyYZYlhD/lHZOtFRphyOQXU6VVkRrRjXer7LF9b7BWYaly1f3FzQf9gwoPyO
sprdOhIsaGIbAzx/hoOgUg28HNyfwih6pI80vGqRg2Zgri8Qb8cOSXVkb6Y0Voic
Y9KaDmilwQAALxGW/H/ScXNExKvWy5o7+H4v3Id/Dr026n1wPE3qOlERoaFmBj0U
+E5UevW1SOwyBjS3XR29cOz89FDIq418DGVpSex2q26ALmNX9E/A0XCPP2bk5lFX
hDzC5OY3o1A=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----`;
export function getYugabytePool() {
    if (pool)
        return pool;
    const cfg = getConfig();
    if (!cfg.YUGABYTE_HOST || !cfg.YUGABYTE_USER || !cfg.YUGABYTE_PASSWORD) {
        throw new Error('Yugabyte direct connection requires YUGABYTE_HOST, YUGABYTE_USER, and YUGABYTE_PASSWORD');
    }
    let sslConfig = {
        rejectUnauthorized: true,
        ca: EMBEDDED_YUGABYTE_CA,
    };
    const candidateCerts = [
        cfg.YUGABYTE_SSL_CERT,
        path.resolve(process.cwd(), 'certs/yugabyte-root.crt'),
        path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../certs/yugabyte-root.crt'),
        path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../certs/yugabyte-root.crt'),
        path.resolve(process.cwd(), 'build/certs/yugabyte-root.crt'),
        '/home/node/certs/yugabyte-root.crt',
        '/home/node/app/certs/yugabyte-root.crt',
        path.resolve(process.cwd(), 'config/root.crt'),
        path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../config/root.crt'),
        '/home/awerkori/.config/project-nox/root.crt',
    ].filter(Boolean);
    for (const certPath of candidateCerts) {
        if (fs.existsSync(certPath)) {
            sslConfig = {
                rejectUnauthorized: true,
                ca: fs.readFileSync(certPath, 'utf8'),
            };
            break;
        }
    }
    pool = new pg.Pool({
        host: cfg.YUGABYTE_HOST,
        port: cfg.YUGABYTE_PORT,
        user: cfg.YUGABYTE_USER,
        password: cfg.YUGABYTE_PASSWORD,
        database: cfg.YUGABYTE_DATABASE,
        max: 1, // Strict requirement: pool max = 1
        connectionTimeoutMillis: 10000,
        idleTimeoutMillis: 30000,
        ssl: sslConfig,
        application_name: 'project-nox-importer-direct',
        options: '-c max_parallel_workers_per_gather=0',
    });
    pool.on('error', (err) => {
        logger.error('Unexpected error on idle direct Yugabyte client', { error: err.message });
    });
    const origConnect = pool.connect.bind(pool);
    pool.connect = async function (...args) {
        const t0 = performance.now();
        const queued = pool.waitingCount || 0;
        try {
            const client = await origConnect(...args);
            const waitMs = performance.now() - t0;
            telemetryCollector.recordDbPoolWait(waitMs, queued);
            const origClientQuery = client.query.bind(client);
            client.query = async function (...qArgs) {
                telemetryCollector.trackActiveDbQuery(1);
                try {
                    return await origClientQuery(...qArgs);
                }
                finally {
                    telemetryCollector.trackActiveDbQuery(-1);
                }
            };
            return client;
        }
        catch (err) {
            const waitMs = performance.now() - t0;
            telemetryCollector.recordDbPoolWait(waitMs, queued);
            throw err;
        }
    };
    telemetryCollector.setPool(pool);
    return pool;
}
export async function closeYugabytePool() {
    if (pool) {
        await pool.end();
        pool = null;
    }
}
export async function testConnection() {
    const p = getYugabytePool();
    const client = await p.connect();
    try {
        const verRes = await client.query('SELECT 1 as connected, current_database() as db, version();');
        const statRes = await client.query(`
      SELECT count(*)::int as total_connections,
             count(*) FILTER (WHERE state = 'active')::int as active,
             count(*) FILTER (WHERE state = 'idle')::int as idle,
             count(*) FILTER (WHERE state = 'idle in transaction')::int as idle_in_tx
      FROM pg_stat_activity;
    `);
        const stats = statRes.rows[0] || {};
        return {
            connected: true,
            database: verRes.rows[0].db,
            version: verRes.rows[0].version,
            totalConnections: stats.total_connections || 0,
            activeConnections: stats.active || 0,
            idleConnections: stats.idle || 0,
            idleInTransaction: stats.idle_in_tx || 0,
        };
    }
    finally {
        client.release();
    }
}
export async function testRollback() {
    const p = getYugabytePool();
    const client = await p.connect();
    try {
        await client.query('BEGIN');
        // Read operation inside transaction
        await client.query('SELECT count(*) FROM works;');
        await client.query('ROLLBACK');
        const statRes = await client.query(`
      SELECT count(*) FILTER (WHERE state = 'idle in transaction')::int as idle_in_tx
      FROM pg_stat_activity;
    `);
        return {
            rollbackOk: true,
            openTransactions: 0,
            idleInTransaction: statRes.rows[0]?.idle_in_tx || 0,
        };
    }
    catch (err) {
        await client.query('ROLLBACK').catch(() => { });
        throw err;
    }
    finally {
        client.release();
    }
}
/* 1. Direct Job Acquisition with FOR UPDATE SKIP LOCKED */
export async function acquireJobsDirect(options) {
    const p = getYugabytePool();
    const workerId = options.workerId;
    const leaseMin = Math.max(1, Math.min(60, options.leaseDurationMinutes || 5));
    const source = options.source || null;
    const taskType = options.taskType || null;
    const batchSize = Math.max(1, Math.min(50, options.batchSize || 10));
    const query = `
    WITH to_lock AS (
      SELECT id
      FROM importer_queue
      WHERE (
        status = 'QUEUED'
        OR (status = 'RETRY' AND next_run_at <= NOW())
        OR (status = 'IMPORTING' AND lease_expires_at <= NOW())
      )
        AND ($1::text IS NULL OR source = $1::text)
        AND (
          $2::text IS NULL
          OR ($2::text = 'DISCOVERY' AND task_type IN ('DISCOVER_WORKS', 'SYNC_WORK'))
          OR task_type = $2::text
        )
      ORDER BY priority DESC, chapter_sort_key ASC NULLS LAST, next_run_at ASC
      FOR UPDATE SKIP LOCKED
      LIMIT $3
    )
    UPDATE importer_queue q
    SET status = 'IMPORTING',
        locked_by = $4,
        locked_at = NOW(),
        lease_expires_at = NOW() + ($5::text || ' minutes')::interval,
        attempts = q.attempts + 1,
        updated_at = NOW()
    FROM to_lock
    WHERE q.id = to_lock.id
    RETURNING q.id, q.task_type, q.source, q.priority, q.payload, q.dedupe_key,
              q.status, q.attempts, q.max_attempts, q.locked_by, q.locked_at,
              q.lease_expires_at, q.next_run_at, q.last_error, q.chapter_sort_key;
  `;
    const res = await p.query(query, [source, taskType, batchSize, workerId, leaseMin]);
    return res.rows.map((r) => ({
        ...r,
        payload: typeof r.payload === 'string' ? JSON.parse(r.payload) : (r.payload || {}),
        chapter_sort_key: r.chapter_sort_key ? parseFloat(r.chapter_sort_key) : null,
    }));
}
/* 2. Direct Heartbeat Batch */
export async function heartbeatDirect(workerId, jobs) {
    const p = getYugabytePool();
    const updates = [];
    for (const item of jobs) {
        const leaseMin = Math.max(1, Math.min(60, item.leaseDurationMinutes || 5));
        const res = await p.query(`
      UPDATE importer_queue
      SET lease_expires_at = NOW() + ($1::text || ' minutes')::interval,
          progress_current = COALESCE($2, progress_current),
          progress_total = COALESCE($3, progress_total),
          progress_stage = COALESCE($4, progress_stage),
          updated_at = NOW()
      WHERE id = $5 AND status = 'IMPORTING' AND locked_by = $6
      RETURNING id, cancel_requested, status;
    `, [leaseMin, item.progressCurrent ?? null, item.progressTotal ?? null, item.progressStage ?? null, item.jobId, workerId]);
        if (res.rows.length > 0) {
            updates.push({
                jobId: item.jobId,
                status: res.rows[0].status,
                cancelRequested: Boolean(res.rows[0].cancel_requested),
                renewed: true,
            });
        }
        else {
            updates.push({ jobId: item.jobId, status: 'UNKNOWN', cancelRequested: false, renewed: false });
        }
    }
    return updates;
}
/* 3. Direct Fail / Retry Batch */
export async function failBatchDirect(jobs) {
    const p = getYugabytePool();
    let updatedCount = 0;
    for (const item of jobs) {
        const res = await p.query(`
      UPDATE importer_queue
      SET status = $1,
          last_error = $2,
          locked_by = NULL,
          locked_at = NULL,
          lease_expires_at = NULL,
          next_run_at = NOW() + ($3::text || ' seconds')::interval,
          updated_at = NOW()
      WHERE id = $4;
    `, [item.status || 'RETRY', item.error || 'Unknown error', item.retryDelaySeconds || 60, item.jobId]);
        updatedCount += res.rowCount || 0;
    }
    return updatedCount;
}
/* 4. Direct Atomic Chapter Publication Batch */
export async function publishBatchDirect(payload) {
    const startTime = Date.now();
    if (!payload.pages || payload.pages.length === 0) {
        throw new Error('SAFETY_GUARD_REJECTED: Publication refused because page count is 0.');
    }
    const chapterNumber = payload.chapter?.number;
    if (typeof chapterNumber !== 'number' || isNaN(chapterNumber) || chapterNumber < 0) {
        throw new Error('Invalid chapter number');
    }
    const sortKey = typeof payload.chapter.chapterSortKey === 'number'
        ? Number(payload.chapter.chapterSortKey.toFixed(4))
        : computeCanonicalChapterKey(chapterNumber, payload.chapter.title).sortKey;
    const nowIso = new Date().toISOString();
    const p = getYugabytePool();
    const client = await p.connect();
    try {
        let tBegin = 0;
        let tValidation = 0;
        let tMasterCte = 0;
        let tMediaPagesCte = 0;
        let tCommit = 0;
        let queryCount = 0;
        let roundTrips = 0;
        let rowsWritten = 0;
        // 1. BEGIN (Round-Trip 1)
        const t0Begin = Date.now();
        await client.query('BEGIN');
        tBegin = Date.now() - t0Begin;
        queryCount++;
        roundTrips++;
        const t0Val = Date.now();
        // 1.1 Canonical Work Resolution (if workId is not provided)
        let workId = payload.work?.id || null;
        if (!workId && payload.workMapping?.source && payload.workMapping?.sourceWorkId) {
            const mapRes = await client.query('SELECT work_id FROM importer_work_mappings WHERE source = $1 AND source_work_id = $2 LIMIT 1', [payload.workMapping.source, payload.workMapping.sourceWorkId]);
            queryCount++;
            roundTrips++;
            if (mapRes.rows.length > 0) {
                workId = mapRes.rows[0].work_id;
            }
        }
        if (!workId && payload.work?.slug) {
            const slugRes = await client.query('SELECT id FROM works WHERE slug = $1 LIMIT 1', [payload.work.slug]);
            queryCount++;
            roundTrips++;
            if (slugRes.rows.length > 0) {
                workId = slugRes.rows[0].id;
            }
        }
        // STRUCTURAL HARD REJECTION: target canonical work MUST exist
        if (!workId) {
            await client.query('ROLLBACK');
            throw new Error('SAFETY_GUARD_REJECTED: Publication refused because target canonical work does not exist. publish-batch cannot create works automatically.');
        }
        // Defense-in-depth heuristics: reject phantom/synthetic patterns
        const workTitle = (payload.work?.title || '').trim();
        const workSlug = (payload.work?.slug || '').trim();
        if (/^Obra\s+[0-9a-fA-F]+$/i.test(workTitle) ||
            workTitle === 'Benchmark Work' ||
            workTitle === 'Sem título' ||
            /^[0-9]+$/.test(workSlug)) {
            const canonRes = await client.query('SELECT title, slug FROM works WHERE id = $1', [workId]);
            queryCount++;
            roundTrips++;
            const canonTitle = (canonRes.rows[0]?.title || '').trim();
            const canonSlug = (canonRes.rows[0]?.slug || '').trim();
            if (/^Obra\s+[0-9a-fA-F]+$/i.test(canonTitle) ||
                canonTitle === 'Benchmark Work' ||
                canonTitle === 'Sem título' ||
                /^[0-9]+$/.test(canonSlug)) {
                await client.query('ROLLBACK');
                throw new Error('SAFETY_GUARD_REJECTED: Publication refused because target work matches phantom/synthetic naming pattern.');
            }
        }
        tValidation = Date.now() - t0Val;
        // 2. Prepare Media & Pages Arrays
        const mediaIds = [];
        const providerKeys = [];
        const mimes = [];
        const widths = [];
        const heights = [];
        const bytesArr = [];
        const sha256s = [];
        const botRefs = [];
        const shardIds = [];
        const positions = [];
        for (let i = 0; i < payload.pages.length; i++) {
            const p = payload.pages[i];
            const mId = p.mediaId || crypto.randomUUID();
            p.mediaId = mId;
            mediaIds.push(mId);
            providerKeys.push(p.providerKey);
            mimes.push(p.mime);
            widths.push(p.width);
            heights.push(p.height);
            bytesArr.push(p.bytes);
            sha256s.push(p.sha256 || crypto.createHash('sha256').update(p.providerKey || crypto.randomUUID()).digest('hex'));
            botRefs.push(p.botReference || 'MANGA_STORAGE_01');
            shardIds.push(p.storageShardId && /^[0-9a-f-]{36}$/i.test(p.storageShardId) ? p.storageShardId : null);
            positions.push(p.position || (i + 1));
        }
        // 3. Giant Compound CTE (Round-Trip 2: All tables updated in a single atomic SQL statement)
        const t0GiantCte = Date.now();
        const newChapterId = crypto.randomUUID();
        const newWorkMappingId = crypto.randomUUID();
        const newChapterMappingId = crypto.randomUUID();
        const wmSource = payload.workMapping?.source || payload.chapter.source;
        const wmSourceWorkId = payload.workMapping?.sourceWorkId || payload.chapter.source;
        const wmSourceSlug = payload.workMapping?.sourceSlug || '';
        const wmSourceTitle = payload.workMapping?.sourceTitle || payload.work.title || '';
        const wmMeta = typeof payload.workMapping?.metadata === 'object'
            ? JSON.stringify(payload.workMapping.metadata)
            : (payload.workMapping?.metadata || '{}');
        const wmConfidence = payload.workMapping?.confidenceScore ?? 1.0;
        const giantRes = await client.query(`
      WITH upsert_ch AS (
        INSERT INTO chapters (
          id, work_id, number, title, published_at, origin, views_total, created_at
        ) VALUES (
          $1::uuid, $2::uuid, $3::numeric, $4::text, $5::timestamptz, 'importer', 0, $5::timestamptz
        )
        ON CONFLICT (work_id, number) DO UPDATE SET
          title = EXCLUDED.title,
          published_at = COALESCE(chapters.published_at, EXCLUDED.published_at)
        RETURNING id, published_at
      ),
      upsert_wm AS (
        INSERT INTO importer_work_mappings (
          id, source, source_work_id, work_id, source_slug, source_title,
          sync_status, metadata, confidence_score, is_primary, match_method,
          last_synced_at, created_at, updated_at
        ) VALUES (
          $6::uuid, $7::text, $8::text, $2::uuid, $9::text, $10::text, 'ACTIVE',
          $11::jsonb, $12::numeric, true, 'EXACT', $5::timestamptz, $5::timestamptz, $5::timestamptz
        )
        ON CONFLICT (source, source_work_id) DO UPDATE SET
          work_id = EXCLUDED.work_id,
          source_slug = EXCLUDED.source_slug,
          source_title = EXCLUDED.source_title,
          sync_status = 'ACTIVE',
          metadata = EXCLUDED.metadata,
          last_synced_at = EXCLUDED.last_synced_at,
          updated_at = EXCLUDED.updated_at
        RETURNING id
      ),
      upsert_cm AS (
        INSERT INTO importer_chapter_mappings (
          id, source, source_chapter_id, chapter_id, work_id, work_mapping_id,
          chapter_number, chapter_sort_key, page_count, is_page_provider, status, is_gap,
          created_at, updated_at
        )
        SELECT
          $13::uuid, $7::text, $14::text, c.id, $2::uuid, wm.id,
          $3::numeric, $15::numeric, $16::int, true, 'COMPLETED', false,
          $5::timestamptz, $5::timestamptz
        FROM upsert_ch c, upsert_wm wm
        ON CONFLICT (source, source_chapter_id) DO UPDATE SET
          chapter_id = EXCLUDED.chapter_id,
          work_id = EXCLUDED.work_id,
          work_mapping_id = EXCLUDED.work_mapping_id,
          chapter_number = EXCLUDED.chapter_number,
          chapter_sort_key = EXCLUDED.chapter_sort_key,
          page_count = EXCLUDED.page_count,
          is_page_provider = EXCLUDED.is_page_provider,
          status = 'COMPLETED',
          is_gap = false,
          last_error = NULL,
          updated_at = EXCLUDED.updated_at
        RETURNING id
      ),
      upd_queue AS (
        UPDATE importer_queue
        SET status = 'COMPLETED',
            locked_by = NULL,
            locked_at = NULL,
            lease_expires_at = NULL,
            last_error = NULL,
            progress_current = $16::int,
            progress_total = $16::int,
            progress_stage = 'COMPLETED',
            updated_at = NOW()
        WHERE id = $17::uuid AND $17::uuid IS NOT NULL
        RETURNING id
      ),
      upd_work AS (
        UPDATE works
        SET latest_chapter_published_at = CASE 
              WHEN latest_chapter_published_at IS NULL THEN (SELECT published_at FROM upsert_ch)
              ELSE GREATEST(latest_chapter_published_at, (SELECT published_at FROM upsert_ch))
            END,
            updated_at = NOW()
        WHERE id = $2::uuid
        RETURNING id
      ),
      ins_media AS (
        INSERT INTO media (
          id, provider, provider_key, mime, width, height, bytes, sha256,
          created_by, storage_ready, purpose, bot_reference, storage_shard_id, chapter_id, created_at
        )
        SELECT 
          m.id, 'telegram', m.provider_key, m.mime, m.width, m.height, m.bytes, m.sha256,
          '732fbe87-5040-41fb-9983-0aedb2af44c8'::uuid, true, 'editorial', m.bot_ref, m.shard_id, (SELECT id FROM upsert_ch), $5::timestamptz
        FROM UNNEST(
          $18::uuid[], $19::text[], $20::text[], $21::int[], $22::int[], $23::bigint[], $24::text[], $25::text[], $26::uuid[]
        ) AS m(id, provider_key, mime, width, height, bytes, sha256, bot_ref, shard_id)
        ON CONFLICT (id) DO UPDATE SET
          provider_key = EXCLUDED.provider_key,
          storage_ready = true,
          chapter_id = EXCLUDED.chapter_id
        RETURNING id
      ),
      del_surplus AS (
        DELETE FROM pages WHERE chapter_id = (SELECT id FROM upsert_ch) AND position > $16::int
        RETURNING position
      ),
      ins_pages AS (
        INSERT INTO pages (chapter_id, position, media_id, width, height)
        SELECT (SELECT id FROM upsert_ch), p.pos, p.m_id, p.w, p.h
        FROM UNNEST($27::int[], $18::uuid[], $21::int[], $22::int[]) AS p(pos, m_id, w, h)
        ON CONFLICT (chapter_id, position) DO UPDATE SET
          media_id = EXCLUDED.media_id,
          width = EXCLUDED.width,
          height = EXCLUDED.height
        RETURNING position
      )
      SELECT c.id as chapter_id, c.published_at, wm.id as work_mapping_id
      FROM upsert_ch c, upsert_wm wm;
    `, [
            newChapterId, workId, chapterNumber, payload.chapter.title || `Capítulo ${chapterNumber}`, nowIso,
            newWorkMappingId, wmSource, wmSourceWorkId, wmSourceSlug, wmSourceTitle,
            wmMeta, wmConfidence,
            newChapterMappingId, payload.chapter.sourceChapterId, sortKey, payload.pages.length,
            payload.jobId || null,
            mediaIds, providerKeys, mimes, widths, heights, bytesArr, sha256s, botRefs, shardIds,
            positions
        ]);
        queryCount++;
        roundTrips++;
        rowsWritten += 4 + (payload.jobId ? 1 : 0) + (payload.pages.length * 2);
        tMasterCte = Date.now() - t0GiantCte;
        const chapterId = giantRes.rows[0].chapter_id;
        const actualPublishedAt = giantRes.rows[0].published_at
            ? new Date(giantRes.rows[0].published_at).toISOString()
            : nowIso;
        // 4. COMMIT (Round-Trip 3)
        const t0Commit = Date.now();
        await client.query('COMMIT');
        queryCount++;
        roundTrips++;
        tCommit = Date.now() - t0Commit;
        const dbDurationMs = Date.now() - startTime;
        const sqlExecutionMs = tBegin + tValidation + tMasterCte + tCommit;
        return {
            success: true,
            workId,
            chapterId,
            pageCount: payload.pages.length,
            publishedAt: actualPublishedAt,
            dbDurationMs,
            metrics: {
                beginMs: tBegin,
                validationMs: tValidation,
                masterCteMs: tMasterCte,
                mediaPagesCteMs: 0,
                commitMs: tCommit,
                sqlExecutionMs,
                queryCount,
                roundTrips,
                rowsWritten,
                // Compatibility aliases for reporting
                workMappingMs: Math.round(tMasterCte / 6),
                chapterMs: Math.round(tMasterCte / 6),
                chapterMappingMs: Math.round(tMasterCte / 6),
                workUpdateMs: Math.round(tMasterCte / 6),
                mediaMs: Math.round(tMasterCte / 6),
                pagesDeleteMs: 0,
                pagesInsertMs: Math.round(tMasterCte / 6),
                queueUpdateMs: 0,
            },
        };
    }
    catch (err) {
        await client.query('ROLLBACK').catch(() => { });
        throw err;
    }
    finally {
        client.release();
    }
}
