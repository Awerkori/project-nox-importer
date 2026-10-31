import { PGlite } from '@electric-sql/pglite';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import type { Pool } from 'pg';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { AsyncSemaphore } from '../../core/concurrency.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { telemetryCollector } from '../../core/telemetry-collector.js';
import { ImporterEngine } from '../../core/engine.js';
import { DURABLE_RECEIPT_BATCH_JOIN_MS, DURABLE_RESERVATION_BATCH_JOIN_MS, LiteExecutor, UploadReceiptBatcher, UploadReservationBatcher } from '../executor.js';
import { PageByteBudget } from '../page-byte-budget.js';
import { ImporterLiteRuntime } from '../runtime.js';
import { DUPLICATE_CANONICAL_BATCH_REASON, LiteRepository } from '../repository.js';
import type { SourceRegistry } from '../../sources/registry.js';
import type { StorageProvider } from '../../storage/provider.js';

const migration = readFileSync(resolve(process.cwd(), 'migrations/20261010090000_importer_lite_v2.sql'), 'utf8');
const readerMigration = readFileSync(resolve(process.cwd(), 'migrations/20260914171000_atomic_reader_repair.sql'), 'utf8');
const fairHistoricalP1Migration = readFileSync(resolve(process.cwd(), 'migrations/20261012083000_lite_fair_historical_p1_catalog.sql'), 'utf8');
const deferHealthyP1CatalogMigration = readFileSync(resolve(process.cwd(), 'migrations/20261013040000_lite_defer_healthy_p1_catalog.sql'), 'utf8');
const manifestGapRepairIndexMigration = readFileSync(resolve(process.cwd(), 'migrations/20261014031500_lite_manifest_gap_repair_index.sql'), 'utf8');
const orderedManifestClaimMigration = readFileSync(resolve(process.cwd(), 'migrations/20261014090000_lite_claim_manifest_chapters_in_order.sql'), 'utf8');
const manifestOrderBeforeRetryMigration = readFileSync(resolve(process.cwd(), 'migrations/20261014091500_lite_claim_manifest_order_before_retry.sql'), 'utf8');
const sameWorkGapPriorityMigration = readFileSync(resolve(process.cwd(), 'migrations/20261014190000_lite_prioritize_recoverable_same_work_gaps.sql'), 'utf8');
const sourceMetadataRefetchMigration = readFileSync(resolve(process.cwd(), 'migrations/20261014180000_metadata_source_refetch.sql'), 'utf8');
const sourceMetadataRefetchIndexesMigration = readFileSync(resolve(process.cwd(), 'migrations/20261014181500_metadata_source_refetch_indexes.sql'), 'utf8');
const sourceMetadataRefetchClaimIndexMigration = readFileSync(resolve(process.cwd(), 'migrations/20261016221500_metadata_refetch_claim_idx.sql'), 'utf8');
const sourceMetadataExpiredLeaseClaimMigration = readFileSync(resolve(process.cwd(), 'migrations/20261019110000_claim_expired_source_metadata_lease.sql'), 'utf8');
const metadataProductMetricsMigration = readFileSync(resolve(process.cwd(), 'migrations/20261015043000_metadata_product_activation_metrics.sql'), 'utf8');
const sourceMetadataProbeCursorMigration = readFileSync(resolve(process.cwd(), 'migrations/20261015060000_metadata_source_probe_cursor.sql'), 'utf8');
const publicPresentationAdmissionMigration = readFileSync(resolve(process.cwd(), 'migrations/20261015194000_lite_public_presentation_admission.sql'), 'utf8');
const zeroDecimalChapterIdentityMigration = readFileSync(resolve(process.cwd(), 'migrations/20261015223000_canonical_zero_decimal_chapters.sql'), 'utf8');
const presentableUnpublishedAdmissionMigration = readFileSync(resolve(process.cwd(), 'migrations/20261015230000_lite_admit_presentable_unpublished_work.sql'), 'utf8');
const newWorkCatalogLaneMigration = readFileSync(resolve(process.cwd(), 'migrations/20261016070000_lite_new_work_catalog_lane.sql'), 'utf8');
const runnableNewWorkCatalogLaneMigration = readFileSync(resolve(process.cwd(), 'migrations/20261016074500_lite_new_work_catalog_runnable_source.sql'), 'utf8');
const fairReadyWorkClaimIndexMigration = readFileSync(resolve(process.cwd(), 'migrations/20261018213000_lite_ready_work_fair_claim_idx.sql'), 'utf8');
const v4ActiveWorkIndexMigration = readFileSync(resolve(process.cwd(), 'migrations/20261019021500_lite_v4_active_work_index.sql'), 'utf8');
const multiWorkClaimMigration = readFileSync(resolve(process.cwd(), 'migrations/20261025044500_multi_work_claim_loop_and_concurrency_unblock.sql'), 'utf8');
const boundedClaimMigration = readFileSync(resolve(process.cwd(), 'migrations/20261025063000_bound_claim_probe_and_metadata_pool.sql'), 'utf8');
const sourceCooldownDueIndexMigration = readFileSync(resolve(process.cwd(), 'migrations/20261025070000_lite_source_cooldown_due_index.sql'), 'utf8');
const progressSnapshotIndexesMigration = readFileSync(resolve(process.cwd(), 'migrations/20261025073000_lite_progress_snapshot_indexes.sql'), 'utf8');
const earliestPendingChapterClaimMigration = readFileSync(resolve(process.cwd(), 'migrations/20261026053000_lite_claim_earliest_pending_chapter.sql'), 'utf8');
const readyWorkCanonicalFrontierMigration = readFileSync(resolve(process.cwd(), 'migrations/20261028043000_lite_ready_work_canonical_frontier.sql'), 'utf8');
const p1FocusGroupsAndFrontierFixMigration = readFileSync(resolve(process.cwd(), 'migrations/20261028190000_lite_p1_focus_groups_and_frontier_fix.sql'), 'utf8');
const claimFastChapterPassMigration = readFileSync(resolve(process.cwd(), 'migrations/20261028202000_lite_claim_fast_chapter_pass.sql'), 'utf8');
const directPublishStoredProcedureMigration = readFileSync(resolve(process.cwd(), 'migrations/20261029170000_lite_direct_publish_stored_procedure.sql'), 'utf8');
const optimizeYsqlCpuAndP0MonitorMigration = readFileSync(resolve(process.cwd(), 'migrations/20261031053000_optimize_ysql_cpu_and_p0_monitor.sql'), 'utf8');
const extremeYsqlCpuAndIndexMigration = readFileSync(resolve(process.cwd(), 'migrations/20261031163000_extreme_ysql_cpu_and_index_optimization.sql'), 'utf8');
const fixReadyWorkPublishPriorityMigration = readFileSync(resolve(process.cwd(), 'migrations/20261031210000_fix_ready_work_publish_priority.sql'), 'utf8');

const ids = {
  staff: '00000000-0000-4000-8000-000000000001',
  p0: '00000000-0000-4000-8000-000000000002',
  p1a: '00000000-0000-4000-8000-000000000003',
  p1b: '00000000-0000-4000-8000-000000000004',
  publish: '00000000-0000-4000-8000-000000000005',
  chapter: '00000000-0000-4000-8000-000000000006',
  cover: '00000000-0000-4000-8000-000000000007',
  page: '00000000-0000-4000-8000-000000000008',
  legacy: '00000000-0000-4000-8000-000000000010',
};

/** PGlite exposes the small pg surface used by LiteRepository. */
function poolFor(db: PGlite): Pick<Pool, 'query' | 'connect'> {
  return {
    query: (text: string, values?: unknown[]) => db.query(text, values),
    connect: async () => ({
      query: (text: string, values?: unknown[]) => db.query(text, values),
      release: () => undefined,
    }),
  } as unknown as Pick<Pool, 'query' | 'connect'>;
}

async function createSchema(db: PGlite, options: { optionalRoles?: boolean } = {}): Promise<void> {
  const optionalRoles = options.optionalRoles ?? true;
  await db.exec(`
    ${optionalRoles ? 'CREATE ROLE anon; CREATE ROLE authenticated; CREATE ROLE service_role;' : ''}
    CREATE TABLE works (
      id uuid PRIMARY KEY, title text, slug text, cover_id uuid,
      published boolean NOT NULL DEFAULT false, latest_chapter_published_at timestamptz,
      status text NOT NULL DEFAULT 'ONGOING',
      aliases text[] NOT NULL DEFAULT ARRAY[]::text[], views_total integer NOT NULL DEFAULT 0,
      author text, artist text, synopsis text, description text, kind text, year integer,
      updated_at timestamptz
    );
    CREATE TABLE chapters (
      id uuid PRIMARY KEY, work_id uuid NOT NULL REFERENCES works(id), number numeric,
      display_number text, canonical_chapter_key text, chapter_order_key text,
      chapter_kind text, edition text, language text,
      title text, origin text NOT NULL DEFAULT 'IMPORTER', published_at timestamptz,
      is_fresh_release boolean NOT NULL DEFAULT false
    );
    CREATE UNIQUE INDEX chapters_work_canonical_chapter_key_uq
      ON chapters(work_id,canonical_chapter_key) WHERE canonical_chapter_key IS NOT NULL;
    CREATE TABLE media (
      id uuid PRIMARY KEY, provider text, provider_key text, bot_reference text,
      storage_shard_id uuid, mime text, width integer, height integer, bytes integer,
      sha256 text, created_by uuid, storage_ready boolean NOT NULL DEFAULT false,
      status text NOT NULL DEFAULT 'ACTIVE', purpose text, created_at timestamptz NOT NULL DEFAULT now()
    );
    CREATE TABLE pages (
      chapter_id uuid NOT NULL REFERENCES chapters(id), position integer NOT NULL,
      media_id uuid NOT NULL REFERENCES media(id), width integer, height integer,
      UNIQUE(chapter_id, position)
    );
    CREATE TABLE importer_sources (
      id text PRIMARY KEY, enabled boolean NOT NULL DEFAULT true,
      status text NOT NULL DEFAULT 'ACTIVE', sync_interval_minutes integer NOT NULL DEFAULT 30,
      cooldown_until timestamptz NULL,
      catalog_discovery_enabled boolean NOT NULL DEFAULT true,
      chapter_ingestion_enabled boolean NOT NULL DEFAULT true,
      blocked_reason text NULL,
      blocked_details jsonb NULL
    );
    CREATE TABLE importer_staff_requests (work_id uuid NOT NULL REFERENCES works(id), status text NOT NULL);
    CREATE TABLE importer_chapter_mappings (
      source text NOT NULL, source_chapter_id text NOT NULL, chapter_id uuid NULL REFERENCES chapters(id),
      PRIMARY KEY(source,source_chapter_id)
    );
    CREATE TABLE importer_work_mappings (
      id uuid PRIMARY KEY, source text NOT NULL, source_work_id text NOT NULL, work_id uuid,
      source_slug text NOT NULL DEFAULT '', source_title text NOT NULL DEFAULT '',
      metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
      sync_status text NOT NULL DEFAULT 'WAITING_ADMISSION', last_synced_at timestamptz NULL,
      updated_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE(source,source_work_id)
    );
    CREATE TABLE tags (id uuid PRIMARY KEY DEFAULT '00000000-0000-4000-8000-000000000399', name text NOT NULL UNIQUE, slug text NOT NULL UNIQUE, kind text NOT NULL DEFAULT 'GENRE');
    CREATE TABLE work_tags (work_id uuid NOT NULL, tag_id uuid NOT NULL, system_generated boolean NOT NULL DEFAULT true, UNIQUE(work_id,tag_id));
    CREATE TABLE settings (key text PRIMARY KEY, value text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
    CREATE TABLE importer_publication_events (
      chapter_id uuid PRIMARY KEY, transition_at timestamptz NOT NULL, recorded_at timestamptz NOT NULL,
      bucket_minute timestamptz NOT NULL, is_fresh_release boolean NOT NULL
    );
    INSERT INTO importer_sources (id) VALUES ('source-a'), ('source-b');
    INSERT INTO works(id,title,slug) VALUES
      ('${ids.staff}','Staff','staff'), ('${ids.p0}','P0','p0'),
      ('${ids.p1a}','P1 A','p1-a'), ('${ids.p1b}','P1 B','p1-b'),
      ('${ids.publish}','Publish','publish');
    INSERT INTO chapters(id,work_id,number,title) VALUES
      ('${ids.chapter}','${ids.publish}',1,'Chapter 1');
    INSERT INTO media(id,mime,width,height,bytes,sha256,storage_ready,status) VALUES
      ('${ids.cover}','image/jpeg',600,900,2000,'cover-sha',true,'ACTIVE'),
      ('${ids.page}','image/jpeg',800,1200,3000,'page-sha',true,'ACTIVE');
    -- Most catalog scenarios model already-presentable existing works. Tests
    -- for the catalog admission gate explicitly clear a cover instead.
    UPDATE works SET cover_id='${ids.cover}';
  `);
  if (optionalRoles) await db.exec(readerMigration);
  await db.exec(migration);
  await db.exec(fairHistoricalP1Migration);
  await db.exec(deferHealthyP1CatalogMigration);
  await db.exec(manifestGapRepairIndexMigration);
  await db.exec(orderedManifestClaimMigration);
  await db.exec(manifestOrderBeforeRetryMigration);
  await db.exec(sameWorkGapPriorityMigration);
  await db.exec(sourceMetadataRefetchMigration);
  await db.exec(sourceMetadataRefetchIndexesMigration);
  await db.exec(sourceMetadataRefetchClaimIndexMigration);
  await db.exec(sourceMetadataExpiredLeaseClaimMigration);
  await db.exec(metadataProductMetricsMigration);
  await db.exec(sourceMetadataProbeCursorMigration);
  await db.exec(publicPresentationAdmissionMigration);
  await db.exec(zeroDecimalChapterIdentityMigration);
  await db.exec(presentableUnpublishedAdmissionMigration);
  await db.exec(newWorkCatalogLaneMigration);
  await db.exec(runnableNewWorkCatalogLaneMigration);
  await db.exec(fairReadyWorkClaimIndexMigration);
  await db.exec(v4ActiveWorkIndexMigration);
  await db.exec(sourceCooldownDueIndexMigration);
  await db.exec(progressSnapshotIndexesMigration);
  await db.exec(earliestPendingChapterClaimMigration);
  await db.exec(readyWorkCanonicalFrontierMigration);
  await db.exec(p1FocusGroupsAndFrontierFixMigration);
  await db.exec(claimFastChapterPassMigration);
  await db.exec(directPublishStoredProcedureMigration);
  await db.exec(optimizeYsqlCpuAndP0MonitorMigration);
  // PGlite cannot implement Yugabyte's ybgin access method. Preserve the
  // production migration and omit only that index in this local fixture.
  await db.exec('ALTER TABLE works ADD COLUMN IF NOT EXISTS search_text text');
  await db.exec(extremeYsqlCpuAndIndexMigration.replace(/CREATE INDEX IF NOT EXISTS idx_works_search_text_trgm\s+ON public\.works USING ybgin \(search_text gin_trgm_ops\)\s+WHERE published = true;/, ''));
  await db.exec(fixReadyWorkPublishPriorityMigration);
}

describe('LiteRepository against the additive Lite migration', () => {
  let db: PGlite;
  let repo: LiteRepository;

  beforeEach(async () => {
    db = new PGlite();
    await createSchema(db);
    repo = new LiteRepository(poolFor(db));
  }, 20_000);

  it.each([
    { name: 'Case A: work 1, source 1–37', existing: [1], manifest: Array.from({ length: 37 }, (_, i) => i + 1) },
    { name: 'Case B: work 1–5, source 1–18', existing: [1, 2, 3, 4, 5], manifest: Array.from({ length: 18 }, (_, i) => i + 1) },
    // Case E: a first catalog pass must retain the whole observed manifest;
    // it may not jump from the first chapter directly to its newest release.
    { name: 'Case E: new work, source manifest 1–100', existing: [1], manifest: Array.from({ length: 100 }, (_, i) => i + 1) },
  ])('materializes old confirmed chapters and claims the earliest P1 before the new P0 ($name)', async ({ existing, manifest }) => {
    const workId = ids.p1a;
    await db.query(`UPDATE works SET published=true WHERE id=$1`, [workId]);
    for (const number of existing) {
      const key = `pt-br|default|chapter|${number}|-`;
      const chapterId = `00000000-0000-4000-8000-${String(number).padStart(12, '0')}`;
      await db.query(`INSERT INTO chapters(id,work_id,number,display_number,canonical_chapter_key)
        VALUES($1::uuid,$2::uuid,$3::numeric,$4,$5)`, [chapterId, workId, number, String(number), key]);
      await db.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key)
        VALUES($1::uuid,$2,$3::uuid,$4::text,$4::text)`, [workId, key, chapterId, number]);
    }
    for (const number of manifest) {
      const key = `pt-br|default|chapter|${number}|-`;
      const order = `n|${String(number).padStart(12, '0')}|000000000000|||`;
      await db.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,confirmed,has_chapter)
        VALUES($1::uuid,$2,'source-a','work-a',$3,'pt-BR','default',$4,true,true)`, [workId, key, `source-${number}`, order]);
    }
    const newest = manifest.at(-1)!;
    const newestKey = `pt-br|default|chapter|${newest}|-`;
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId, workKey: `work:${workId}`, chapterKey: newestKey,
      priority: 'P0', idempotencyKey: `lite:chapter:${workId}:${newestKey}`, payload: { orderKey: `n|${String(newest).padStart(12, '0')}|000000000000|||` } });

    const expectedRepairs = manifest.length - existing.length - 1;
    expect(await repo.materializeConfirmedMissingP1Tasks(100)).toBe(expectedRepairs);
    const claimed = await repo.claim('same-work-gap-test', 120);
    const firstMissing = existing.at(-1)! + 1;
    expect(claimed?.chapterKey).toBe(`pt-br|default|chapter|${firstMissing}|-`);
    expect(claimed?.priority).toBe('P1');
  });

  it('projects a work at its executable P1 frontier until its later P0 release can legally advance', async () => {
    const workId = ids.p1a;
    const predecessorKey = 'pt-br|default|chapter|71|-';
    const laterP0Key = 'pt-br|default|chapter|73|-';
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId, workKey: `work:${workId}`,
      chapterKey: predecessorKey, priority: 'P1', idempotencyKey: 'frontier-p1-71',
      payload: { orderKey: 'n|000000000071|000000000000|||' },
    });
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId, workKey: `work:${workId}`,
      chapterKey: laterP0Key, priority: 'P0', idempotencyKey: 'frontier-p0-73',
      payload: { orderKey: 'n|000000000073|000000000000|||' },
    });
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId: ids.p0, workKey: `work:${ids.p0}`,
      chapterKey: 'pt-br|default|chapter|1|-', priority: 'P0', idempotencyKey: 'independent-p0',
      payload: { orderKey: 'n|000000000001|000000000000|||' },
    });

    // A P0 chapter behind a confirmed P1 predecessor must not make the whole
    // work look P0-ready to the bounded fair probe.  The real frontier is P1.
    expect((await db.query(`SELECT priority,operation_rank FROM importer_lite_ready_works
      WHERE work_key=$1`, [`work:${workId}`])).rows[0]).toEqual({ priority: 2, operation_rank: 1 });

    const independent = await repo.claim('frontier-worker', 120);
    expect(independent?.idempotencyKey).toBe('independent-p0');
    await repo.finish(independent!, 'frontier-worker', { state: 'DONE' });

    const predecessor = await repo.claim('frontier-worker', 120);
    expect(predecessor?.idempotencyKey).toBe('frontier-p1-71');
    await repo.finish(predecessor!, 'frontier-worker', { state: 'DONE' });

    // Once the dependency finishes, the actual P0 release immediately gains
    // normal P0 precedence rather than being demoted with its old barrier.
    expect((await db.query(`SELECT priority,operation_rank FROM importer_lite_ready_works
      WHERE work_key=$1`, [`work:${workId}`])).rows[0]).toEqual({ priority: 1, operation_rank: 1 });
    expect((await repo.claim('frontier-worker', 120))?.idempotencyKey).toBe('frontier-p0-73');
  });

  it('keeps a retrying canonical predecessor as the ready-work boundary', async () => {
    const workId = ids.p1a;
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId, workKey: `work:${workId}`,
      chapterKey: 'pt-br|default|chapter|71|-', priority: 'P1', idempotencyKey: 'retry-frontier-p1',
      payload: { orderKey: 'n|000000000071|000000000000|||' },
    });
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId, workKey: `work:${workId}`,
      chapterKey: 'pt-br|default|chapter|73|-', priority: 'P0', idempotencyKey: 'retry-frontier-p0',
      payload: { orderKey: 'n|000000000073|000000000000|||' },
    });
    await db.query(`UPDATE importer_lite_tasks SET state='RETRY_WAIT',next_run_at=now()+interval '5 minutes'
      WHERE idempotency_key='retry-frontier-p1'`);
    await (repo as any).refreshReadyWork((repo as any).db, `work:${workId}`);

    const projection = (await db.query(`SELECT priority,next_run_at>now() AS deferred
      FROM importer_lite_ready_works WHERE work_key=$1`, [`work:${workId}`])).rows[0];
    expect(projection).toEqual({ priority: 2, deferred: true });
    expect(await repo.claim('retry-frontier-worker', 120)).toBeNull();
  });

  it('does not invent numeric gaps when the source confirms only chapters 1 and 37', async () => {
    const workId = ids.p1a;
    await db.query(`UPDATE works SET published=true WHERE id=$1`, [workId]);
    for (const number of [1, 37]) {
      const key = `pt-br|default|chapter|${number}|-`;
      const chapterId = `00000000-0000-4000-8000-${String(number).padStart(12, '0')}`;
      await db.query(`INSERT INTO chapters(id,work_id,number,display_number,canonical_chapter_key)
        VALUES($1::uuid,$2::uuid,$3::numeric,$4,$5)`, [chapterId, workId, number, String(number), key]);
      await db.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key)
        VALUES($1::uuid,$2,$3::uuid,$4::text,$4::text)`, [workId, key, chapterId, number]);
      await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,confirmed,has_chapter)
      VALUES($1::uuid,$2,'source-a','work-a',$3,'pt-BR','default',$4,true,true)`, [workId,
        key, `source-${number}`, `n|${String(number).padStart(12, '0')}|000000000000|||`]);
    }
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId, workKey: `work:${workId}`, chapterKey: 'pt-br|default|chapter|37|-',
      priority: 'P0', idempotencyKey: `lite:chapter:${workId}:pt-br|default|chapter|37|-` });
    expect(await repo.materializeConfirmedMissingP1Tasks(100)).toBe(0);
    expect((await repo.claim('no-invention-test', 120))?.idempotencyKey)
      .toBe(`lite:chapter:${workId}:pt-br|default|chapter|37|-`);
  });

  it('claims an old confirmed gap before newer same-work chapters and does not rematerialize it after import', async () => {
    const workId = ids.p1a;
    await db.query(`UPDATE works SET published=true WHERE id=$1`, [workId]);
    for (const number of [5, 7]) {
      const key = `pt-br|default|chapter|${number}|-`;
      const chapterId = `00000000-0000-4000-8000-${String(number).padStart(12, '0')}`;
      await db.query(`INSERT INTO chapters(id,work_id,number,display_number,canonical_chapter_key)
        VALUES($1::uuid,$2::uuid,$3::numeric,$4,$5)`, [chapterId,workId,number,String(number),key]);
      await db.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key)
        VALUES($1::uuid,$2,$3::uuid,$4,$4)`, [workId,key,chapterId,String(number)]);
    }
    for (const number of [6,20,21]) {
      const key = `pt-br|default|chapter|${number}|-`;
      const order = `n|${String(number).padStart(12,'0')}|000000000000|||`;
      await db.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,confirmed,has_chapter)
        VALUES($1::uuid,$2,'source-a','work-a',$3,'pt-BR','default',$4,true,true)`, [workId,key,`source-${number}`,order]);
    }

    // Model an older P1 backlog created before the rule existed, with newer
    // tasks already waiting and a distinct decimal chapter present in source
    // metadata. The normal missing chapter is still selected first.
    for (const [number,priority,order] of [
      [20,'P1','n|000000000020|000000000000|||'],
      [21,'P0','n|000000000021|000000000000|||'],
    ] as const) {
      await repo.enqueue({ taskType:'IMPORT_CHAPTER',workId,workKey:`work:${workId}`,
        chapterKey:`pt-br|default|chapter|${number}|-`,priority,
        idempotencyKey:`lite:chapter:${workId}:pt-br|default|chapter|${number}|-`,
        payload:{displayNumber:String(number),orderKey:order,manifestP1Repair:true} });
    }
    const inserted = await repo.materializeConfirmedMissingP1Tasks(20);
    expect(inserted).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE work_id=$1::uuid AND chapter_key IN ('pt-br|default|chapter|5.2|-','pt-br|default|special|6|extra')`,[workId])).rows[0].count).toBe(0);
    const first = await repo.claim('gap-before-continuation',120);
    expect(first?.chapterKey).toBe('pt-br|default|chapter|6|-');

    const chapterId = '00000000-0000-4000-8000-000000000099';
    await db.query(`INSERT INTO chapters(id,work_id,number,display_number,canonical_chapter_key)
      VALUES($1::uuid,$2::uuid,6,'6','pt-br|default|chapter|6|-')`, [chapterId,workId]);
    await db.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key)
      VALUES($1::uuid,'pt-br|default|chapter|6|-',$2::uuid,'6','6')`, [workId,chapterId]);
    await repo.finish(first!, 'gap-before-continuation', {state:'DONE'});
    expect(await repo.materializeConfirmedMissingP1Tasks(20)).toBe(0);
    expect((await db.query(`SELECT count(*)::int AS count FROM chapters
      WHERE work_id=$1::uuid AND canonical_chapter_key='pt-br|default|chapter|6|-'`,[workId])).rows[0].count).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE work_id=$1::uuid AND chapter_key='pt-br|default|chapter|6|-'`,[workId])).rows[0].count).toBe(1);
  });

  it('lets a newer P0 proceed when older manifest evidence has no currently eligible source', async () => {
    const workId = ids.p1a;
    await db.query(`UPDATE works SET published=true WHERE id=$1`, [workId]);
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS blocked_reason text, ADD COLUMN IF NOT EXISTS blocked_details jsonb`);
    for (const number of [2, 37]) await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,confirmed,has_chapter)
      VALUES($1::uuid,$2,'source-a','work-a',$3,'pt-BR','default',$4,true,true)`, [workId,
      `pt-br|default|chapter|${number}|-`, `source-${number}`, `n|${String(number).padStart(12, '0')}|000000000000|||`]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId, workKey: `work:${workId}`, chapterKey: 'pt-br|default|chapter|2|-',
      priority: 'P1', idempotencyKey: 'external-gap', payload: { manifestP1Repair: true,
        orderKey: 'n|000000000002|000000000000|||' } });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId, workKey: `work:${workId}`, chapterKey: 'pt-br|default|chapter|37|-',
      priority: 'P0', idempotencyKey: 'new-release-external-gap', payload: { orderKey: 'n|000000000037|000000000000|||' } });
    await db.query(`UPDATE importer_sources SET status='COOLDOWN',cooldown_until=now()+interval '1 hour',blocked_reason='TURNSTILE' WHERE id='source-a'`);

    expect((await repo.claim('external-gap-test', 120))?.idempotencyKey).toBe('new-release-external-gap');
  });

  afterEach(async () => { await db.close(); });

  it('applies the additive Lite schema without optional Supabase roles', async () => {
    const directYsql = new PGlite();
    try {
      await createSchema(directYsql, { optionalRoles: false });
      expect((await directYsql.query(`SELECT to_regclass('public.importer_lite_tasks') AS tasks`)).rows[0].tasks)
        .toBe('importer_lite_tasks');
    } finally {
      await directYsql.close();
    }
  });

  it('staggering due discovery leaves catalog work eligible instead of creating a P2 sweep', async () => {
    await repo.enqueue({ taskType: 'DISCOVER', workKey: 'source:source-a', priority: 'P2', idempotencyKey: 'stagger-a' });
    await repo.enqueue({ taskType: 'DISCOVER', workKey: 'source:source-b', priority: 'P2', idempotencyKey: 'stagger-b' });
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:catalog-now', priority: 'P2', idempotencyKey: 'catalog-now' });
    expect(await repo.staggerReadyDiscoveries(60_000)).toBe(2);
    const claimed = await repo.claim('stagger-worker', 600);
    expect(claimed?.taskType).toBe('CATALOG');
  });

  it('enqueues DISCOVER directly without a ready-work projection transaction', async () => {
    const refreshReadyWork = vi.spyOn(repo as any, 'refreshReadyWork');
    await repo.enqueue({
      taskType: 'DISCOVER', workKey: 'source:source-a', priority: 'P2',
      idempotencyKey: 'lite:discover:source-a', payload: { source: 'source-a' }, skipReadyWorkRefresh: true,
    });

    expect(refreshReadyWork).not.toHaveBeenCalled();
    expect(await repo.claimDueDiscovery('discovery-worker', 30)).toMatchObject({
      taskType: 'DISCOVER', idempotencyKey: 'lite:discover:source-a',
    });
  });

  it('admits one low-priority no-network metadata task from explicit stored mapping fields', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000111','source-a','metadata-a',$1::uuid,
        'p1-a','P1 A',$2::jsonb,'SYNCED')`, [ids.p1a, JSON.stringify({
      alternativeTitles: ['P1 Alias'],
      tags: ['MILF'],
    })]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000119','source-b','metadata-b',$1::uuid,
        'p1-a','P1 A',$2::jsonb,'SYNCED')`, [ids.p1a, JSON.stringify({ alternativeTitles: ['P1 Alias B'] })]);

    // A pre-v2 replay can be queued behind normal P1.  It must not consume
    // the corrected parser's bounded seats or prevent the v2 task for the
    // same explicit mapping from becoming durable.
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      priority: 'P2', idempotencyKey: 'lite:metadata:source-a:metadata-a',
      payload: { source: 'source-a', sourceWorkId: 'metadata-a', metadataEnrichment: true } });

    expect(await repo.refillStoredMetadataEnrichment()).toBe(1);
    const firstV2 = await db.query(`SELECT task_type,priority,base_priority,payload
      FROM importer_lite_tasks WHERE idempotency_key LIKE 'lite:metadata:v2:%'`);
    expect(firstV2.rows).toHaveLength(1);
    expect(firstV2.rows[0]).toMatchObject({ task_type: 'CATALOG', priority: 3, base_priority: 3 });
    expect(firstV2.rows[0].payload).toMatchObject({ metadataEnrichment: true, metadataEnrichmentVersion: 2 });

    // The second low-priority seat belongs to a different source rather than
    // another mapping from the large source.
    expect(await repo.refillStoredMetadataEnrichment()).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:v2:source-a:metadata-a'`)).rows[0].count).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:v2:source-b:metadata-b'`)).rows[0].count).toBe(1);
    // Both bounded seats are durable; a future recovery turn cannot append a
    // hidden metadata queue.
    expect(await repo.refillStoredMetadataEnrichment()).toBe(0);

    // A normal chapter worker still admits P1 first. The bounded replay lane
    // also drains pre-v2 metadata tasks instead of leaving them orphaned.
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'pt-br|default|chapter|99|-', priority: 'P1', idempotencyKey: 'p1-remains-first' });
    expect((await repo.claim('chapter-worker', 120))?.idempotencyKey).toBe('p1-remains-first');
    expect(await repo.claimStoredMetadataReplay('metadata-replay', 45)).toMatchObject({
      taskType: 'CATALOG', priority: 'P2', payload: expect.objectContaining({
        metadataEnrichment: true, source: 'source-a', sourceWorkId: 'metadata-a',
      }),
    });
  });

  it('admits source-backed refetch tasks across mappings and keeps them at P2 behind P1', async () => {
    await db.query(`UPDATE works SET published=true WHERE id IN ($1,$2)`, [ids.p1a,ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000201','source-a','refetch-a',$1::uuid,'p1-a','P1 A','SYNCED'),
      ('00000000-0000-4000-8000-000000000202','source-b','refetch-b',$2::uuid,'p1-b','P1 B','SYNCED')`, [ids.p1a,ids.p1b]);
    await repo.enqueue({ taskType:'IMPORT_CHAPTER',workId:ids.p1a,workKey:`work:${ids.p1a}`,
      chapterKey:'pt-br|default|chapter|1|-',priority:'P1',idempotencyKey:'p1-before-metadata-refetch' });

    expect(await repo.refillSourceMetadataEnrichment(2)).toBe(2);
    const tasks = await db.query(`SELECT priority,payload->>'source' AS source,payload->>'metadataSourceRefresh' AS refresh
      FROM importer_lite_tasks WHERE payload ? 'metadataSourceRefresh' ORDER BY source`);
    expect(tasks.rows).toEqual([
      { priority:3,source:'source-a',refresh:'true' },
      { priority:3,source:'source-b',refresh:'true' },
    ]);
    expect((await repo.claim('priority-check',60))?.idempotencyKey).toBe('p1-before-metadata-refetch');
    expect(await repo.refillSourceMetadataEnrichment(2)).toBe(0);
  });

  it('does not let an expired historical metadata lease consume the protected P2 seat', async () => {
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id IN ($1,$2)`, [ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000212','source-a','expired-metadata',$1::uuid,'p1-a','P1 A','SYNCED'),
      ('00000000-0000-4000-8000-000000000213','source-b','fresh-metadata',$2::uuid,'p1-b','P1 B','SYNCED')`, [ids.p1a,ids.p1b]);
    await repo.enqueue({ taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P2',
      idempotencyKey:'lite:metadata:source:v3:source-a:expired-metadata',
      payload:{ source:'source-a',sourceWorkId:'expired-metadata',metadataSourceRefresh:true } });
    await db.query(`UPDATE importer_lite_tasks SET state='RUNNING',lease_owner='old-runtime',
      lease_until=now()-interval '1 second' WHERE idempotency_key='lite:metadata:source:v3:source-a:expired-metadata'`);

    expect(await repo.refillSourceMetadataEnrichment(1, 2)).toBe(1);
    expect((await db.query(`SELECT state FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:source:v3:source-b:fresh-metadata'`)).rows[0]).toEqual({ state:'READY' });
  });

  it('prioritizes a source proven to add aliases over a never-queried source', async () => {
    await db.query(`INSERT INTO importer_sources(id) VALUES('source-rich'),('source-new')`);
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id IN ($1,$2)`, [ids.p1a,ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000221','source-rich','aliasless-rich',$1::uuid,'p1-a','P1 A','SYNCED'),
      ('00000000-0000-4000-8000-000000000222','source-new','aliasless-new',$2::uuid,'p1-b','P1 B','SYNCED')`, [ids.p1a,ids.p1b]);
    await db.query(`INSERT INTO importer_lite_tasks
      (task_type,work_id,work_key,priority,base_priority,idempotency_key,state,payload,checkpoint)
      VALUES('CATALOG',$1::uuid,$2,3,3,'prior-alias-rich-source-history','DONE',
        '{"metadataSourceRefresh":true,"source":"source-rich","sourceWorkId":"prior-rich"}'::jsonb,
        '{"aliases":2,"aliasDelta":1}'::jsonb)`, [ids.p1a,`work:${ids.p1a}`]);

    expect(await repo.refillSourceMetadataEnrichment(1)).toBe(1);
    expect((await db.query(`SELECT payload->>'source' AS source FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:source:v3:source-rich:aliasless-rich'`)).rows).toEqual([{ source:'source-rich' }]);
  });

  it('prioritizes a work with no aliases over a rich-source mapping that can only complement an aliasful work', async () => {
    await db.query(`INSERT INTO importer_sources(id) VALUES('source-rich'),('source-new')`);
    await db.query(`UPDATE works SET published=true WHERE id IN ($1,$2)`, [ids.p1a,ids.p1b]);
    await db.query(`UPDATE works SET aliases=ARRAY['Already has an alias']::text[] WHERE id=$1`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000231','source-rich','complement-only',$1::uuid,'p1-a','P1 A','SYNCED'),
      ('00000000-0000-4000-8000-000000000232','source-new','needs-aliases',$2::uuid,'p1-b','P1 B','SYNCED')`, [ids.p1a,ids.p1b]);
    await db.query(`INSERT INTO importer_lite_tasks
      (task_type,work_id,work_key,priority,base_priority,idempotency_key,state,payload,checkpoint)
      VALUES('CATALOG',$1::uuid,$2,3,3,'prior-alias-rich-source-history-fallback','DONE',
        '{"metadataSourceRefresh":true,"source":"source-rich","sourceWorkId":"prior-rich"}'::jsonb,
        '{"aliases":2}'::jsonb)`, [ids.p1a,`work:${ids.p1a}`]);

    expect(await repo.refillSourceMetadataEnrichment(1)).toBe(1);
    expect((await db.query(`SELECT payload->>'source' AS source,payload->>'sourceWorkId' AS source_work_id
      FROM importer_lite_tasks WHERE state='READY' AND payload ? 'metadataSourceRefresh'`)).rows)
      .toEqual([{ source:'source-new',source_work_id:'needs-aliases' }]);
  });

  it('prioritizes a published work with no explicit tags after alias-first work', async () => {
    await db.query(`INSERT INTO importer_sources(id) VALUES('source-tag-rich'),('source-tagged')`);
    await db.query(`UPDATE works SET published=true,aliases=ARRAY['Existing alias']::text[] WHERE id IN ($1,$2)`,
      [ids.p1a,ids.p1b]);
    await db.query(`INSERT INTO work_tags(work_id,tag_id,system_generated)
      VALUES($1::uuid,'00000000-0000-4000-8000-00000000b001'::uuid,true)`, [ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000233','source-tag-rich','needs-tags',$1::uuid,'p1-a','P1 A','SYNCED'),
      ('00000000-0000-4000-8000-000000000234','source-tagged','already-tagged',$2::uuid,'p1-b','P1 B','SYNCED')`,
      [ids.p1a,ids.p1b]);
    await db.query(`INSERT INTO importer_lite_tasks
      (task_type,work_id,work_key,priority,base_priority,idempotency_key,state,payload,checkpoint)
      VALUES('CATALOG',$1::uuid,$2,3,3,'prior-tag-rich-source-history','DONE',
        '{"metadataSourceRefresh":true,"source":"source-tag-rich","sourceWorkId":"prior-rich"}'::jsonb,
        '{"tags":3,"tagDelta":2}'::jsonb)`, [ids.p1a,`work:${ids.p1a}`]);

    expect(await repo.refillSourceMetadataEnrichment(1)).toBe(1);
    expect((await db.query(`SELECT payload->>'source' AS source,payload->>'sourceWorkId' AS source_work_id
      FROM importer_lite_tasks WHERE state='READY' AND payload ? 'metadataSourceRefresh'`)).rows)
      .toEqual([{ source:'source-tag-rich',source_work_id:'needs-tags' }]);
  });

  it('does not prefer a provider whose fetched aliases were all duplicates over an untried source', async () => {
    await db.query(`INSERT INTO importer_sources(id) VALUES('source-duplicate'),('source-untried')`);
    await db.query(`UPDATE works SET published=true,aliases=ARRAY['Existing alias']::text[] WHERE id IN ($1,$2)`,[ids.p1a,ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000241','source-duplicate','same-aliases',$1::uuid,'p1-a','P1 A','SYNCED'),
      ('00000000-0000-4000-8000-000000000242','source-untried','new-possibility',$2::uuid,'p1-b','P1 B','SYNCED')`,[ids.p1a,ids.p1b]);
    await db.query(`INSERT INTO importer_lite_tasks
      (task_type,work_id,work_key,priority,base_priority,idempotency_key,state,payload,checkpoint)
      VALUES('CATALOG',$1::uuid,$2,3,3,'duplicate-alias-source-history','DONE',
        '{"metadataSourceRefresh":true,"source":"source-duplicate","sourceWorkId":"prior-duplicate"}'::jsonb,
        '{"aliases":3,"aliasDelta":0}'::jsonb)`,[ids.p1a,`work:${ids.p1a}`]);

    expect(await repo.refillSourceMetadataEnrichment(1)).toBe(1);
    expect((await db.query(`SELECT payload->>'source' AS source,payload->>'sourceWorkId' AS source_work_id
      FROM importer_lite_tasks WHERE state='READY' AND payload ? 'metadataSourceRefresh'`)).rows)
      .toEqual([{source:'source-untried',source_work_id:'new-possibility'}]);
  });

  it('resumes each metadata source after its last mapping and wraps without skipping earlier mappings', async () => {
    await db.query(`INSERT INTO importer_sources(id) VALUES('cursor-after'),('cursor-wrap')`);
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id IN ($1,$2,$3)`, [ids.p1a,ids.p1b,ids.p0]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000223','cursor-after','a-before',$1::uuid,'p1-a','P1 A','SYNCED'),
      ('00000000-0000-4000-8000-000000000224','cursor-after','c-after',$2::uuid,'p1-b','P1 B','SYNCED'),
      ('00000000-0000-4000-8000-000000000225','cursor-wrap','a-wrap',$3::uuid,'p0','P0','SYNCED')`, [ids.p1a,ids.p1b,ids.p0]);
    for (const [source,sourceWorkId,workId,key] of [
      ['cursor-after','b-cursor',ids.p1a,'cursor-after-history'],
      ['cursor-wrap','z-cursor',ids.p0,'cursor-wrap-history'],
    ] as const) {
      await db.query(`INSERT INTO importer_lite_tasks
        (task_type,work_id,work_key,priority,base_priority,idempotency_key,state,payload,checkpoint)
        VALUES('CATALOG',$1::uuid,$2,3,3,$3,'DONE',$4::jsonb,'{"aliases":0}'::jsonb)`, [
        workId,`work:${workId}`,key,JSON.stringify({metadataSourceRefresh:true,source,sourceWorkId}),
      ]);
    }

    expect(await repo.refillSourceMetadataEnrichment(2)).toBe(2);
    const admitted = await db.query(`SELECT payload->>'source' AS source,payload->>'sourceWorkId' AS source_work_id
      FROM importer_lite_tasks WHERE payload ? 'metadataSourceRefresh' AND state='READY' ORDER BY source`);
    expect(admitted.rows).toEqual([
      { source:'cursor-after',source_work_id:'c-after' },
      { source:'cursor-wrap',source_work_id:'a-wrap' },
    ]);
  });

  it('advances the durable source probe past an exhausted horizon before selecting a later pending mapping', async () => {
    const exhausted = Array.from({ length: 8 }, (_, index) => `a-stalled-${String(index + 1).padStart(2, '0')}`);
    await db.exec(`INSERT INTO importer_sources(id) VALUES ${[...exhausted, 'z-live'].map((source) => `('${source}')`).join(',')}`);
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id IN ($1,$2)`, [ids.p1a, ids.p1b]);
    for (const [index, source] of exhausted.entries()) {
      const mappingId = `00000000-0000-4000-8000-${String(301 + index).padStart(12, '0')}`;
      const sourceWorkId = `already-classified-${index + 1}`;
      await db.query(`INSERT INTO importer_work_mappings
        (id,source,source_work_id,work_id,source_slug,source_title,sync_status)
        VALUES($1::uuid,$2,$3,$4::uuid,'p1-a','P1 A','SYNCED')`, [mappingId,source,sourceWorkId,ids.p1a]);
      await db.query(`INSERT INTO importer_lite_metadata_enrichment
        (source,source_work_id,work_id,aliases_state,tags_state,version)
        VALUES($1,$2,$3::uuid,'ENRICHED','ENRICHED',3)`, [source,sourceWorkId,ids.p1a]);
    }
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status)
      VALUES('00000000-0000-4000-8000-000000000309','z-live','needs-refetch',$1::uuid,'p1-b','P1 B','SYNCED')`, [ids.p1b]);

    // A priority candidate may be selected ahead of the cursor scan. Either
    // path must reach the live source in a bounded number of turns.
    let admitted = 0;
    for (let turn = 0; turn < 5 && !admitted; turn++) admitted = await repo.refillSourceMetadataEnrichment(1);
    expect(admitted).toBe(1);
    expect((await db.query(`SELECT payload->>'source' AS source,payload->>'sourceWorkId' AS source_work_id
      FROM importer_lite_tasks WHERE state='READY' AND payload ? 'metadataSourceRefresh'`)).rows)
      .toEqual([{ source:'z-live',source_work_id:'needs-refetch' }]);
  });

  it('uses a two-source constrained refetch horizon without revisiting an exhausted source', async () => {
    await db.exec(`INSERT INTO importer_sources(id) VALUES('000-metadata-stalled'),('001-metadata-live')`);
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id IN ($1,$2)`, [ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000310','000-metadata-stalled','old-classified',$1::uuid,'p1-a','P1 A','SYNCED'),
      ('00000000-0000-4000-8000-000000000311','001-metadata-live','needs-refetch',$2::uuid,'p1-b','P1 B','SYNCED')`, [ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_lite_metadata_enrichment
      (source,source_work_id,work_id,aliases_state,tags_state,version)
      VALUES('000-metadata-stalled','old-classified',$1::uuid,'ENRICHED','ENRICHED',3)`, [ids.p1a]);

    expect(await repo.refillSourceMetadataEnrichment(1, 1)).toBe(0);
    expect((await db.query(`SELECT last_source FROM importer_lite_metadata_source_cursor
      WHERE cursor_name='source_refetch'`)).rows).toEqual([{ last_source:'000-metadata-stalled' }]);
    expect(await repo.refillSourceMetadataEnrichment(1, 1)).toBe(1);
    expect((await db.query(`SELECT payload->>'source' AS source FROM importer_lite_tasks
      WHERE state='READY' AND payload ? 'metadataSourceRefresh'`)).rows).toEqual([{ source:'001-metadata-live' }]);
  });

  it('rechecks a formerly NO_ALIASES active mapping after V4 completes', async () => {
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id=$1`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status)
      VALUES('00000000-0000-4000-8000-000000000314','source-a','v5-recheck',$1::uuid,'p1-a','P1 A','SYNCED')`, [ids.p1a]);
    await db.query(`INSERT INTO importer_lite_metadata_enrichment
      (source,source_work_id,work_id,aliases_state,tags_state,version)
      VALUES('source-a','v5-recheck',$1::uuid,'NO_ALIASES','NO_TAGS',3)`, [ids.p1a]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','complete')`);

    expect(await repo.refillSourceMetadataEnrichment(4, 1)).toBe(1);
    expect((await db.query(`SELECT idempotency_key,payload->>'metadataEnrichmentVersion' AS version
      FROM importer_lite_tasks WHERE state='READY' AND payload ? 'metadataSourceRefresh'`)).rows)
      .toEqual([{ idempotency_key:'lite:metadata:source:v5:source-a:v5-recheck', version:'5' }]);
  });

  it('starts durable global discovery after V4 and prioritizes an aliasless work without an active mapping', async () => {
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id IN ($1,$2)`, [ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status)
      VALUES('00000000-0000-4000-8000-000000000316','source-a','already-linked',$1::uuid,'p1-b','P1 B','SYNCED')`, [ids.p1b]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','complete')`);

    expect(await repo.refillCrossSourceMetadataDiscovery()).toBe(2);
    expect((await db.query(`SELECT work_id::text,payload->>'metadataCrossSourceDiscovery' AS coverage
      FROM importer_lite_tasks WHERE payload ? 'metadataCrossSourceDiscovery' ORDER BY work_id`)).rows)
      .toEqual([
        { work_id: ids.p1a, coverage:'true' },
        { work_id: ids.p1b, coverage:'true' },
      ]);
  });

  it('fills the popular linked discovery seat when an unmapped coverage pass is already active', async () => {
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id IN ($1,$2)`, [ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status)
      VALUES('00000000-0000-4000-8000-000000000317','source-a','linked-popular',$1::uuid,'p1-b','P1 B','SYNCED')`, [ids.p1b]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','complete')`);
    await repo.enqueue({ taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P2',
      idempotencyKey:`lite:metadata:cross-source:v7:${ids.p1a}`,
      payload:{metadataCrossSourceDiscovery:true,metadataEnrichmentVersion:7} });

    expect(await repo.refillCrossSourceMetadataDiscovery()).toBe(1);
    expect((await db.query(`SELECT work_id::text FROM importer_lite_tasks
      WHERE payload ? 'metadataCrossSourceDiscovery' ORDER BY work_id`)).rows)
      .toEqual([{ work_id:ids.p1a }, { work_id:ids.p1b }]);
  });

  it('keeps the popular discovery reservation after an unlinked pass finds its first mapping', async () => {
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id IN ($1,$2)`, [ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status)
      VALUES('00000000-0000-4000-8000-000000000319','source-a','popular-linked',$1::uuid,'p1-b','P1 B','SYNCED')`, [ids.p1b]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','complete')`);
    await repo.enqueue({ taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P2',
      idempotencyKey:`lite:metadata:cross-source:v7:${ids.p1a}`,
      payload:{metadataCrossSourceDiscovery:true,metadataEnrichmentVersion:7,discoveryLane:'unlinked'} });
    // The unlinked probe attached a provider after admission. It must retain
    // its original reservation rather than displacing the popular lane.
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status)
      VALUES('00000000-0000-4000-8000-000000000320','source-a','newly-linked',$1::uuid,'p1-a','P1 A','SYNCED')`, [ids.p1a]);

    expect(await repo.refillCrossSourceMetadataDiscovery()).toBe(1);
    expect((await db.query(`SELECT work_id::text,payload->>'discoveryLane' AS lane
      FROM importer_lite_tasks WHERE payload ? 'metadataCrossSourceDiscovery' ORDER BY work_id`)).rows)
      .toEqual([{ work_id:ids.p1a, lane:'unlinked' }, { work_id:ids.p1b, lane:'popular' }]);
  });

  it('claims cross-source coverage outside the generic P2 claim', async () => {
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id=$1`, [ids.p1a]);
    await repo.enqueue({ taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P2',
      idempotencyKey:`lite:metadata:cross-source:v7:${ids.p1a}`,
      payload:{metadataCrossSourceDiscovery:true,metadataEnrichmentVersion:7} });

    const task = await repo.claimCrossSourceMetadataDiscovery('cross-source-worker', 90);
    expect(task).toMatchObject({ workId:ids.p1a,state:'RUNNING',payload:{metadataCrossSourceDiscovery:true} });
  });

  it('reopens a bounded partial global-discovery pass only for deferred providers', async () => {
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id=$1`, [ids.p1a]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','complete')`);
    await db.query(`INSERT INTO importer_lite_tasks
      (id,task_type,work_id,work_key,priority,base_priority,idempotency_key,state,next_run_at,max_attempts,payload,checkpoint)
      VALUES('00000000-0000-4000-8000-000000000318','CATALOG',$1::uuid,$2,3,3,$3,'DONE',now()-interval '1 minute',4,$4::jsonb,$5::jsonb)`, [
      ids.p1a, `work:${ids.p1a}`, `lite:metadata:cross-source:v7:${ids.p1a}`,
      JSON.stringify({ metadataCrossSourceDiscovery:true,metadataEnrichmentVersion:7 }),
      JSON.stringify({ coverage:'CROSS_SOURCE_PARTIAL_EXTERNAL_DEFERRED',checkedSources:['source-a','source-b'],deferredSources:['source-b'],deferredRounds:0 }),
    ]);

    expect(await repo.refillCrossSourceMetadataDiscovery()).toBe(1);
    expect((await db.query(`SELECT state,checkpoint->'checkedSources' AS checked,checkpoint->>'deferredRounds' AS rounds
      FROM importer_lite_tasks WHERE id='00000000-0000-4000-8000-000000000318'`)).rows)
      .toEqual([{ state:'READY',checked:['source-a'],rounds:'1' }]);
  });

  it('attaches only detail-confirmed cross-source metadata and persists its alias delta', async () => {
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[],title='The Specific P1 A Work',slug='the-specific-p1-a-work' WHERE id=$1`, [ids.p1a]);
    await repo.enqueue({ taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P2',
      idempotencyKey:`lite:metadata:cross-source:v7:${ids.p1a}`,
      payload:{metadataCrossSourceDiscovery:true,metadataEnrichmentVersion:7} });
    const task = await repo.claim('cross-source-worker', 120);
    const adapter = { id:'source-a',baseUrl:'https://source-a.example',
      // The search result is a different primary title.  The exact identity
      // proof appears only in the source's explicit detail alias.
      searchWorks:async () => [{ sourceWorkId:'cross-source-a',title:'O Trabalho P1 Específico',slug:'o-trabalho-p1-especifico' }],
      fetchWorkDetails:async () => ({ sourceWorkId:'cross-source-a',title:'O Trabalho P1 Específico',slug:'o-trabalho-p1-especifico',
        alternativeTitles:['The Specific P1 A Work','P1 A English'],genres:[],raw:{} }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, { sql: (text: string, values?: unknown[]) => db.query(text, values) },
      { get: (source: string) => source === 'source-a' ? adapter : undefined } as unknown as SourceRegistry,
      { upload:async ()=>'unused',healthCheck:async()=>true,getProviderKey:()=>'telegram' },
      { IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY:1,SOURCE_OPERATION_TIMEOUT_MS:2_000 } as any,
    );
    (runtime as any).dedupe = {
      attachVerifiedMapping: async (candidate: any, workId: string, source: string) => {
        await db.query(`INSERT INTO importer_work_mappings
          (id,source,source_work_id,work_id,source_slug,source_title,sync_status,metadata)
          VALUES('00000000-0000-4000-8000-000000000317',$1,$2,$3::uuid,$4,$5,'SYNCED',$6::jsonb)`,
        [source,candidate.sourceWorkId,workId,candidate.slug,candidate.title,JSON.stringify(candidate.rawMetadata)]);
        await db.query(`UPDATE works SET aliases=ARRAY['P1 A English']::text[] WHERE id=$1::uuid`, [workId]);
        return { workId, status:'EXISTING_MAPPING', metadataDelta:{ aliasesAdded:1, tagsAdded:0 } };
      },
    };
    await (runtime as any).catalog(task,'cross-source-worker');

    expect((await db.query(`SELECT aliases FROM works WHERE id=$1`, [ids.p1a])).rows[0].aliases).toContain('P1 A English');
    expect((await db.query(`SELECT work_id::text FROM importer_work_mappings
      WHERE source='source-a' AND source_work_id='cross-source-a'`)).rows).toEqual([{ work_id:ids.p1a }]);
    expect((await db.query(`SELECT work_id::text,monitoring,state FROM importer_lite_tasks
      WHERE idempotency_key='lite:p0-monitor:source-a:cross-source-a'`)).rows)
      .toEqual([{ work_id: ids.p1a, monitoring: true, state: 'READY' }]);
  });

  it('does not attach a similarly named work without independent identity evidence', async () => {
    await db.query(`UPDATE works SET published=true,title='The Return of the Iron Blooded Sword Hound',
      slug='the-return-of-the-iron-blooded-sword-hound',aliases='{}'::text[] WHERE id=$1`, [ids.p1a]);
    await repo.enqueue({taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P2',
      idempotencyKey:`lite:metadata:cross-source:v7:${ids.p1a}`,
      payload:{metadataCrossSourceDiscovery:true,metadataEnrichmentVersion:7}});
    const task=await repo.claim('cross-source-worker',120);
    const adapter={id:'source-a',baseUrl:'https://source-a.example',
      searchWorks:async()=>[{sourceWorkId:'similar',title:'The Return of the Iron Blooded Sword Hounds',slug:'the-return-of-the-iron-blooded-sword-hounds'}],
      fetchWorkDetails:async()=>({sourceWorkId:'similar',title:'The Return of the Iron Blooded Sword Hounds',slug:'the-return-of-the-iron-blooded-sword-hounds',genres:[],raw:{}})};
    const runtime=new ImporterLiteRuntime(poolFor(db) as Pool,{sql:(text:string,values?:unknown[])=>db.query(text,values)},
      {get:(source:string)=>source==='source-a'?adapter:undefined} as unknown as SourceRegistry,
      {upload:async()=>'unused',healthCheck:async()=>true,getProviderKey:()=> 'telegram'},
      {IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,BATCH_PAGE_DOWNLOAD_CONCURRENCY:1,SOURCE_OPERATION_TIMEOUT_MS:2000} as any);
    const attachVerifiedMapping=vi.fn();
    (runtime as any).dedupe={attachVerifiedMapping};
    await (runtime as any).catalog(task,'cross-source-worker');
    expect(attachVerifiedMapping).not.toHaveBeenCalled();
    expect((await db.query(`SELECT count(*)::int n FROM importer_lite_tasks WHERE idempotency_key='lite:p0-monitor:source-a:similar'`)).rows[0].n).toBe(0);
  });

  it('persists aliases found by a V5 external recheck', async () => {
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id=$1`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status)
      VALUES('00000000-0000-4000-8000-000000000315','source-a','v5-persist',$1::uuid,'p1-a','P1 A','SYNCED')`, [ids.p1a]);
    await repo.enqueue({ taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P2',
      idempotencyKey:'lite:metadata:source:v5:source-a:v5-persist',
      payload:{source:'source-a',sourceWorkId:'v5-persist',metadataSourceRefresh:true,metadataEnrichmentVersion:5} });
    const task = await repo.claimSourceMetadataRefresh('metadata-v5-worker', 60);
    const adapter = { id:'source-a',baseUrl:'https://source-a.example',fetchWorkDetails:async () => ({
      sourceWorkId:'v5-persist',title:'P1 A',slug:'p1-a',alternativeTitles:['P1 V5 Alias'],genres:[],raw:{},
    }) };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, { sql: (text: string, values?: unknown[]) => db.query(text, values) },
      { get: () => adapter } as unknown as SourceRegistry,
      { upload:async ()=>'unused',healthCheck:async()=>true,getProviderKey:()=>'telegram' },
      { IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY:1,SOURCE_OPERATION_TIMEOUT_MS:2_000 } as any,
    );
    await (runtime as any).catalog(task,'metadata-v5-worker');

    expect((await db.query(`SELECT aliases FROM works WHERE id=$1`, [ids.p1a])).rows[0].aliases).toContain('P1 V5 Alias');
    expect((await db.query(`SELECT aliases_state,version FROM importer_lite_metadata_enrichment
      WHERE source='source-a' AND source_work_id='v5-persist'`)).rows[0])
      .toMatchObject({ aliases_state:'ENRICHED',version:5 });
  });

  it('briefly reuses a productive source without losing the durable cursor frontier', async () => {
    await db.query(`INSERT INTO importer_sources(id) VALUES('sticky-metadata-source')`);
    await db.query(`UPDATE works SET published=true,aliases='{}'::text[] WHERE id IN ($1,$2)`, [ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000312','sticky-metadata-source','first',$1::uuid,'p1-a','P1 A','SYNCED'),
      ('00000000-0000-4000-8000-000000000313','sticky-metadata-source','second',$2::uuid,'p1-b','P1 B','SYNCED')`,
    [ids.p1a, ids.p1b]);

    expect(await repo.refillSourceMetadataEnrichment(1, 1, 'sticky-metadata-source')).toBe(1);
    expect((await db.query(`SELECT payload->>'sourceWorkId' AS source_work_id FROM importer_lite_tasks
      WHERE state='READY' AND payload ? 'metadataSourceRefresh'`)).rows).toEqual([{ source_work_id:'first' }]);
    await db.query(`UPDATE importer_lite_tasks SET state='DONE',lease_owner=NULL,lease_until=NULL
      WHERE state='READY' AND payload ? 'metadataSourceRefresh'`);

    expect(await repo.refillSourceMetadataEnrichment(1, 1, 'sticky-metadata-source')).toBe(1);
    expect((await db.query(`SELECT payload->>'sourceWorkId' AS source_work_id FROM importer_lite_tasks
      WHERE state='READY' AND payload ? 'metadataSourceRefresh'`)).rows).toEqual([{ source_work_id:'second' }]);
    expect((await db.query(`SELECT last_source FROM importer_lite_metadata_source_cursor
      WHERE cursor_name='source_refetch'`)).rows[0].last_source).toBeTruthy();
  });

  it('claims source metadata from its dedicated lane while chapter claims remain available', async () => {
    await repo.enqueue({ taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P2',
      idempotencyKey:'source-metadata-dedicated',payload:{source:'source-a',sourceWorkId:'old-work',metadataSourceRefresh:true} });
    await repo.enqueue({ taskType:'IMPORT_CHAPTER',workId:ids.p1b,workKey:`work:${ids.p1b}`,chapterKey:'chapter-p1',priority:'P1',
      idempotencyKey:'chapter-p1-remains-first' });

    const sourceTask = await repo.claimSourceMetadataRefresh('metadata-lane',90);
    expect(sourceTask).toMatchObject({ idempotencyKey:'source-metadata-dedicated',priority:'P2',state:'RUNNING' });
    expect((await repo.claim('chapter-worker',90))?.idempotencyKey).toBe('chapter-p1-remains-first');
  });

  it('reclaims an expired source-metadata lease after a rolling restart', async () => {
    await repo.enqueue({ taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P2',
      idempotencyKey:'source-metadata-expired-lease',payload:{source:'source-a',sourceWorkId:'stale-work',metadataSourceRefresh:true} });
    await db.query(`UPDATE importer_lite_tasks
      SET state='RUNNING',lease_owner='previous-runtime',lease_until=now()-interval '1 minute'
      WHERE idempotency_key='source-metadata-expired-lease'`);

    const task = await repo.claimSourceMetadataRefresh('replacement-runtime',90);
    expect(task).toMatchObject({ idempotencyKey:'source-metadata-expired-lease',state:'RUNNING',leaseOwner:'replacement-runtime' });
  });

  it('uses the isolated P2 database seat for the historical source claim', async () => {
    await repo.enqueue({ taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P2',
      idempotencyKey:'source-metadata-isolated-seat',payload:{source:'source-a',sourceWorkId:'isolated-work',metadataSourceRefresh:true} });
    const mainPool = poolFor(db) as any;
    const isolatedPool = poolFor(db) as any;
    mainPool.query = vi.fn(mainPool.query);
    isolatedPool.query = vi.fn(isolatedPool.query);
    const isolatedRepository = new LiteRepository(mainPool, isolatedPool);

    await expect(isolatedRepository.claimSourceMetadataRefresh('metadata-isolated',90))
      .resolves.toMatchObject({ idempotencyKey:'source-metadata-isolated-seat', state:'RUNNING' });

    expect(isolatedPool.query).toHaveBeenCalledWith(
      expect.stringContaining('metadataSourceRefresh'), expect.any(Array),
    );
    expect(mainPool.query).not.toHaveBeenCalledWith(
      expect.stringContaining('metadataSourceRefresh'), expect.any(Array),
    );
  });

  it('claims one unmapped onboarding catalog task without stealing a P1 chapter worker claim', async () => {
    await repo.enqueue({ taskType:'CATALOG', workKey:'source:source-a:fresh-work', priority:'P2',
      idempotencyKey:'fresh-work-catalog', payload:{ source:'source-a', sourceWorkId:'fresh-work' } });
    await repo.enqueue({ taskType:'IMPORT_CHAPTER',workId:ids.p1a,workKey:`work:${ids.p1a}`,
      chapterKey:'chapter-p1',priority:'P1',idempotencyKey:'chapter-p1-remains-first' });

    const onboarding = await repo.claimNewWorkCatalog('new-work-catalog',90);
    expect(onboarding).toMatchObject({ idempotencyKey:'fresh-work-catalog',priority:'P2',state:'RUNNING',workId:null });
    expect((await repo.claim('chapter-worker',90))?.idempotencyKey).toBe('chapter-p1-remains-first');
  });

  it('renews a live onboarding lease without allowing a stale owner to revive it', async () => {
    await repo.enqueue({ taskType:'CATALOG', workKey:'source:source-a:long-first-pass', priority:'P2',
      idempotencyKey:'long-first-pass-catalog', payload:{ source:'source-a', sourceWorkId:'long-first-pass' } });
    const onboarding = await repo.claimNewWorkCatalog('new-work-catalog',30);
    expect(onboarding).not.toBeNull();

    expect(await repo.renewLease(onboarding!, 'new-work-catalog',90)).toBe(true);
    expect((await db.query(`SELECT lease_until > now()+interval '80 seconds' AS renewed
      FROM importer_lite_tasks WHERE id=$1`, [onboarding!.id])).rows[0]).toEqual({ renewed:true });
    expect(await repo.renewLease(onboarding!, 'stale-worker',90)).toBe(false);

    await db.query(`UPDATE importer_lite_tasks SET lease_until=now()-interval '1 second' WHERE id=$1`, [onboarding!.id]);
    expect(await repo.renewLease(onboarding!, 'new-work-catalog',90)).toBe(false);
  });

  it('skips a cooled legacy source so a healthy new work is not stranded in the serial onboarding lane', async () => {
    await repo.enqueue({ taskType:'CATALOG', workKey:'source:source-a:old-cooled', priority:'P2',
      idempotencyKey:'old-cooled-catalog', payload:{ source:'source-a', sourceWorkId:'old-cooled' } });
    await repo.enqueue({ taskType:'CATALOG', workKey:'source:source-b:healthy-new', priority:'P2',
      idempotencyKey:'healthy-new-catalog', payload:{ source:'source-b', sourceWorkId:'healthy-new' } });
    await db.query(`UPDATE importer_sources SET status='COOLDOWN',cooldown_until=now()+interval '30 minutes' WHERE id='source-a'`);

    const onboarding = await repo.claimNewWorkCatalog('new-work-catalog',90);
    expect(onboarding).toMatchObject({ idempotencyKey:'healthy-new-catalog',workId:null,state:'RUNNING' });
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE idempotency_key='old-cooled-catalog'`)).rows[0])
      .toMatchObject({ state:'READY' });
  });

  it('keeps the serial onboarding lane live through a transient chapter pool waiter', async () => {
    await repo.enqueue({ taskType:'CATALOG', workKey:'source:source-a:pool-wait-fresh', priority:'P2',
      idempotencyKey:'pool-wait-fresh-catalog', payload:{ source:'source-a', sourceWorkId:'pool-wait-fresh' } });
    const pool = Object.assign(poolFor(db), { waitingCount: 2 });
    const runtime = new ImporterLiteRuntime(
      pool as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload:async ()=>'unused',healthCheck:async()=>true,getProviderKey:()=>'telegram' },
      { IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY:1 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    const catalog = vi.fn(async () => undefined);
    (runtime as any).catalog = catalog;

    await (runtime as any).runNewWorkCatalog();

    expect(catalog).toHaveBeenCalledTimes(1);
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE idempotency_key='pool-wait-fresh-catalog'`)).rows[0])
      .toMatchObject({ state:'RUNNING' });
  });

  it('admits bounded alternating P2 discovery and onboarding turns under sustained metadata pressure', async () => {
    const runtime=new ImporterLiteRuntime(poolFor(db) as Pool,{}, {get:()=>undefined} as unknown as SourceRegistry,
      {upload:async()=>'unused',healthCheck:async()=>true,getProviderKey:()=> 'telegram'},
      {IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,BATCH_PAGE_DOWNLOAD_CONCURRENCY:1} as any);
    (runtime as any).isRuntimeLeader=true;
    (runtime as any).hasSustainedSourceMetadataPoolPressure=()=>true;
    (runtime as any).shouldPauseLowPriorityMetadata=()=>false;
    const discovery={id:'p2-discovery'};
    const catalog={id:'p2-catalog'};
    const claimDueDiscovery=vi.fn().mockResolvedValue(discovery);
    const claimNewWorkCatalog=vi.fn().mockResolvedValue(catalog);
    (runtime as any).queue={claimDueDiscovery,claimNewWorkCatalog};
    const discover=vi.fn().mockResolvedValue(undefined);
    const handleCatalog=vi.fn().mockResolvedValue(undefined);
    (runtime as any).discover=discover;
    (runtime as any).withCatalogLeaseHeartbeat=async (_task:any,_worker:string,fn:()=>Promise<void>)=>fn();
    (runtime as any).catalog=handleCatalog;
    await (runtime as any).runNewWorkCatalog();
    await (runtime as any).runNewWorkCatalog();
    expect(discover).toHaveBeenCalledTimes(1);
    expect(claimNewWorkCatalog).not.toHaveBeenCalled();
    (runtime as any).lastNewWorkPressureTurnAt=0;
    await (runtime as any).runNewWorkCatalog();
    expect(handleCatalog).toHaveBeenCalledTimes(1);
  });

  it('retains exactly one P2 metadata seat while chapter demand is admitted or ready, and fully opens P2 once idle', () => {
    const pool = Object.assign(poolFor(db), { waitingCount: 0 });
    const runtime = new ImporterLiteRuntime(
      pool as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload:async ()=>'unused',healthCheck:async()=>true,getProviderKey:()=>'telegram' },
      { IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY:1,
        BUFFERED_PAGE_CONCURRENCY:1, BATCH_PAGE_DOWNLOAD_CONCURRENCY:1,
        LITE_CHAPTER_WORKERS:1, MAX_CONCURRENT_CHAPTERS:1, TESTED_CONCURRENCY_CEILING:1 } as any,
    );
    const chapterGate = (runtime as any).chapterAdmissionGate;
    expect(chapterGate.tryAcquire()).toBe(true);

    pool.waitingCount = 1;
    expect((runtime as any).tryAcquireBackgroundMetadataBudget()).toBeNull();
    pool.waitingCount = 0;
    const first = (runtime as any).tryAcquireBackgroundMetadataBudget();
    expect(first).toEqual(expect.any(Function));
    expect((runtime as any).tryAcquireBackgroundMetadataBudget()).toBeNull();
    first();

    const replacement = (runtime as any).tryAcquireBackgroundMetadataBudget();
    expect(replacement).toEqual(expect.any(Function));
    replacement();
    chapterGate.release();

    // The durable fair-source sample closes the startup window before the
    // first worker has acquired its chapter admission permit.
    (runtime as any).executor.setFairSourceDemand(['source-a']);
    const readyDemand = (runtime as any).tryAcquireBackgroundMetadataBudget();
    expect(readyDemand).toEqual(expect.any(Function));
    expect((runtime as any).tryAcquireBackgroundMetadataBudget()).toBeNull();
    readyDemand();
    (runtime as any).executor.setFairSourceDemand([]);

    // An idle chapter budget never throttles the P2 recovery lanes.
    expect((runtime as any).tryAcquireBackgroundMetadataBudget()).toEqual(expect.any(Function));
    expect((runtime as any).tryAcquireBackgroundMetadataBudget()).toEqual(expect.any(Function));
  });

  it('lets the bounded P2 discovery lane progress while preserving queued P1 work', async () => {
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000901','source-a','already-mapped',$1::uuid,'p1-a','P1 A','SYNCED')`, [ids.p1a]);
    await repo.enqueue({ taskType:'IMPORT_CHAPTER', workId:ids.p1a, workKey:`work:${ids.p1a}`,
      chapterKey:'chapter-p1-busy', priority:'P1', idempotencyKey:'chapter-p1-busy' });
    await repo.enqueue({ taskType:'DISCOVER', workKey:'source:source-a', priority:'P2',
      idempotencyKey:'lite:discover:source-a', payload:{ source:'source-a' } });

    const adapter = {
      id: 'source-a',
      fetchUpdatedWorks: vi.fn(async () => ({
        works: [
          { sourceWorkId: 'already-mapped', title: 'P1 A', slug: 'p1-a' },
          { sourceWorkId: 'brand-new-work', title: 'Brand New Work', slug: 'brand-new-work' },
        ],
        nextCursor: null,
      })),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: (id: string) => id === 'source-a' ? adapter : undefined } as unknown as SourceRegistry,
      { upload:async ()=>'unused', healthCheck:async()=>true, getProviderKey:()=>'telegram' },
      { IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY:1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY:1, QUEUE_LEASE_DURATION_SECONDS:60, SOURCE_OPERATION_TIMEOUT_MS:5_000 } as any,
    );
    (runtime as any).isRuntimeLeader = true;

    // P2 has a dedicated bounded lane and must not consume the queued P1 task.
    await (runtime as any).runNewWorkCatalog();
    expect(adapter.fetchUpdatedWorks).toHaveBeenCalledTimes(1);
    expect((await db.query(`SELECT state,priority FROM importer_lite_tasks WHERE idempotency_key='chapter-p1-busy'`)).rows[0])
      .toEqual({state:'READY',priority:2});

    const catalogTasks = await db.query<{ idempotency_key: string; work_id: string | null }>(`
      SELECT idempotency_key, work_id::text AS work_id
      FROM importer_lite_tasks
      WHERE task_type='CATALOG'
      ORDER BY idempotency_key`);
    expect(catalogTasks.rows).toEqual([
      { idempotency_key: 'lite:catalog:source-a:already-mapped', work_id: ids.p1a },
      { idempotency_key: 'lite:catalog:source-a:brand-new-work', work_id: null },
    ]);

    const nextOnboarding = await repo.claimNewWorkCatalog('new-work-catalog', 90);
    expect(nextOnboarding?.idempotencyKey).toBe('lite:catalog:source-a:brand-new-work');
  });

  it('keeps the new-work catalog lease alive while its bounded metadata pass is still running', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload:async ()=>'unused',healthCheck:async()=>true,getProviderKey:()=>'telegram' },
      { IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY:1 } as any,
    );
    const renewLease = vi.spyOn((runtime as any).queue, 'renewLease').mockResolvedValue(true);
    let release!: () => void;
    const catalogPass = new Promise<void>((resolve) => { release = resolve; });
    const task = { id:'00000000-0000-4000-8000-000000000011' } as any;

    vi.useFakeTimers();
    try {
      const running = (runtime as any).withCatalogLeaseHeartbeat(task, 'new-work-catalog', () => catalogPass);
      await vi.advanceTimersByTimeAsync(30_000);
      expect(renewLease).toHaveBeenCalledWith(task, 'new-work-catalog', 90);
      release();
      await running;
    } finally {
      vi.useRealTimers();
    }
  });

  it('counts alias and explicit-tag activation once per work', async () => {
    await db.query(`UPDATE works SET aliases=ARRAY['Título nativo'] WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`UPDATE works SET aliases=ARRAY['Título nativo','Romanized title'] WHERE id=$1::uuid`, [ids.p1a]);
    // The importer submits associations in batches. PostgreSQL/YSQL executes
    // each row trigger in order; separate statements keep this PGlite
    // contract test independent from its multi-row trigger visibility.
    await db.query(`INSERT INTO work_tags(work_id,tag_id,system_generated)
      VALUES($1::uuid,'00000000-0000-4000-8000-00000000a001'::uuid,true)`, [ids.p1a]);
    await db.query(`INSERT INTO work_tags(work_id,tag_id,system_generated)
      VALUES($1::uuid,'00000000-0000-4000-8000-00000000a002'::uuid,true)`, [ids.p1a]);

    await expect(repo.metadataProductSnapshot()).resolves.toEqual({
      aliasesActivatedWorks: 1,
      tagsActivatedWorks: 1,
    });
  });

  it('keeps the historical metadata lane live through a lone pool waiter with one serial YSQL seat', async () => {
    const memorySpy = vi.spyOn(process, 'memoryUsage').mockReturnValue({ rss: 128 * 1024 * 1024 } as NodeJS.MemoryUsage);
    const lanePool = poolFor(db) as any;
    lanePool.waitingCount = 1;
    const runtime = new ImporterLiteRuntime(
      lanePool, {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'metadata-lane-test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        LITE_CHAPTER_WORKERS: 1, MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );
    const claim = vi.fn(async () => null);
    (runtime as any).queue = {
      claimSourceMetadataRefresh: claim,
      refillSourceMetadataEnrichment: vi.fn().mockResolvedValue(0),
    };
    (runtime as any).isRuntimeLeader = true;
    // One waiter is normal during chapter receipt persistence. It must not
    // leave the dedicated historical lane permanently unable even to claim.
    await (runtime as any).runSourceMetadataRefresh();
    expect(claim).toHaveBeenCalledTimes(1);

    lanePool.waitingCount = 0;
    let active = 0;
    let maximum = 0;
    let releaseFetches!: () => void;
    let notifyStarted!: () => void;
    const started = new Promise<void>((resolve) => { notifyStarted = resolve; });
    const holdFetches = new Promise<void>((resolve) => { releaseFetches = resolve; });
    let remainingClaims = 1;
    claim.mockImplementation(async () => remainingClaims-- > 0 ? ({ id: `metadata-${remainingClaims}` } as any) : null);
    (runtime as any).catalog = async () => {
      active++;
      maximum = Math.max(maximum, active);
      if (active === 1) notifyStarted();
      await holdFetches;
      active--;
    };
    const idleRun = (runtime as any).runSourceMetadataRefresh();
    await started;
    expect(maximum).toBe(1);
    expect((runtime as any).sourceMetadataGate.capacity).toBe(4);
    expect((runtime as any).sourceMetadataPreparationGate.capacity).toBe(1);
    expect((runtime as any).sourceMetadataPersistenceGate.capacity).toBe(1);
    releaseFetches();
    await idleRun;
    memorySpy.mockRestore();
  });

  it('defers exceptional unknown-upload recovery until chapter admission is quiet', async () => {
    const lanePool = Object.assign(poolFor(db), { waitingCount: 0 });
    const runtime = new ImporterLiteRuntime(
      lanePool as Pool, {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        LITE_CHAPTER_WORKERS: 1, MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );
    const reconcile = vi.fn().mockResolvedValue({});
    const newestUnknownUploadAt = vi.fn().mockResolvedValue(new Date('2026-10-25T00:00:00.000Z'));
    (runtime as any).queue = { reconcile, newestUnknownUploadAt, recoverDuplicateCanonicalCatalogFailures: vi.fn().mockResolvedValue(0) };
    for (const method of ['probeDueSourceRecovery', 'refreshSourceFairness', 'refillHistoricalP1Catalog',
      'refillManifestProvenP1Imports', 'refillStoredMetadataEnrichment', 'runStoredMetadataReplay']) {
      vi.spyOn(runtime as any, method).mockResolvedValue(undefined);
    }

    const admission = (runtime as any).chapterAdmissionGate;
    expect(admission.tryAcquire()).toBe(true);
    await (runtime as any).runRecoveryTurn();
    expect(reconcile).toHaveBeenLastCalledWith(50, expect.objectContaining({
      recoverUploadOutcomes: false,
      recoverSourceCooldowns: true,
    }));

    // Do not repeatedly run the multi-row cooldown scan during this same
    // chapter wave merely because the pool had a transient idle seat.
    await (runtime as any).runRecoveryTurn();
    expect(reconcile).toHaveBeenLastCalledWith(50, expect.objectContaining({
      recoverUploadOutcomes: false,
      recoverSourceCooldowns: false,
    }));

    // The pending upload proof and cooldown pass both resume promptly once
    // quiet; the latter retains its short idle cadence for replenishment.
    admission.release();
    (runtime as any).lastSourceCooldownRecoveryAt = Date.now() - 30_000;
    await (runtime as any).runRecoveryTurn();
    expect(reconcile).toHaveBeenLastCalledWith(50, expect.objectContaining({
      recoverUploadOutcomes: true,
      recoverSourceCooldowns: true,
    }));

    // An empty proof pass records the durable watermark. Repeating the
    // recovery interval without a newer UNKNOWN must keep the cold join out
    // of the chapter pool while all ordinary recovery remains live.
    (runtime as any).lastUploadOutcomeRecoveryAt = Date.now() - 5 * 60_000;
    (runtime as any).lastSourceCooldownRecoveryAt = Date.now() - 30_000;
    await (runtime as any).runRecoveryTurn();
    expect(reconcile).toHaveBeenLastCalledWith(50, expect.objectContaining({
      recoverUploadOutcomes: false,
      recoverSourceCooldowns: true,
    }));

    // A sustained wave still receives one bounded source-cooldown pass at
    // the max gap, so this efficiency guard cannot starve deferred sources.
    expect(admission.tryAcquire()).toBe(true);
    (runtime as any).lastSourceCooldownRecoveryAt = Date.now() - 2 * 60_000;
    await (runtime as any).runRecoveryTurn();
    expect(reconcile).toHaveBeenLastCalledWith(50, expect.objectContaining({
      recoverUploadOutcomes: false,
      recoverSourceCooldowns: true,
    }));
    admission.release();
  });

  it('prefetches one durable successor without widening the historical fetch lane under shared-pool pressure', async () => {
    const memorySpy = vi.spyOn(process, 'memoryUsage').mockReturnValue({ rss: 128 * 1024 * 1024 } as NodeJS.MemoryUsage);
    const lanePool = poolFor(db) as any;
    lanePool.waitingCount = 1;
    const runtime = new ImporterLiteRuntime(
      lanePool, {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'metadata-refill-pressure-test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        LITE_CHAPTER_WORKERS: 1, MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );
    const refill = vi.fn().mockResolvedValueOnce(4).mockResolvedValue(2);
    (runtime as any).queue = {
      refillSourceMetadataEnrichment: refill,
      claimSourceMetadataRefresh: vi.fn().mockResolvedValue(null),
    };
    (runtime as any).isRuntimeLeader = true;
    vi.spyOn(runtime as any, 'runSourceMetadataRefresh').mockResolvedValue(false);

    await (runtime as any).refillSourceMetadataEnrichment(true);
    expect(refill).toHaveBeenLastCalledWith(4, undefined);

    refill.mockClear();
    lanePool.waitingCount = 2;
    // Constrain only after the configured sustained-pressure window.
    (runtime as any).sourceMetadataPoolWaitStartedAt = Date.now() - 16_000;
    await (runtime as any).refillSourceMetadataEnrichment(true);
    expect(refill).toHaveBeenLastCalledWith(2, 2);
    memorySpy.mockRestore();
  });

  it('does not constrain source metadata prefetch for unrelated main-pool waiters', async () => {
    const chapterPool = Object.assign(poolFor(db), { waitingCount: 4 }) as Pool;
    const metadataPool = Object.assign(poolFor(db), { waitingCount: 0 }) as Pool;
    const runtime = new ImporterLiteRuntime(
      chapterPool, {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'isolated-source-metadata-test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
      new HostRateLimiter(2), metadataPool,
    );
    const refill = vi.fn().mockResolvedValue(1);
    (runtime as any).queue = {
      refillSourceMetadataEnrichment: refill,
      claimSourceMetadataRefresh: vi.fn().mockResolvedValue(null),
    };
    (runtime as any).isRuntimeLeader = true;

    await (runtime as any).refillSourceMetadataEnrichment(true);
    expect(refill).toHaveBeenLastCalledWith(4, undefined);
  });

  it('does not queue overlapping cross-source refills on the isolated metadata connection', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'cross-source-refill-gate-test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    let resolveRefill!: (value: number) => void;
    const refill = vi.fn(() => new Promise<number>((resolve) => { resolveRefill = resolve; }));
    (runtime as any).queue = { refillCrossSourceMetadataDiscovery: refill };
    (runtime as any).isRuntimeLeader = true;

    const first = (runtime as any).refillCrossSourceMetadataDiscovery();
    await Promise.resolve();
    // Simulate the next timer turn after the regular 15-second fence has
    // elapsed while the first YSQL transaction is still in progress.
    (runtime as any).lastCrossSourceDiscoveryRefillAt = 0;
    await expect((runtime as any).refillCrossSourceMetadataDiscovery()).resolves.toBe(0);
    expect(refill).toHaveBeenCalledTimes(1);

    resolveRefill(2);
    await expect(first).resolves.toBe(2);
    expect((runtime as any).crossSourceMetadataRefillGate.active).toBe(0);
  });

  it('advances only a bounded number of empty historical source horizons', async () => {
    const memorySpy = vi.spyOn(process, 'memoryUsage').mockReturnValue({ rss: 128 * 1024 * 1024 } as NodeJS.MemoryUsage);
    const runtime = new ImporterLiteRuntime(
      poolFor(db), {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'metadata-empty-horizon-test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        LITE_CHAPTER_WORKERS: 1, MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );
    const refill = vi.fn().mockResolvedValue(0);
    (runtime as any).queue = {
      refillSourceMetadataEnrichment: refill,
      claimSourceMetadataRefresh: vi.fn().mockResolvedValue(null),
    };
    (runtime as any).isRuntimeLeader = true;

    await (runtime as any).refillSourceMetadataEnrichment(true);
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(refill).toHaveBeenCalledTimes(4);
    expect(refill).toHaveBeenCalledWith(4, undefined);
    memorySpy.mockRestore();
  });

  it('advances a bounded sparse horizon to fill remaining V5 seats', async () => {
    const memorySpy = vi.spyOn(process, 'memoryUsage').mockReturnValue({ rss: 128 * 1024 * 1024 } as NodeJS.MemoryUsage);
    const runtime = new ImporterLiteRuntime(
      poolFor(db), {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'metadata-sparse-horizon-test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        LITE_CHAPTER_WORKERS: 1, MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );
    const refill = vi.fn().mockResolvedValue(1);
    (runtime as any).queue = {
      refillSourceMetadataEnrichment: refill,
      claimSourceMetadataRefresh: vi.fn().mockResolvedValue(null),
    };
    (runtime as any).isRuntimeLeader = true;

    await (runtime as any).refillSourceMetadataEnrichment(true);
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(refill).toHaveBeenCalledTimes(4);
    memorySpy.mockRestore();
  });

  it('keeps only one historical source fetch active while its metadata pool is persistently queued', async () => {
    const memorySpy = vi.spyOn(process, 'memoryUsage').mockReturnValue({ rss: 128 * 1024 * 1024 } as NodeJS.MemoryUsage);
    const lanePool = poolFor(db) as any;
    lanePool.waitingCount = 2;
    const runtime = new ImporterLiteRuntime(
      lanePool, {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'metadata-lane-pressure-test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        LITE_CHAPTER_WORKERS: 1, MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );
    let remainingClaims = 1;
    const claim = vi.fn(async () => remainingClaims-- > 0 ? ({ id: 'metadata-pressure-task' } as any) : null);
    (runtime as any).queue = { claimSourceMetadataRefresh: claim, refillSourceMetadataEnrichment: vi.fn().mockResolvedValue(0) };
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).sourceMetadataPoolWaitStartedAt = Date.now() - 15_001;
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    let started!: () => void;
    const startedFetch = new Promise<void>((resolve) => { started = resolve; });
    (runtime as any).catalog = async () => { started(); await held; };

    const running = (runtime as any).runSourceMetadataRefresh();
    await startedFetch;
    await Promise.resolve();
    expect(claim).toHaveBeenCalledTimes(1);
    expect((runtime as any).sourceMetadataLaneGate.active).toBe(1);
    release();
    await running;
    memorySpy.mockRestore();
  });

  it('wakes the protected source lane immediately after the producer admits metadata work', async () => {
    const memorySpy = vi.spyOn(process, 'memoryUsage').mockReturnValue({ rss: 128 * 1024 * 1024 } as NodeJS.MemoryUsage);
    const runtime = new ImporterLiteRuntime(
      poolFor(db), {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'metadata-admit-wakeup-test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        LITE_CHAPTER_WORKERS: 1, MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).queue = { refillSourceMetadataEnrichment: vi.fn().mockResolvedValue(2) };
    const wake = vi.spyOn(runtime as any, 'runSourceMetadataRefresh').mockResolvedValue(undefined);

    await (runtime as any).refillSourceMetadataEnrichment(true);

    expect(wake).toHaveBeenCalledTimes(1);
    memorySpy.mockRestore();
  });

  it('bounds a productive-source preference before returning to the durable cursor', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db), {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'metadata-preferred-burst-test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        LITE_CHAPTER_WORKERS: 1, MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );

    expect((runtime as any).nextPreferredSourceMetadataSource('productive-source')).toBe('productive-source');
    expect((runtime as any).nextPreferredSourceMetadataSource('productive-source')).toBe('productive-source');
    expect((runtime as any).nextPreferredSourceMetadataSource('productive-source')).toBe('productive-source');
    expect((runtime as any).nextPreferredSourceMetadataSource('productive-source')).toBeUndefined();
    expect((runtime as any).nextPreferredSourceMetadataSource('other-source')).toBe('other-source');
  });

  it('replays legacy Portuguese tag objects with a bounded v3 task after v2 completed', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000124','mangotoons','legacy-pt-tags',$1::uuid,
        'p1-a','P1 A',$2::jsonb,'SYNCED')`, [ids.p1a, JSON.stringify({
      tags: [{ id: 11, nome: 'Ação' }, { id: 12, nome: 'Fantasia' }],
    })]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      priority: 'P2', idempotencyKey: 'lite:metadata:v2:mangotoons:legacy-pt-tags',
      payload: { source: 'mangotoons', sourceWorkId: 'legacy-pt-tags', metadataEnrichment: true, metadataEnrichmentVersion: 2 } });
    const priorReplay = await repo.claim('metadata-v2', 60);
    expect(priorReplay).toBeTruthy();
    await repo.finish(priorReplay!, 'metadata-v2', { state: 'DONE' });

    expect(await repo.refillStoredMetadataEnrichment()).toBe(1);
    const replay = await db.query(`SELECT idempotency_key,payload FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:v3:mangotoons:legacy-pt-tags'`);
    expect(replay.rows[0]).toMatchObject({
      idempotency_key: 'lite:metadata:v3:mangotoons:legacy-pt-tags',
      payload: expect.objectContaining({ metadataEnrichment: true, metadataEnrichmentVersion: 3 }),
    });
  });

  it('gates the v4 multifonte replay and schedules one durable, throttled P2 mapping', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_sources (id) VALUES ('source-c')`);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES
        ('00000000-0000-4000-8000-000000000125','source-a','v4-source-a',$1::uuid,
          'p1-a-source-a','P1 Source A Title',$2::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000126','source-b','v4-provider-evidence',$1::uuid,
          'p1-a-secondary','P1 Secondary Title',$3::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000128','source-a','v4-z-empty',$1::uuid,
          'p1-a-empty','P1 A','{}'::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000129','source-c','v4-source-c',$1::uuid,
          'p1-a-source-c','P1 Source C Title',$3::jsonb,'SYNCED')`, [ids.p1a, JSON.stringify({
      titulo_alternative: ['P1 Source A Native'], generos: ['Ação'],
    }), JSON.stringify({
      titulo_alternative: ['P1 Native Title'], generos: ['Ação'],
    })]);
    expect(await repo.refillStoredMetadataEnrichmentV4()).toBe(0);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','true')`);

    expect(await repo.refillStoredMetadataEnrichmentV4()).toBe(1);
    const task = await db.query(`SELECT idempotency_key,payload,max_attempts,priority,next_run_at
      FROM importer_lite_tasks WHERE idempotency_key='lite:metadata:v4:source-a:v4-source-a'`);
    expect(task.rows[0]).toMatchObject({
      idempotency_key: 'lite:metadata:v4:source-a:v4-source-a', max_attempts: 4, priority: 3,
      payload: expect.objectContaining({ metadataEnrichment: true, metadataEnrichmentVersion: 4,
        metadataReplayReason: 'EXPLICIT_METADATA' }),
    });
    // V4 has its own claimant and deliberately does not refresh the generic
    // chapter-ready projection for each local merge.
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_ready_works
      WHERE work_key=$1`, [`work:${ids.p1a}`])).rows[0].count).toBe(0);
    const first = await repo.claimStoredMetadataReplay('v4-test-worker', 60, 4);
    await repo.finish(first!, 'v4-test-worker', { state: 'DONE' });
    // The durable source cursor advances after source-a. Its next turn must
    // give source-b a seat rather than draining one large provider first.
    expect(await repo.refillStoredMetadataEnrichmentV4()).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:v4:source-b:v4-provider-evidence'`)).rows[0].count).toBe(1);
    await db.query(`UPDATE importer_lite_tasks SET state='DONE',updated_at=now()
      WHERE idempotency_key='lite:metadata:v4:source-b:v4-provider-evidence'`);
    expect(await repo.refillStoredMetadataEnrichmentV4()).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:v4:source-c:v4-source-c'`)).rows[0].count).toBe(1);
    expect((await db.query(`SELECT value FROM settings WHERE key='metadata_replay_v4_enabled'`)).rows[0].value).toBe('true');
  });

  it('persists and clears the reversible V4 exclusive mode', async () => {
    expect(await repo.storedMetadataReplayV4ExclusiveMode()).toEqual({ enabled: false, intervalMs: 2_000 });
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_exclusive_mode',$1)`,
      [JSON.stringify({ enabled: true, intervalMs: 1_000 })]);
    expect(await repo.storedMetadataReplayV4ExclusiveMode()).toEqual({ enabled: true, intervalMs: 1_000 });
    await repo.disableStoredMetadataReplayV4ExclusiveMode();
    expect(await repo.storedMetadataReplayV4ExclusiveMode()).toEqual({ enabled: false, intervalMs: 2_000 });
  });

  it('does not turn a source slug fallback into a historical alias candidate', async () => {
    // Production's legacy settings table deliberately has no updated_at
    // column; replay cursors must remain compatible with it.
    await db.query(`ALTER TABLE settings DROP COLUMN updated_at`);
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000127','source-a','slug-fallback',$1::uuid,
        'p1-a-machine-slug','p1-a-machine-slug','{"genres":[]}'::jsonb,'SYNCED')`, [ids.p1a]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','true')`);

    expect(await repo.refillStoredMetadataEnrichmentV4()).toBe(0);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:v4:source-a:slug-fallback'`)).rows[0].count).toBe(0);
  });

  it('replays a display title even when a provider stores it as its slug', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000130','source-a','display-title-slug',$1::uuid,
        'Secondary Display Title','Secondary Display Title','{}'::jsonb,'SYNCED')`, [ids.p1a]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','true')`);

    expect(await repo.refillStoredMetadataEnrichmentV4()).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:v4:source-a:display-title-slug'`)).rows[0].count).toBe(1);
  });

  it('replays explicit object-shaped provider metadata through the v4 selector', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000131','source-a','object-metadata',$1::uuid,
        'p1-a-machine-slug','p1-a-machine-slug',$2::jsonb,'SYNCED')`, [ids.p1a,
      JSON.stringify({ generos: [{ nome: 'Ação' }] })]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','true')`);

    expect(await repo.refillStoredMetadataEnrichmentV4()).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:v4:source-a:object-metadata'`)).rows[0].count).toBe(1);
  });

  it('keeps a bounded three-item v4 burst before returning to source rotation', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES
        ('00000000-0000-4000-8000-000000000132','source-a','burst-1',$1::uuid,'p1-a','P1 A',$2::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000133','source-a','burst-2',$1::uuid,'p1-a','P1 A',$2::jsonb,'SYNCED')`,
      [ids.p1a, JSON.stringify({ tags: ['Ação'] })]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','true')`);

    expect(await repo.refillStoredMetadataEnrichmentV4()).toBe(1);
    await db.query(`UPDATE importer_lite_tasks SET state='DONE',updated_at=now()
      WHERE idempotency_key='lite:metadata:v4:source-a:burst-1'`);
    expect(await repo.refillStoredMetadataEnrichmentV4()).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:v4:source-a:burst-2'`)).rows[0].count).toBe(1);
  });

  it('uses a bounded faster due fence for V4 without admitting a second task', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000134','source-a','fast-fence',$1::uuid,
        'p1-a','P1 A',$2::jsonb,'SYNCED')`, [ids.p1a, JSON.stringify({ tags: ['Ação'] })]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','true')`);

    expect(await repo.refillStoredMetadataEnrichmentV4(2, 10_000)).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key LIKE 'lite:metadata:v4:%' AND state IN ('READY','RUNNING','RETRY_WAIT')`)).rows[0].count).toBe(1);
  });

  it('admits two V4 tasks only when they belong to different canonical works', async () => {
    await db.query(`UPDATE works SET published=true WHERE id IN ($1::uuid,$2::uuid)`, [ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES
        ('00000000-0000-4000-8000-000000000135','source-a','parallel-a',$1::uuid,'p1-a','P1 A',$3::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000136','source-b','parallel-b',$2::uuid,'p1-b','P1 B',$3::jsonb,'SYNCED')`,
      [ids.p1a, ids.p1b, JSON.stringify({ tags: ['Ação'] })]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','true')`);

    expect(await repo.refillStoredMetadataEnrichmentV4(2, 500)).toBe(1);
    expect(await repo.refillStoredMetadataEnrichmentV4(2, 500)).toBe(1);
    expect((await db.query(`SELECT count(DISTINCT work_id)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key LIKE 'lite:metadata:v4:%' AND state IN ('READY','RUNNING','RETRY_WAIT')`)).rows[0].count).toBe(2);

    await db.query(`UPDATE importer_lite_tasks SET state='DONE' WHERE idempotency_key='lite:metadata:v4:source-a:parallel-a'`);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000137','source-c','same-work',$1::uuid,'p1-a','P1 A',$2::jsonb,'SYNCED')`,
      [ids.p1b, JSON.stringify({ tags: ['Fantasia'] })]);
    await db.query(`INSERT INTO importer_sources(id) VALUES('source-c')`);
    expect(await repo.refillStoredMetadataEnrichmentV4(2, 500)).toBe(0);
  });

  it('admits four exclusive V4 tasks only for four distinct canonical works', async () => {
    await db.query(`UPDATE works SET published=true WHERE id IN ($1::uuid,$2::uuid,$3::uuid,$4::uuid)`, [ids.staff, ids.p1a, ids.p1b, ids.publish]);
    await db.query(`INSERT INTO importer_sources(id) VALUES('source-c'),('source-d')`);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES
        ('00000000-0000-4000-8000-000000000138','source-a','triple-a',$1::uuid,'staff','Staff',$5::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000139','source-b','triple-b',$2::uuid,'p1-a','P1 A',$5::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000140','source-c','triple-c',$3::uuid,'p1-b','P1 B',$5::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000141','source-d','triple-d',$4::uuid,'publish','Publish',$5::jsonb,'SYNCED')`,
      [ids.staff, ids.p1a, ids.p1b, ids.publish, JSON.stringify({ tags: ['Ação'] })]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','true')`);

    expect(await repo.refillStoredMetadataEnrichmentV4(4, 500)).toBe(1);
    expect(await repo.refillStoredMetadataEnrichmentV4(4, 500)).toBe(1);
    expect(await repo.refillStoredMetadataEnrichmentV4(4, 500)).toBe(1);
    expect(await repo.refillStoredMetadataEnrichmentV4(4, 500)).toBe(1);
    expect((await db.query(`SELECT count(DISTINCT work_id)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key LIKE 'lite:metadata:v4:%' AND state IN ('READY','RUNNING','RETRY_WAIT')`)).rows[0].count).toBe(4);
  });

  it('fills one bounded source burst with distinct canonical V4 works', async () => {
    await db.query(`UPDATE works SET published=true WHERE id IN ($1::uuid,$2::uuid,$3::uuid)`, [ids.staff, ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES
        ('00000000-0000-4000-8000-000000000142','source-a','batch-1',$1::uuid,'staff','Staff',$4::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000143','source-a','batch-2',$2::uuid,'p1-a','P1 A',$4::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000144','source-a','batch-3',$3::uuid,'p1-b','P1 B',$4::jsonb,'SYNCED')`,
      [ids.staff, ids.p1a, ids.p1b, JSON.stringify({ tags: ['Ação'] })]);
    await db.query(`INSERT INTO settings(key,value) VALUES('metadata_replay_v4_enabled','true')`);

    expect(await repo.refillStoredMetadataEnrichmentV4(4, 500)).toBe(3);
    expect((await db.query(`SELECT count(DISTINCT work_id)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key LIKE 'lite:metadata:v4:%' AND state IN ('READY','RUNNING','RETRY_WAIT')`)).rows[0].count).toBe(3);
  });

  it('prioritizes a real alias over empty or tag-only legacy metadata from the same source', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES
        ('00000000-0000-4000-8000-000000000119','source-a','tags-only',$1::uuid,'p1-a','P1 A',$2::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000120','source-a','empty-metadata',$1::uuid,'p1-a','P1 A',$3::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000121','source-a','real-metadata',$1::uuid,'p1-a','P1 A',$4::jsonb,'SYNCED')`, [
      ids.p1a, JSON.stringify({ tags: ['MILF'] }), JSON.stringify({ alternative_titles: '[]' }),
      JSON.stringify({ alternative_titles: '["Stored Alias"]' }),
    ]);
    expect(await repo.refillStoredMetadataEnrichment()).toBe(1);
    expect((await db.query(`SELECT idempotency_key FROM importer_lite_tasks
      WHERE idempotency_key LIKE 'lite:metadata:v2:source-a:%'`)).rows[0].idempotency_key)
      .toBe('lite:metadata:v2:source-a:real-metadata');
  });

  it('prioritizes a mapping that can add a missing alias over an already represented alias', async () => {
    await db.query(`UPDATE works SET published=true,aliases=ARRAY['Existing Alias'] WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES
        ('00000000-0000-4000-8000-000000000122','source-a','represented',$1::uuid,'p1-a','P1 A',$3::jsonb,'SYNCED'),
        ('00000000-0000-4000-8000-000000000123','source-b','missing',$2::uuid,'p1-b','P1 B',$4::jsonb,'SYNCED')`, [
      ids.p1a, ids.p1b, JSON.stringify({ alternative_titles: '["Existing Alias"]' }),
      JSON.stringify({ alternative_titles: '["New Missing Alias"]' }),
    ]);
    expect(await repo.refillStoredMetadataEnrichment()).toBe(1);
    expect((await db.query(`SELECT idempotency_key FROM importer_lite_tasks
      WHERE idempotency_key LIKE 'lite:metadata:v2:%'`)).rows[0].idempotency_key)
      .toBe('lite:metadata:v2:source-b:missing');
  });

  it('refills a low P1 reserve from durable historical mappings without inventing numeric gaps or letting one source monopolize', async () => {
    await db.query(`INSERT INTO importer_sources(id) VALUES('source-c')`);
    const mappings = [
      ['00000000-0000-4000-8000-000000000101', 'source-a', 'a-1', ids.p1a],
      ['00000000-0000-4000-8000-000000000102', 'source-a', 'a-2', ids.p1a],
      ['00000000-0000-4000-8000-000000000103', 'source-a', 'a-3', ids.p1a],
      ['00000000-0000-4000-8000-000000000104', 'source-b', 'b-1', ids.p1b],
      ['00000000-0000-4000-8000-000000000105', 'source-b', 'b-2', ids.p1b],
      ['00000000-0000-4000-8000-000000000106', 'source-c', 'c-1', ids.p1a],
    ];
    for (const [id, source, sourceWorkId, workId] of mappings) {
      await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
        VALUES($1::uuid,$2,$3,$4::uuid,'WAITING_ADMISSION')`, [id, source, sourceWorkId, workId]);
    }

    expect(await repo.refillHistoricalP1Catalog(8, 4)).toBe(4);
    const admitted = await db.query(`SELECT priority,base_priority,task_type,payload->>'source' AS source,
      payload->>'historicalP1Backfill' AS historical
      FROM importer_lite_tasks ORDER BY idempotency_key`);
    expect(admitted.rows).toHaveLength(4);
    expect(admitted.rows.every((row: any) => row.priority === 2 && row.base_priority === 2 && row.task_type === 'CATALOG' && row.historical === 'true')).toBe(true);
    const perSource = admitted.rows.reduce((counts: Record<string, number>, row: any) => {
      counts[row.source] = (counts[row.source] || 0) + 1;
      return counts;
    }, {});
    expect(Math.max(...Object.values(perSource))).toBeLessThanOrEqual(2);

    // A low reserve must not append another batch every recovery turn while
    // the first historical catalog batch is still waiting behind imports.
    expect(await repo.refillHistoricalP1Catalog(8, 4)).toBe(0);

    // Existing executable P1 work limits the producer to the small existing
    // reserve; it cannot grow a hidden queue on every recovery turn.
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'ready-reserve-1', priority: 'P1', idempotencyKey: 'ready-reserve-1' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'ready-reserve-2', priority: 'P1', idempotencyKey: 'ready-reserve-2' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      chapterKey: 'ready-reserve-3', priority: 'P1', idempotencyKey: 'ready-reserve-3' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      chapterKey: 'ready-reserve-4', priority: 'P1', idempotencyKey: 'ready-reserve-4' });
    expect(await repo.refillHistoricalP1Catalog(8, 4)).toBe(0);
  });

  it('keeps a four-source historical P1 producer lane while a hot import reserve is healthy', async () => {
    await db.query(`INSERT INTO importer_sources(id) VALUES('source-c'),('source-d'),('source-e')`);
    for (let index = 0; index < 8; index++) {
      await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
        chapterKey: `hot-${index}`, priority: 'P1', idempotencyKey: `hot-${index}` });
      await db.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
        VALUES($1,$2,'source-a','hot-a',$3,'pt-BR','default',$2,10,true)`, [ids.p1a, `hot-${index}`, `hot-source-${index}`]);
    }
    const mappings = [
      ['00000000-0000-4000-8000-000000000111', 'source-a', 'a-hot', ids.p1a],
      ['00000000-0000-4000-8000-000000000112', 'source-b', 'b-hot', ids.p1b],
      ['00000000-0000-4000-8000-000000000113', 'source-c', 'c-hot', ids.p1a],
      ['00000000-0000-4000-8000-000000000114', 'source-d', 'd-hot', ids.p1b],
      ['00000000-0000-4000-8000-000000000115', 'source-e', 'e-hot', ids.p1a],
    ];
    for (const [id, source, sourceWorkId, workId] of mappings) {
      await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
        VALUES($1::uuid,$2,$3,$4::uuid,'WAITING_ADMISSION')`, [id, source, sourceWorkId, workId]);
    }

    expect(await repo.refillHistoricalP1Catalog(4, 16)).toBe(4);
    const admitted = await db.query(`SELECT payload->>'source' AS source,payload->>'deferBehindReadyImport' AS deferred FROM importer_lite_tasks
      WHERE task_type='CATALOG' AND payload ? 'historicalP1Backfill' ORDER BY source`);
    expect(new Set(admitted.rows.map((row: any) => row.source)).size).toBe(4);
    expect(admitted.rows.every((row: any) => row.deferred === 'true')).toBe(true);
    // Four pending checks cap the healthy-reserve producer; the fifth mapped
    // source waits durably without turning recovery into a catalog sweep.
    expect(await repo.refillHistoricalP1Catalog(4, 16)).toBe(0);
  });

  it('materializes only manifest-proven historical gaps with textual chapter identities', async () => {
    await db.query(`UPDATE works SET published=true WHERE id IN ($1,$2)`, [ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter)
      VALUES
        ($1,'pt-br|default|chapter|12.10|-','source-a','gap-a','gap-a-12-10','pt-BR','default','n|000000000012|100000000000|10|||',10,true,true),
        ($2,'pt-br|default|special|12|extra','source-b','gap-b','gap-b-extra','pt-BR','default','n|000000000012|000000000000||||extra',10,true,true)`, [ids.p1a, ids.p1b]);

    expect(await repo.refillManifestProvenP1Imports(8, 4)).toBe(2);
    const repairs = await db.query(`SELECT work_id::text AS work_id,chapter_key,priority,base_priority,
      payload->>'displayNumber' AS display_number,payload->>'chapterTitle' AS chapter_title,
      payload->>'chapterKind' AS chapter_kind,payload->>'manifestP1Repair' AS repair
      FROM importer_lite_tasks WHERE payload ? 'manifestP1Repair' ORDER BY work_id,chapter_key`);
    expect(repairs.rows).toEqual([
      { work_id: ids.p1a, chapter_key: 'pt-br|default|chapter|12.10|-', priority: 2, base_priority: 2,
        display_number: '12.10', chapter_title: '', chapter_kind: 'chapter', repair: 'true' },
      { work_id: ids.p1b, chapter_key: 'pt-br|default|special|12|extra', priority: 2, base_priority: 2,
        display_number: '12', chapter_title: 'extra', chapter_kind: 'special', repair: 'true' },
    ]);
    // The producer reads concrete manifest rows only: it must never fill an
    // imagined numeric interval between two known chapter identities.
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE payload ? 'manifestP1Repair' AND chapter_key LIKE '%|12.1|%'`)).rows[0].count).toBe(0);
    expect(await repo.refillManifestProvenP1Imports(8, 4)).toBe(0);
  });

  it('materializes confirmed gaps even when their only source is externally blocked', async () => {
    await db.query(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS blocked_reason text, ADD COLUMN IF NOT EXISTS blocked_details jsonb`);
    await db.query(`UPDATE works SET published=true WHERE id=$1`, [ids.p1a]);
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter)
      VALUES($1,'pt-br|default|chapter|21|-','source-a','blocked-gap','blocked-gap-21','pt-BR','default','n|000021|000000|0',10,true,true)`, [ids.p1a]);
    await db.query(`UPDATE importer_sources SET status='COOLDOWN',blocked_reason='TURNSTILE',
      cooldown_until=now()-interval '1 second' WHERE id='source-a'`);

    expect(await repo.refillManifestProvenP1Imports(8, 4)).toBe(1);
    expect((await db.query(`SELECT priority FROM importer_lite_tasks
      WHERE idempotency_key LIKE 'lite:chapter:%:pt-br|default|chapter|21|-'`)).rows[0].priority).toBe(2);
  });

  it('one-shot materialization fills a confirmed-missing backlog idempotently despite an abundant P1 reserve', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1`,[ids.p1a]);
    for (const chapterKey of ['pt-br|default|chapter|31|-','pt-br|default|chapter|32.10|-','pt-br|default|special|32|extra']) {
      await db.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter)
        VALUES($1,$2,'source-a','gap-backfill',$3,'pt-BR','default',$2,10,true,true)`,
        [ids.p1a,chapterKey,`source-chapter-${chapterKey}`]);
    }
    for (let index=0;index<20;index++) await repo.enqueue({
      taskType:'IMPORT_CHAPTER',workId:ids.p1b,workKey:`work:${ids.p1b}`,chapterKey:`reserve-${index}`,
      priority:'P1',idempotencyKey:`reserve-before-manifest-repair-${index}`,
    });
    expect(await repo.materializeConfirmedMissingP1Tasks(20)).toBe(3);
    expect(await repo.materializeConfirmedMissingP1Tasks(20)).toBe(0);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks WHERE payload ? 'manifestP1Repair'`)).rows[0].count).toBe(3);
  });

  it('keeps an executable P1 reserve dominant without hiding proven historical repairs forever', async () => {
    await db.query(`UPDATE works SET published=true WHERE id IN ($1,$2)`, [ids.p1a, ids.p1b]);
    for (let index = 0; index < 8; index++) {
      const key = `reserve-${index}`;
      await db.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter)
        VALUES($1,$2,'source-a','reserve',$3,'pt-BR','default',$2,10,true,true)`, [ids.p1a, key, `reserve-source-${index}`]);
      await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
        chapterKey: key, priority: 'P1', idempotencyKey: `lite:chapter:${ids.p1a}:${key}` });
    }
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter)
      VALUES($1,'pt-br|default|chapter|99|-','source-b','repair','repair-99','pt-BR','default','n|000000000099|000000000000||||',10,true,true)`, [ids.p1b]);

    expect(await repo.refillManifestProvenP1Imports(8, 16)).toBe(1);
    expect((await db.query(`SELECT priority,payload->>'manifestP1Repair' AS repair
      FROM importer_lite_tasks WHERE idempotency_key='lite:chapter:' || $1 || ':pt-br|default|chapter|99|-'`, [ids.p1b])).rows[0])
      .toEqual({ priority: 2, repair: 'true' });
  });

  it('applies source and work fairness sequentially so crossed manifest order cannot erase every repair candidate', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1`, [ids.p1b]);
    const rows = [
      ['3', '2026-01-01T00:00:00Z'], ['4', '2026-01-02T00:00:00Z'],
      ['1', '2026-01-03T00:00:00Z'], ['2', '2026-01-04T00:00:00Z'],
    ];
    for (const [display, updatedAt] of rows) {
      const whole = String(display).padStart(12, '0');
      await db.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter,updated_at)
        VALUES($1,$2,'source-a','crossed',$3,'pt-BR','default',$4,10,true,true,$5::timestamptz)`, [
        ids.p1b, `pt-br|default|chapter|${display}|-`, `crossed-${display}`,
        `n|${whole}|000000000000|||`, updatedAt,
      ]);
    }
    expect(await repo.refillManifestProvenP1Imports(8, 4)).toBe(2);
    const tasks = await db.query(`SELECT chapter_key FROM importer_lite_tasks
      WHERE payload ? 'manifestP1Repair' ORDER BY chapter_key`);
    expect(tasks.rows.map((row: any) => row.chapter_key)).toEqual([
      'pt-br|default|chapter|3|-', 'pt-br|default|chapter|4|-',
    ]);
  });

  it('keeps one deferred historical P1 producer turn while an abundant import reserve is healthy', async () => {
    for (let index = 0; index < 64; index++) {
      await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
        chapterKey: `abundant-${index}`, priority: 'P1', idempotencyKey: `abundant-${index}` });
      await db.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
        VALUES($1,$2,'source-a','abundant-a',$3,'pt-BR','default',$2,10,true)`, [ids.p1a, `abundant-${index}`, `abundant-source-${index}`]);
    }
    await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
      VALUES('00000000-0000-4000-8000-000000000125'::uuid,'source-b','abundant-b',$1::uuid,'WAITING_ADMISSION')`, [ids.p1a]);

    expect(await repo.refillHistoricalP1Catalog(4, 16)).toBe(1);
    expect(repo.hasAbundantHistoricalP1Reserve()).toBe(true);
    expect((await db.query(`SELECT payload->>'source' AS source,payload->>'deferBehindReadyImport' AS deferred
      FROM importer_lite_tasks WHERE task_type='CATALOG' AND payload ? 'historicalP1Backfill'`)).rows)
      .toEqual([{ source: 'source-b', deferred: 'true' }]);
    expect(await repo.promoteDeferredHistoricalP1Catalog()).toBe(true);
    expect((await db.query(`SELECT payload->>'deferBehindReadyImport' AS deferred
      FROM importer_lite_tasks WHERE task_type='CATALOG' AND payload ? 'historicalP1Backfill'`)).rows)
      .toEqual([{ deferred: null }]);
  });

  it('rotates the historical P1 producer past a hot source instead of cutting candidates by global recency', async () => {
    await db.query(`INSERT INTO importer_sources(id) VALUES('source-c'),('source-d'),('source-e')`);
    for (let index = 0; index < 64; index++) {
      await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
        chapterKey: `recent-hot-${index}`, priority: 'P1', idempotencyKey: `recent-hot-${index}` });
    }
    for (let index = 0; index < 128; index++) {
      await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
        VALUES($1::uuid,'source-a',$2,$3::uuid,'WAITING_ADMISSION')`, [
        `00000000-0000-4000-8000-${String(300 + index).padStart(12, '0')}`,
        `recent-hot-${String(index).padStart(3, '0')}`, ids.p1a,
      ]);
    }
    for (const [index, source] of ['source-b', 'source-c', 'source-d', 'source-e'].entries()) {
      await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
        VALUES($1::uuid,$2,$3,$4::uuid,'WAITING_ADMISSION')`, [
        `00000000-0000-4000-8000-${String(500 + index).padStart(12, '0')}`,
        source, `${source}-older`, ids.p1b,
      ]);
    }

    // The first bounded turn samples source-a through source-d. Its four
    // durable checks finish, then the cursor must reach source-e even though
    // source-a still owns every globally-newest mapping row.
    expect(await repo.refillHistoricalP1Catalog(4, 16)).toBe(4);
    await db.query(`UPDATE importer_lite_tasks SET state='DONE'
      WHERE task_type='CATALOG' AND payload ? 'historicalP1Backfill'`);
    expect(await repo.refillHistoricalP1Catalog(4, 16)).toBeGreaterThanOrEqual(1);
    expect((await db.query(`SELECT payload->>'source' AS source
      FROM importer_lite_tasks WHERE idempotency_key='lite:catalog:source-e:source-e-older'`)).rows)
      .toEqual([{ source: 'source-e' }]);
  });

  it('does not let a cooled single-source P1 backlog suppress healthy catalog alternatives', async () => {
    for (let index = 0; index < 64; index++) {
      const chapterKey = `cooled-${index}`;
      await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
        chapterKey, priority: 'P1', idempotencyKey: `cooled-${index}` });
      await db.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
        VALUES($1,$2,'source-a','cooled-a',$3,'pt-BR','default',$2,10,true)`, [ids.p1a, chapterKey, `cooled-source-${index}`]);
    }
    await db.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until)
      VALUES('source-a','RATE_LIMITED',now()+interval '5 minutes')`);
    await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
      VALUES('00000000-0000-4000-8000-000000000126'::uuid,'source-b','healthy-alternative',$1::uuid,'WAITING_ADMISSION')`, [ids.p1b]);

    expect(await repo.refillHistoricalP1Catalog(4, 16)).toBe(1);
    expect(repo.hasAbundantHistoricalP1Reserve()).toBe(false);
    expect((await db.query(`SELECT payload->>'source' AS source FROM importer_lite_tasks
      WHERE idempotency_key='lite:catalog:source-b:healthy-alternative'`)).rows[0])
      .toEqual({ source: 'source-b' });
  });

  it('replaces duplicate pending source seats with distinct healthy P1 catalog sources', async () => {
    await db.query(`INSERT INTO importer_sources(id) VALUES('source-c'),('source-d'),('source-e')`);
    for (let index = 0; index < 4; index++) {
      await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
        chapterKey: `reserve-diverse-${index}`, priority: 'P1', idempotencyKey: `reserve-diverse-${index}` });
      await db.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
        VALUES($1,$2,'source-a','reserve-a',$3,'pt-BR','default',$2,10,true)`, [ids.p1a, `reserve-diverse-${index}`, `reserve-source-${index}`]);
    }
    // These emulate durable retries from a previously unhealthy source. They
    // remain intact, but must count as one active source seat, not three.
    for (let index = 0; index < 3; index++) {
      await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`,
        priority: 'P1', idempotencyKey: `stale-source-a-${index}`,
        payload: { source: 'source-a', sourceWorkId: `a-stale-${index}`, historicalP1Backfill: true } });
    }
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      priority: 'P1', idempotencyKey: 'stale-source-b',
      payload: { source: 'source-b', sourceWorkId: 'b-stale', historicalP1Backfill: true } });
    const mappings = [
      ['00000000-0000-4000-8000-000000000131', 'source-c', 'c-fresh', ids.p1a],
      ['00000000-0000-4000-8000-000000000132', 'source-d', 'd-fresh', ids.p1b],
      ['00000000-0000-4000-8000-000000000133', 'source-e', 'e-fresh', ids.p1a],
    ];
    for (const [id, source, sourceWorkId, workId] of mappings) {
      await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
        VALUES($1::uuid,$2,$3,$4::uuid,'WAITING_ADMISSION')`, [id, source, sourceWorkId, workId]);
    }

    // The healthy reserve has four seats. Two are already represented by
    // source-a/source-b, so only two distinct alternatives are admitted.
    expect(await repo.refillHistoricalP1Catalog(4, 16)).toBe(2);
    const fresh = await db.query(`SELECT payload->>'source' AS source FROM importer_lite_tasks
      WHERE payload->>'sourceWorkId' IN ('c-fresh','d-fresh','e-fresh') ORDER BY source`);
    expect(fresh.rows.map((row: any) => row.source)).toHaveLength(2);
    expect(fresh.rows.every((row: any) => !['source-a', 'source-b'].includes(row.source))).toBe(true);
  });

  it('does not let delayed source cooldown checks occupy the healthy historical P1 producer lane', async () => {
    await db.query(`INSERT INTO importer_sources(id) VALUES('source-c'),('source-d'),('source-e'),('source-f')`);
    for (let index = 0; index < 4; index++) {
      await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
        chapterKey: `reserve-${index}`, priority: 'P1', idempotencyKey: `reserve-${index}` });
      await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1b, workKey: `work:${ids.p1b}`, priority: 'P1',
        idempotencyKey: `cooldown-${index}`, nextRunAt: new Date(Date.now() + 60_000),
        payload: { source: index % 2 ? 'source-a' : 'source-b', sourceWorkId: `cooldown-${index}`, historicalP1Backfill: true } });
    }
    const mappings = [
      ['00000000-0000-4000-8000-000000000121', 'source-c', 'c-ready', ids.p1a],
      ['00000000-0000-4000-8000-000000000122', 'source-d', 'd-ready', ids.p1b],
      ['00000000-0000-4000-8000-000000000123', 'source-e', 'e-ready', ids.p1a],
      ['00000000-0000-4000-8000-000000000124', 'source-f', 'f-ready', ids.p1b],
    ];
    for (const [id, source, sourceWorkId, workId] of mappings) {
      await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
        VALUES($1::uuid,$2,$3,$4::uuid,'WAITING_ADMISSION')`, [id, source, sourceWorkId, workId]);
    }

    expect(await repo.refillHistoricalP1Catalog(4, 16)).toBe(4);
    const fresh = await db.query(`SELECT count(*)::int AS tasks FROM importer_lite_tasks
      WHERE task_type='CATALOG' AND payload ? 'historicalP1Backfill' AND next_run_at<=now()`);
    expect(fresh.rows[0].tasks).toBe(4);
  });

  it('admits the bounded historical P1 source reserve alongside P1 imports instead of hiding it behind a giant work', async () => {
    const giant = '00000000-0000-4000-8000-000000000099';
    await db.query(`INSERT INTO works(id,title,slug,cover_id) VALUES($1::uuid,'Giant','giant',$2::uuid)`, [giant, ids.cover]);
    for (let index = 0; index < 32; index++) {
      await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: giant, workKey: `work:${giant}`,
        chapterKey: `giant-${index}`, priority: 'P1', idempotencyKey: `giant-${index}` });
    }
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      priority: 'P1', idempotencyKey: 'historical-source-a',
      payload: { source: 'source-a', sourceWorkId: 'a', historicalP1Backfill: true } });
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      priority: 'P1', idempotencyKey: 'historical-source-b',
      payload: { source: 'source-b', sourceWorkId: 'b', historicalP1Backfill: true } });

    const claimed = [] as string[];
    for (let index = 0; index < 3; index++) {
      const task = await repo.claim(`historical-fair-${index}`, 120);
      expect(task).toBeTruthy();
      claimed.push(task!.taskType);
      expect(await repo.finish(task!, `historical-fair-${index}`, { state: 'DONE' })).toBe(true);
    }

    // Both bounded source checks receive a turn before the giant can consume
    // a second wave. Without the equal admission rank they remain hidden
    // behind all 32 IMPORT_CHAPTER tasks.
    expect(claimed.filter((type) => type === 'CATALOG')).toHaveLength(2);
  });

  it('keeps a healthy historical P1 producer reserve behind a deep materialized import backlog', async () => {
    const giant = '00000000-0000-4000-8000-000000000098';
    await db.query(`INSERT INTO works(id,title,slug,cover_id) VALUES($1::uuid,'Deep giant','deep-giant',$2::uuid)`, [giant, ids.cover]);
    for (let index = 0; index < 80; index++) {
      await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: giant, workKey: `work:${giant}`,
        chapterKey: `deep-${index}`, priority: 'P1', idempotencyKey: `deep-${index}` });
    }
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      priority: 'P1', idempotencyKey: 'deferred-historical-source',
      payload: { source: 'source-a', sourceWorkId: 'deep-a', historicalP1Backfill: true, deferBehindReadyImport: true } });

    for (let index = 0; index < 4; index++) {
      const task = await repo.claim(`deep-import-${index}`, 120);
      expect(task?.taskType).toBe('IMPORT_CHAPTER');
      await repo.finish(task!, `deep-import-${index}`, { state: 'DONE' });
    }
  });

  it('promotes only one deferred historical source check per bounded recovery turn', async () => {
    const giant = '00000000-0000-4000-8000-000000000097';
    await db.query(`INSERT INTO works(id,title,slug) VALUES($1::uuid,'Quota giant','quota-giant')`, [giant]);
    for (let index = 0; index < 80; index++) {
      await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: giant, workKey: `work:${giant}`,
        chapterKey: `quota-${index}`, priority: 'P1', idempotencyKey: `quota-${index}` });
    }
    for (const [index, workId] of [ids.p1a, ids.p1b].entries()) {
      await repo.enqueue({ taskType: 'CATALOG', workId, workKey: `work:${workId}`,
        priority: 'P1', idempotencyKey: `deferred-turn-${index}`,
        payload: { source: `source-${index + 1}`, sourceWorkId: `turn-${index}`, historicalP1Backfill: true, deferBehindReadyImport: true } });
    }

    expect(await repo.promoteDeferredHistoricalP1Catalog(['source-1'])).toBe(true);
    const turns = await db.query(`SELECT payload->>'source' AS source,payload->>'deferBehindReadyImport' AS deferred,count(*)::int AS tasks
      FROM importer_lite_tasks WHERE idempotency_key LIKE 'deferred-turn-%'
      GROUP BY source,deferred ORDER BY source`);
    expect(turns.rows).toEqual([
      { source: 'source-1', deferred: 'true', tasks: 1 },
      { source: 'source-2', deferred: null, tasks: 1 },
    ]);
    expect((await db.query(`SELECT operation_rank FROM importer_lite_ready_works
      WHERE work_key=$1`, [`work:${ids.p1b}`])).rows[0].operation_rank).toBe(1);
  });

  it('samples multiple ready P1 source routes outside the claim path', async () => {
    await db.query(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS blocked_reason text, ADD COLUMN IF NOT EXISTS blocked_details jsonb`);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'source-fair-a', priority: 'P1', idempotencyKey: 'source-fair-a' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      chapterKey: 'source-fair-b', priority: 'P1', idempotencyKey: 'source-fair-b' });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
      VALUES($1::uuid,'source-fair-a','source-a','a','a-1','pt-BR','default','n|000001|000000|0',10,true),
            ($1::uuid,'source-fair-a','source-b','b','a-fallback','pt-BR','default','n|000001|000000|0',20,true),
            ($2::uuid,'source-fair-b','source-b','b','b-1','pt-BR','default','n|000001|000000|0',10,true)`, [ids.p1a, ids.p1b]);

    expect(await repo.readyImportSources()).toEqual(['source-a', 'source-b']);
    expect(await repo.readyImportSourceDemand()).toEqual(new Map([['source-a', 1], ['source-b', 1]]));

    await db.query(`UPDATE importer_sources SET status='COOLDOWN',blocked_reason='TURNSTILE',
      cooldown_until=now()-interval '1 second' WHERE id='source-b'`);
    expect(await repo.readyImportSources()).toEqual(['source-a']);
    await db.query(`UPDATE importer_sources SET blocked_details='{"probe_success":true}'::jsonb WHERE id='source-b'`);
    expect(await repo.readyImportSources()).toEqual(['source-a', 'source-b']);
  });

  it('does not consume an import source slot for historical catalog work before it materializes chapters', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'hot-source-a', priority: 'P1', idempotencyKey: 'hot-source-a' });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
      VALUES($1::uuid,'hot-source-a','source-a','a','hot-a','pt-BR','default','n|000001|000000|0',10,true)`, [ids.p1a]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      priority: 'P1', idempotencyKey: 'historical-visible-source-b',
      payload: { source: 'source-b', sourceWorkId: 'b', historicalP1Backfill: true } });

    expect(await repo.readyImportSources()).toEqual(['source-a']);
  });

  it('does not let a P0 task behind an active earlier chapter hide executable P1 source demand', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'active-earlier-p1', priority: 'P1', idempotencyKey: 'active-earlier-p1',
      payload: { orderKey: 'n|000001|000000|0' } });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'blocked-p0', priority: 'P0', idempotencyKey: 'blocked-p0',
      payload: { orderKey: 'n|000002|000000|0' } });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      chapterKey: 'executable-p1', priority: 'P1', idempotencyKey: 'executable-p1',
      payload: { orderKey: 'n|000001|000000|0' } });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
      VALUES($1::uuid,'blocked-p0','source-a','a','blocked-p0','pt-BR','default','n|000002|000000|0',10,true),
            ($2::uuid,'executable-p1','source-b','b','executable-p1','pt-BR','default','n|000001|000000|0',10,true)`, [ids.p1a, ids.p1b]);
    await db.query(`UPDATE importer_lite_tasks
      SET state='RUNNING',lease_owner='active-p1',lease_until=now()+interval '5 minutes'
      WHERE idempotency_key='active-earlier-p1'`);

    expect(await repo.readyImportSources()).toEqual(['source-b']);
  });

  it('falls through an unroutable P0 lane to executable P1 source demand', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'unroutable-p0', priority: 'P0', idempotencyKey: 'unroutable-p0',
      payload: { orderKey: 'n|000001|000000|0' } });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      chapterKey: 'routable-p1', priority: 'P1', idempotencyKey: 'routable-p1',
      payload: { orderKey: 'n|000001|000000|0' } });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
      VALUES($1::uuid,'routable-p1','source-b','b','routable-p1','pt-BR','default','n|000001|000000|0',10,true)`, [ids.p1b]);

    expect(await repo.readyImportSources()).toEqual(['source-b']);
  });

  it('keeps a routable P1 alternative in the fairness view behind live P0 work', async () => {
    // The durable claim remains P0-first. This view is only consumed by the
    // local source permits: one P0 may wait for its saturated source, while a
    // later P0 claim can yield and let P1 use an otherwise-idle chapter seat.
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'p0-hot-source', priority: 'P0', idempotencyKey: 'p0-hot-source',
      payload: { orderKey: 'n|000001|000000|0' } });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      chapterKey: 'p1-alternative-source', priority: 'P1', idempotencyKey: 'p1-alternative-source',
      payload: { orderKey: 'n|000001|000000|0' } });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
      VALUES($1::uuid,'p0-hot-source','source-a','a','p0-hot','pt-BR','default','n|000001|000000|0',10,true),
            ($2::uuid,'p1-alternative-source','source-b','b','p1-alternative','pt-BR','default','n|000001|000000|0',10,true)`, [ids.p1a, ids.p1b]);

    expect(await repo.readyImportSourceDemand()).toEqual(new Map([['source-a', 1], ['source-b', 1]]));
  });

  it('caps one source only while other ready sources can use a chapter slot', () => {
    const executor = new LiteExecutor({} as Pool, repo, {} as SourceRegistry, {} as StorageProvider, ids.staff,
      new AsyncSemaphore(24, 'fair-downloads'), 4, new AsyncSemaphore(48, 'fair-buffers'), new HostRateLimiter(2), 5);
    const mangaFlix = (executor as any).sourceSlot('mangaflix') as AsyncSemaphore;
    expect(mangaFlix.capacity).toBe(8);
    executor.setFairSourceDemandCounts(new Map([['mangaflix',1],['source-b',1],['source-c',1],['source-d',1],['source-e',1]]));
    expect(mangaFlix.capacity).toBe(1);
    // A selected fallback is folded into the current bounded set rather than
    // bypassing source fairness until the next periodic sample.
    const fallback = (executor as any).sourceSlot('fallback') as AsyncSemaphore;
    expect(fallback.capacity).toBe(1);
  });

  it('uses executable source demand without reserving idle chapter shares', () => {
    const executor = new LiteExecutor({} as Pool, repo, {} as SourceRegistry, {} as StorageProvider, ids.staff,
      new AsyncSemaphore(24, 'demand-aware-downloads'), 4,
      new AsyncSemaphore(48, 'demand-aware-buffers'), new HostRateLimiter(2), 12);
    executor.setFairSourceDemandCounts(new Map([
      ['manhastro', 44], ['mangaflix', 4], ['taimumangas', 4], ['nexus', 2], ['mangotoons', 1],
    ]));
    const demand = { manhastro:44, mangaflix:4, taimumangas:4, nexus:2, mangotoons:1 };
    const capacities = Object.fromEntries(Object.keys(demand).map(source => [source, (executor as any).sourceSlot(source).capacity]));
    for (const [source, count] of Object.entries(demand)) {
      expect(capacities[source]).toBeGreaterThanOrEqual(1);
      expect(capacities[source]).toBeLessThanOrEqual(count);
    }
    expect(Object.values(capacities).reduce((sum: number, count: any) => sum + Number(count), 0)).toBeLessThanOrEqual(12);
    expect(capacities.mangotoons).toBe(1);
  });

  it('re-samples a stale singleton source before it can take a long-source burst', async () => {
    const queue = { yieldForSourceFairness: vi.fn(async () => true) } as unknown as LiteRepository;
    let executor: LiteExecutor;
    const refresh = vi.fn(async () => {
      executor.setFairSourceDemand(['mangaflix', 'manhastro', 'nexus', 'taimumangas', 'hanamiheaven']);
    });
    executor = new LiteExecutor({} as Pool, queue, {} as SourceRegistry, {} as StorageProvider, ids.staff,
      new AsyncSemaphore(24, 'burst-fair-downloads'), 4, new AsyncSemaphore(48, 'burst-fair-buffers'),
      new HostRateLimiter(2), 12, undefined, undefined, 30_000, refresh);
    executor.setFairSourceDemand(['mangaflix']);
    const mangaFlix = (executor as any).sourceSlot('mangaflix') as AsyncSemaphore;
    expect([mangaFlix.tryAcquire(), mangaFlix.tryAcquire(), mangaFlix.tryAcquire()]).toEqual([true, true, true]);

    try {
      const permit = await (executor as any).acquireSourcePermit({ priority: 'P1' }, 'burst-fair-worker', 'mangaflix');
      expect(permit).not.toBeNull();
      expect(refresh).toHaveBeenCalledTimes(1);
      expect((queue as any).yieldForSourceFairness).not.toHaveBeenCalled();
      expect(executor.getSourceFairnessBurstSnapshot()).toMatchObject({ refreshes: 1, demandSources: 5, refreshInFlight: false });
      expect(mangaFlix.capacity).toBe(6);
      expect(mangaFlix.capacity).toBeLessThan(executor.getEffectiveChapterAdmissionCap());
      permit.release();
    } finally {
      mangaFlix.release(); mangaFlix.release(); mangaFlix.release();
    }
  });

  it('refreshes source demand before the first P1 claim after a P0-only wave', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'priority-fairness-test',
        QUEUE_LEASE_DURATION_SECONDS: 60, DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, LITE_CHAPTER_WORKERS: 5, MAX_CONCURRENT_CHAPTERS: 5,
        TESTED_CONCURRENCY_CEILING: 5 } as any,
    );
    const claims = [
      { id: 'p0', taskType: 'IMPORT_CHAPTER', priority: 'P0' },
      { id: 'p1', taskType: 'IMPORT_CHAPTER', priority: 'P1' },
    ];
    const readyImportSourceDemand = vi.fn()
      .mockResolvedValueOnce(new Map([['mangaflix', 1]]))
      .mockResolvedValueOnce(new Map([['mangaflix', 1], ['manhastro', 1], ['nexus', 1]]));
    (runtime as any).queue = {
      claim: vi.fn(async () => claims.shift() || null),
      readyImportSourceDemand,
    };
    (runtime as any).isRuntimeLeader = true;

    await (runtime as any).claimNext('priority-fairness-worker');
    await (runtime as any).claimNext('priority-fairness-worker');

    expect(readyImportSourceDemand).toHaveBeenCalledTimes(2);
    expect((runtime as any).executor.getFairSourceDemand()).toEqual(['mangaflix', 'manhastro', 'nexus']);
    const mangaFlix = (runtime as any).executor.sourceSlot('mangaflix') as AsyncSemaphore;
    // Each source has one executable frontier, so the remaining two chapter
    // seats are not reserved against work that cannot run concurrently.
    expect(mangaFlix.capacity).toBe(1);
  });

  it('retains the last source-fairness view while live YSQL work is queued', async () => {
    const pool = Object.assign(poolFor(db), { waitingCount: 1 }) as Pool & { waitingCount: number };
    const runtime = new ImporterLiteRuntime(
      pool, {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'fairness-busy-pool-test',
        QUEUE_LEASE_DURATION_SECONDS: 60, DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, LITE_CHAPTER_WORKERS: 5, MAX_CONCURRENT_CHAPTERS: 5,
        TESTED_CONCURRENCY_CEILING: 5 } as any,
    );
    const readyImportSourceDemand = vi.fn(async () => new Map([['mangaflix', 1], ['manhastro', 1]]));
    (runtime as any).queue = { readyImportSourceDemand };
    (runtime as any).executor.setFairSourceDemand(['mangaflix', 'manhastro']);

    await expect((runtime as any).refreshSourceFairness()).resolves.toBe(false);
    expect(readyImportSourceDemand).not.toHaveBeenCalled();
    expect((runtime as any).executor.getFairSourceDemand()).toEqual(['mangaflix', 'manhastro']);
  });

  it('refreshes fairness through an idle isolated metadata pool while primary claims wait', async () => {
    const pool = Object.assign(poolFor(db), { waitingCount: 1 }) as Pool & { waitingCount: number };
    const metadataPool = Object.assign(poolFor(db), { waitingCount: 0, idleCount: 1, totalCount: 1 }) as Pool;
    const runtime = new ImporterLiteRuntime(
      pool, {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', WORKER_ID: 'fairness-isolated-pool-test',
        QUEUE_LEASE_DURATION_SECONDS: 60, DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, LITE_CHAPTER_WORKERS: 5, MAX_CONCURRENT_CHAPTERS: 5,
        TESTED_CONCURRENCY_CEILING: 5 } as any,
      new HostRateLimiter(2), metadataPool,
    );
    const readyImportSourceDemand = vi.fn(async () => new Map([['mangaflix', 2], ['manhastro', 2]]));
    (runtime as any).queue = { readyImportSourceDemand };

    await expect((runtime as any).refreshSourceFairness()).resolves.toBe(true);
    expect(readyImportSourceDemand).toHaveBeenCalledWith(128, metadataPool);
    expect((runtime as any).executor.getFairSourceDemand()).toEqual(['mangaflix', 'manhastro']);
  });

  it('uses the effective admission cap, not configured workers, for source shares', () => {
    const executor = new LiteExecutor({} as Pool, repo, {} as SourceRegistry, {} as StorageProvider, ids.staff,
      new AsyncSemaphore(24, 'effective-cap-downloads'), 4, new AsyncSemaphore(48, 'effective-cap-buffers'), new HostRateLimiter(2), 20);
    executor.setFairSourceDemandCounts(new Map([['mangaflix',100], ['source-b',100]]));
    const mangaFlix = (executor as any).sourceSlot('mangaflix') as AsyncSemaphore;
    expect(mangaFlix.capacity).toBe(6);

    executor.setEffectiveChapterAdmissionCap(3);
    expect(executor.getEffectiveChapterAdmissionCap()).toBe(3);
    expect(mangaFlix.capacity).toBe(2);
    expect([mangaFlix.tryAcquire(), mangaFlix.tryAcquire(), mangaFlix.tryAcquire()]).toEqual([true, true, false]);
    mangaFlix.release();
    mangaFlix.release();
  });

  it('keeps Nexus at one seat only while an alternative ready source can publish', () => {
    const executor = new LiteExecutor({} as Pool, repo, {} as SourceRegistry, {} as StorageProvider, ids.staff,
      new AsyncSemaphore(24, 'nexus-fair-downloads'), 4, new AsyncSemaphore(48, 'nexus-fair-buffers'), new HostRateLimiter(2), 6);
    const nexus = (executor as any).sourceSlot('nexus') as AsyncSemaphore;
    // Scarcity must not penalize the sole healthy source.
    expect(nexus.capacity).toBe(22);

    executor.setFairSourceDemandCounts(new Map([['nexus',1], ['mangaflix',100]]));
    // The cap-six ramp published ten Nexus chapters from two works in one
    // minute while MangaFlix was ready. Its fair seat leaves concurrent room
    // for the alternative without widening global media limits.
    expect(nexus.capacity).toBe(1);

    executor.setFairSourceDemandCounts(new Map([['nexus',1]]));
    expect(nexus.capacity).toBe(22);
  });

  it('yields a source-capped claim without spending a retry attempt', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'yield-source', priority: 'P1', idempotencyKey: 'yield-source' });
    const task = await repo.claim('source-yield-worker', 120);
    expect(task).toBeTruthy();
    expect(task!.attempts).toBe(1);
    expect(await repo.yieldForSourceFairness(task!, 'source-yield-worker', 50)).toBe(true);
    const row = await db.query(`SELECT state,attempts,lease_owner,lease_until,next_run_at>now() AS delayed
      FROM importer_lite_tasks WHERE id=$1::uuid`, [task!.id]);
    expect(row.rows[0]).toMatchObject({ state: 'READY', attempts: 0, lease_owner: null, lease_until: null, delayed: true });
  });

  it('defers sole-route imports for a provider retry boundary so lower executable work is not hidden', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'cooling-source', priority: 'P1', idempotencyKey: 'cooling-source',
      payload: { language: 'pt-BR', edition: 'default' } });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
      VALUES($1::uuid,'cooling-source','source-a','a','cooling-source','pt-BR','default','n|000001|000000|0',10,true)`, [ids.p1a]);
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:still-executable', priority: 'P2', idempotencyKey: 'still-executable' });

    await repo.recordSourceFailure('source-a', { retryAfter: 30, message: 'PAGE_HTTP_429' });

    const deferred = await db.query(`SELECT state,attempts,reason,next_run_at>now() AS delayed
      FROM importer_lite_tasks WHERE idempotency_key='cooling-source'`);
    expect(deferred.rows[0]).toMatchObject({ state: 'RETRY_WAIT', attempts: 0, reason: 'SOURCE_RATE_LIMITED', delayed: true });
    expect((await repo.reconcile(1)).sourceCooldownRecoveries).toBe(0);
    expect((await repo.claim('cooldown-skips-ineligible', 120))?.idempotencyKey).toBe('still-executable');
  });

  it('reopens an expired source cooldown fairly across works and in canonical chapter order', async () => {
    await db.query(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS blocked_reason text, ADD COLUMN IF NOT EXISTS blocked_details jsonb`);
    // Keep generic recovery occupied with an older unrelated retry. The
    // source-specific pass must then admit only its small bounded wave rather
    // than leaving a recovered rate-limited source to wait behind thousands
    // of unrelated retries.
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:older-retry', priority: 'P2',
      idempotencyKey: 'older-retry', nextRunAt: new Date(Date.now() - 60_000) });
    await db.query(`UPDATE importer_lite_tasks SET state='RETRY_WAIT',reason='TRANSIENT',next_run_at=now()-interval '1 minute'
      WHERE idempotency_key='older-retry'`);
    const workIds = [ids.p1a, ids.p1b, ids.publish, ids.staff, ids.p0];
    for (const [workIndex, workId] of workIds.entries()) {
      // One giant work puts more than the old global 48-task head ahead of
      // every other work. The recovery turn must still admit small per-work
      // waves and start each work at its earliest canonical chapter.
      const chapterCount = workIndex === 0 ? 60 : 9;
      for (let chapterIndex = 1; chapterIndex <= chapterCount; chapterIndex++) {
        const chapterKey = `cooldown-recovery-${workIndex}-${chapterIndex}`;
        await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId, workKey: `work:${workId}`,
          chapterKey, priority: 'P1', idempotencyKey: chapterKey,
          payload: { language: 'pt-BR', edition: 'default', orderKey: `n|${String(chapterIndex).padStart(6, '0')}` } });
        await db.query(`INSERT INTO importer_lite_chapter_sources
          (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
          VALUES($1::uuid,$2,'source-a','a',$2,'pt-BR','default',$3,10,true)`,
        [workId, chapterKey, `n|${String(chapterIndex).padStart(6, '0')}`]);
      }
    }
    await repo.recordSourceFailure('source-a', { retryAfter: 30, message: 'PAGE_HTTP_429' });
    await db.query(`UPDATE importer_lite_source_health SET cooldown_until=now()-interval '1 second' WHERE source='source-a'`);
    await db.query(`UPDATE importer_lite_tasks SET next_run_at=now()-interval '1 second',updated_at=now()-interval '1 minute'
      WHERE idempotency_key LIKE 'cooldown-recovery-%'`);

    // An expired cooldown alone is not proof that an operator/provider block
    // such as Turnstile has recovered. Recovery must wait for the explicit
    // source-probe marker instead of repeatedly sending the blocked route.
    await db.query(`UPDATE importer_sources SET status='COOLDOWN',blocked_reason='TURNSTILE',
      cooldown_until=now()-interval '1 second' WHERE id='source-a'`);
    const blocked = await repo.reconcile(1);
    expect(blocked.sourceCooldownRecoveries).toBe(0);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key LIKE 'cooldown-recovery-%' AND state='RETRY_WAIT' AND reason='SOURCE_RATE_LIMITED'`)).rows[0].count).toBe(96);

    await db.query(`UPDATE importer_sources SET blocked_reason=NULL WHERE id='source-a'`);

    const recovered = await repo.reconcile(1);
    expect(recovered.sourceCooldownRecoveries).toBe(8);
    const ready = await db.query(`SELECT work_id,count(*)::int AS count,
        array_agg(payload->>'orderKey' ORDER BY payload->>'orderKey') AS order_keys
      FROM importer_lite_tasks WHERE idempotency_key LIKE 'cooldown-recovery-%' AND state='READY'
      GROUP BY work_id ORDER BY work_id`);
    expect(ready.rows.length).toBeGreaterThanOrEqual(4);
    expect(ready.rows.reduce((sum, row) => sum + Number(row.count), 0)).toBe(8);
    expect(Math.max(...ready.rows.map((row) => Number(row.count)))).toBeLessThanOrEqual(2);
    for (const row of ready.rows) {
      expect(row.order_keys[0]).toBe('n|000001');
      expect(row.order_keys).toEqual([...row.order_keys].sort());
    }
    const remaining = await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key LIKE 'cooldown-recovery-%' AND state='RETRY_WAIT' AND reason='SOURCE_RATE_LIMITED'`);
    expect(remaining.rows[0].count).toBe(88);
  });

  it('does not let an unrouteable cooldown head hide a later healthy gap', async () => {
    await db.query(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS blocked_reason text, ADD COLUMN IF NOT EXISTS blocked_details jsonb`);
    // More than the bounded recovery horizon are older on source-a, but that
    // provider is still externally blocked. They must not prevent a due
    // source-b chapter with a healthy route from returning to READY.
    await db.query(`UPDATE importer_sources SET status='COOLDOWN',blocked_reason='TURNSTILE',
      cooldown_until=now()+interval '1 hour' WHERE id='source-a'`);
    for (let index=0; index<50; index++) {
      const chapterKey = `unrouteable-cooldown-${index}`;
      await repo.enqueue({ taskType:'IMPORT_CHAPTER',workId:ids.p1a,workKey:`cold-head:${index}`,
        chapterKey,priority:'P1',idempotencyKey:chapterKey,
        payload:{language:'pt-BR',edition:'default',orderKey:`n|${String(index).padStart(6,'0')}`} });
      await db.query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter)
        VALUES($1::uuid,$2,'source-a','blocked-head',$2,'pt-BR','default',$3,10,true,true)`,
      [ids.p1a,chapterKey,`n|${String(index).padStart(6,'0')}`]);
    }
    await repo.enqueue({ taskType:'IMPORT_CHAPTER',workId:ids.p1b,workKey:`work:${ids.p1b}`,
      chapterKey:'healthy-cooldown-gap',priority:'P1',idempotencyKey:'healthy-cooldown-gap',
      payload:{language:'pt-BR',edition:'default',orderKey:'n|999999'} });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter)
      VALUES($1::uuid,'healthy-cooldown-gap','source-b','healthy-head','healthy-cooldown-gap','pt-BR','default','n|999999',10,true,true)`, [ids.p1b]);
    await db.query(`UPDATE importer_lite_tasks
      SET state='RETRY_WAIT',reason='SOURCE_RATE_LIMITED',next_run_at=now()-interval '1 minute'
      WHERE idempotency_key LIKE 'unrouteable-cooldown-%' OR idempotency_key='healthy-cooldown-gap'`);

    expect((await repo.reconcile(1)).sourceCooldownRecoveries).toBe(1);
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks
      WHERE idempotency_key='healthy-cooldown-gap'`)).rows[0]).toEqual({ state:'READY', reason:null });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key LIKE 'unrouteable-cooldown-%' AND state='RETRY_WAIT'`)).rows[0].count).toBe(50);
  });

  it('keeps generic recovery live while explicitly deferring only the large source-cooldown sweep', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'deferred-cooldown', priority: 'P1', idempotencyKey: 'deferred-cooldown' });
    await db.query(`UPDATE importer_lite_tasks SET state='RETRY_WAIT',reason='SOURCE_RATE_LIMITED',next_run_at=now()-interval '1 minute'
      WHERE idempotency_key='deferred-cooldown'`);
    const deferred = await repo.reconcile(1, { recoverSourceCooldowns: false });
    expect(deferred.sourceCooldownRecoveries).toBe(0);
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks WHERE idempotency_key='deferred-cooldown'`)).rows[0])
      .toEqual({ state: 'RETRY_WAIT', reason: 'SOURCE_RATE_LIMITED' });
  });

  it('uses the due-only source-cooldown index for the bounded recovery sweep', async () => {
    const query = vi.fn((text: string, values?: unknown[]) => db.query(text, values));
    const instrumentedPool = {
      query,
      connect: async () => ({ query, release: () => undefined }),
    } as unknown as Pool;
    const observed = new LiteRepository(instrumentedPool);
    await observed.reconcile(1, { recoverUploadOutcomes: false, recoverSourceCooldowns: true });
    expect(query).toHaveBeenCalledWith(
      expect.stringContaining('IndexScan(task importer_lite_tasks_source_cooldown_due_idx)'),
      expect.any(Array),
    );
  });

  it('keeps an import ready when a confirmed healthy alternative can satisfy the same chapter', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'fallback-survives-cooldown', priority: 'P1', idempotencyKey: 'fallback-survives-cooldown',
      payload: { language: 'pt-BR', edition: 'default' } });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
      VALUES($1::uuid,'fallback-survives-cooldown','source-a','a','fallback-a','pt-BR','default','n|000001|000000|0',10,true),
            ($1::uuid,'fallback-survives-cooldown','source-b','b','fallback-b','pt-BR','default','n|000001|000000|0',20,true)`, [ids.p1a]);

    await repo.recordSourceFailure('source-a', { retryAfter: 30, message: 'PAGE_HTTP_429' });

    const row = await db.query(`SELECT state,attempts FROM importer_lite_tasks WHERE idempotency_key='fallback-survives-cooldown'`);
    expect(row.rows[0]).toMatchObject({ state: 'READY', attempts: 0 });
    expect((await repo.claim('fallback-remains-executable', 120))?.idempotencyKey).toBe('fallback-survives-cooldown');
  });

  it('yields an actual hot-source import before networking when another ready source has capacity', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'hot-source-import', priority: 'P1', idempotencyKey: 'hot-source-import',
      payload: { language: 'pt-BR', edition: 'default' } });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      chapterKey: 'other-source-import', priority: 'P1', idempotencyKey: 'other-source-import',
      payload: { language: 'pt-BR', edition: 'default' } });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
      VALUES($1::uuid,'hot-source-import','source-a','a','hot-a','pt-BR','default','n|000001|000000|0',10,true),
            ($2::uuid,'other-source-import','source-b','b','other-b','pt-BR','default','n|000001|000000|0',10,true)`, [ids.p1a, ids.p1b]);
    const task = await repo.claim('fair-hot-worker', 120);
    expect(task?.chapterKey).toBe('hot-source-import');
    const adapter = { fetchChapterPages: vi.fn(async () => ['https://source-a.test/page.png']) };
    const executor = new LiteExecutor(poolFor(db) as Pool, repo, { get: () => adapter } as unknown as SourceRegistry,
      {} as StorageProvider, ids.staff, new AsyncSemaphore(2, 'fair-hot-downloads'), 1,
      new AsyncSemaphore(2, 'fair-hot-buffers'), new HostRateLimiter(2), 2);
    executor.setFairSourceDemand(['source-a', 'source-b']);
    const hotSlot = (executor as any).sourceSlot('source-a') as AsyncSemaphore;
    for (let i=0; i<hotSlot.capacity; i++) expect(hotSlot.tryAcquire()).toBe(true);
    try {
      await executor.importChapter(task!, 'fair-hot-worker');
    } finally {
      for (let i=0; i<hotSlot.capacity; i++) hotSlot.release();
    }
    expect(adapter.fetchChapterPages).not.toHaveBeenCalled();
    const row = await db.query(`SELECT state,attempts,lease_owner,next_run_at>now() AS delayed
      FROM importer_lite_tasks WHERE id=$1::uuid`, [task!.id]);
    expect(row.rows[0]).toMatchObject({ state: 'READY', attempts: 0, lease_owner: null, delayed: true });
  });

  it('reserves one saturated source slot for P0 instead of yielding it behind P1 alternatives', async () => {
    const queue = { yieldForSourceFairness: vi.fn(async () => true) } as unknown as LiteRepository;
    const executor = new LiteExecutor({} as Pool, queue, {} as SourceRegistry, {} as StorageProvider, ids.staff,
      new AsyncSemaphore(2, 'p0-source-reserve-downloads'), 1,
      new AsyncSemaphore(2, 'p0-source-reserve-buffers'), new HostRateLimiter(2), 2);
    executor.setFairSourceDemand(['source-a', 'source-b']);
    const hotSlot = (executor as any).sourceSlot('source-a') as AsyncSemaphore;
    for (let i=0; i<hotSlot.capacity; i++) expect(hotSlot.tryAcquire()).toBe(true);

    const waiting = (executor as any).acquireSourcePermit({ priority: 'P0' }, 'p0-worker', 'source-a');
    await vi.waitFor(() => expect(hotSlot.queued).toBe(1));
    expect((queue as any).yieldForSourceFairness).not.toHaveBeenCalled();

    hotSlot.release();
    const permit = await waiting;
    expect(permit.waitMs).toBeGreaterThanOrEqual(0);
    permit.release();
    for (let i=1; i<hotSlot.capacity; i++) hotSlot.release();
  });

  it('yields a second saturated P0 claimant after preserving its one source reservation', async () => {
    const queue = { yieldForSourceFairness: vi.fn(async () => true) } as unknown as LiteRepository;
    const executor = new LiteExecutor({} as Pool, queue, {} as SourceRegistry, {} as StorageProvider, ids.staff,
      new AsyncSemaphore(2, 'p0-second-source-yield-downloads'), 1,
      new AsyncSemaphore(2, 'p0-second-source-yield-buffers'), new HostRateLimiter(2), 2);
    executor.setFairSourceDemand(['source-a', 'source-b']);
    const hotSlot = (executor as any).sourceSlot('source-a') as AsyncSemaphore;
    for (let i=0; i<hotSlot.capacity; i++) expect(hotSlot.tryAcquire()).toBe(true);

    const reserved = (executor as any).acquireSourcePermit({ priority: 'P0' }, 'p0-reserved-worker', 'source-a');
    await vi.waitFor(() => expect(hotSlot.queued).toBe(1));
    const second = (executor as any).acquireSourcePermit({ priority: 'P0' }, 'p0-yield-worker', 'source-a');
    await vi.waitFor(() => expect(hotSlot.queued).toBe(2));
    hotSlot.release();
    const permit = await reserved;
    const yielded = await second;
    expect(yielded).toBeNull();
    expect((queue as any).yieldForSourceFairness).toHaveBeenCalledTimes(1);

    permit.release();
    for (let i=1; i<hotSlot.capacity; i++) hotSlot.release();
  });

  it('reports durable public publication diversity rather than task completion', async () => {
    await db.query(`INSERT INTO importer_publication_events
      (chapter_id,transition_at,recorded_at,bucket_minute,is_fresh_release,source,work_id,priority)
      VALUES
        ('00000000-0000-4000-8000-000000000201',now(),now(),date_trunc('minute',now()),false,'source-a',$1::uuid,2),
        ('00000000-0000-4000-8000-000000000202',now(),now(),date_trunc('minute',now()),false,'source-a',$1::uuid,2),
        ('00000000-0000-4000-8000-000000000203',now(),now(),date_trunc('minute',now()),true,'source-b',$2::uuid,1)`, [ids.p1a, ids.p1b]);
    const snapshot = await repo.publicationDiversitySnapshot();
    expect(snapshot).toMatchObject({
      chapters: 3, attributedChapters: 3, uniqueSources: 2, uniqueWorks: 2,
      topSourceSharePercent: 66.67, topThreeSourcesSharePercent: 100,
      topWorkSharePercent: 66.67, topThreeWorksSharePercent: 100,
    });
  });

  it('retries only catalog tasks blocked by the fixed duplicate canonical batch bug', async () => {
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:duplicate-bug', priority: 'P1',
      idempotencyKey: 'duplicate-bug', maxAttempts: 1, payload: { source: 'source-a', sourceWorkId: 'duplicate-bug' } });
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:unrelated', priority: 'P1',
      idempotencyKey: 'unrelated-bug', maxAttempts: 1, payload: { source: 'source-a', sourceWorkId: 'unrelated' } });
    await db.query(`UPDATE importer_lite_tasks SET state='BLOCKED',attempts=max_attempts,reason=$1
      WHERE idempotency_key='duplicate-bug'`, [DUPLICATE_CANONICAL_BATCH_REASON]);
    await db.query(`UPDATE importer_lite_tasks SET state='BLOCKED',attempts=max_attempts,reason='INVALID_SOURCE_PAYLOAD'
      WHERE idempotency_key='unrelated-bug'`);

    expect(await repo.recoverDuplicateCanonicalCatalogFailures()).toBe(1);
    const rows = await db.query(`SELECT idempotency_key,state,attempts,reason FROM importer_lite_tasks ORDER BY idempotency_key`);
    expect(rows.rows).toEqual([
      { idempotency_key: 'duplicate-bug', state: 'READY', attempts: 0, reason: null },
      { idempotency_key: 'unrelated-bug', state: 'BLOCKED', attempts: 1, reason: 'INVALID_SOURCE_PAYLOAD' },
    ]);
  });

  it('admits ready publication before another same-priority chapter download', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'download', priority: 'P1', idempotencyKey: 'p1-download' });
    await repo.enqueue({ taskType: 'PUBLISH', workId: ids.p1b, workKey: `work:${ids.p1b}`, chapterKey: 'publish', priority: 'P1', idempotencyKey: 'p1-publish' });
    expect((await repo.claim('publisher-first', 30))?.taskType).toBe('PUBLISH');
  });

  it('reapplies the additive migration without changing pre-existing Reader data', async () => {
    // This is deliberately a second application in a disposable database:
    // the deployment procedure must be safe to resume after an interrupted
    // staging run, but this suite never touches a production database.
    await db.query(`INSERT INTO importer_chapter_mappings(source,source_chapter_id,chapter_id)
      VALUES('source-a','legacy-reader-chapter',$1)`, [ids.chapter]);
    await db.exec(migration);
    expect((await db.query(`SELECT title,cover_id FROM works WHERE id=$1`, [ids.publish])).rows[0])
      .toEqual({ title: 'Publish', cover_id: ids.cover });
    expect((await db.query(`SELECT chapter_id FROM importer_chapter_mappings
      WHERE source='source-a' AND source_chapter_id='legacy-reader-chapter'`)).rows[0].chapter_id).toBe(ids.chapter);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks`)).rows[0].count).toBe(0);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_chapter_sources`)).rows[0].count).toBe(0);
  });

  it('claims strict STAFF > P0 > P1 > P2 priority immediately after a panel request', async () => {
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P1', idempotencyKey: 'p1' });
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p0, workKey: `work:${ids.p0}`, priority: 'P0', idempotencyKey: 'p0' });
    await repo.enqueue({ taskType: 'DISCOVER', workKey: 'source:source-a:page:1', priority: 'P2', idempotencyKey: 'p2' });
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.staff, workKey: `work:${ids.staff}`, priority: 'P1', idempotencyKey: 'staff' });
    await db.query(`INSERT INTO importer_staff_requests(work_id,status) VALUES($1,'ACTIVE')`, [ids.staff]);

    const first = await repo.claim('worker-a', 120);
    expect(first?.idempotencyKey).toBe('staff');
    expect(first?.priority).toBe('STAFF');
    await repo.finish(first!, 'worker-a', { state: 'DONE' });

    const second = await repo.claim('worker-a', 120);
    expect(second?.idempotencyKey).toBe('p0');
    await repo.finish(second!, 'worker-a', { state: 'DONE' });
    const third = await repo.claim('worker-a', 120);
    expect(third?.idempotencyKey).toBe('p1');
  });

  it('admits P2 immediately when every higher-priority task is not due', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'later', priority: 'P1', idempotencyKey: 'p1-not-due', nextRunAt: new Date(Date.now() + 60_000) });
    await repo.enqueue({ taskType: 'DISCOVER', workKey: 'source:source-a:eligible', priority: 'P2', idempotencyKey: 'p2-eligible' });

    expect((await repo.claim('p2-worker', 120))?.idempotencyKey).toBe('p2-eligible');
  });

  it('treats duplicate panel requests as one STAFF overlay and never blocks already-running finalization', async () => {
    // These simulate a P0 download, a P1 upload and a P2 publication already
    // beyond admission. A subsequent STAFF request may preempt claims only;
    // it must not create a priority lock around their durable finalization.
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p0, workKey: `work:${ids.p0}`, chapterKey: 'p0-running', priority: 'P0', idempotencyKey: 'p0-running' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'p1-running', priority: 'P1', idempotencyKey: 'p1-running' });
    await repo.enqueue({ taskType: 'PUBLISH', workId: ids.p1b, workKey: `work:${ids.p1b}`, chapterKey: 'p2-running', priority: 'P2', idempotencyKey: 'p2-running' });
    const running = [
      await repo.claim('p0-worker', 120), await repo.claim('p1-worker', 120), await repo.claim('p2-worker', 120),
    ];
    expect(running.map((task) => task?.idempotencyKey).sort()).toEqual(['p0-running', 'p1-running', 'p2-running']);

    await repo.enqueue({ taskType: 'CATALOG', workId: ids.staff, workKey: `work:${ids.staff}`, priority: 'P1', idempotencyKey: 'staff-overlay' });
    // A double-click creates two panel rows in this minimal fixture. It must
    // still produce one durable task and retain STAFF until both are released.
    await db.query(`INSERT INTO importer_staff_requests(work_id,status) VALUES($1,'QUEUED'),($1,'ACTIVE')`, [ids.staff]);
    await Promise.all([
      repo.finish(running[0]!, 'p0-worker', { state: 'DONE' }),
      repo.finish(running[1]!, 'p1-worker', { state: 'DONE' }),
      repo.finish(running[2]!, 'p2-worker', { state: 'DONE' }),
    ]);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks WHERE idempotency_key='staff-overlay'`)).rows[0].count).toBe(1);
    const staff = await repo.claim('staff-worker', 120);
    expect(staff).toMatchObject({ idempotencyKey: 'staff-overlay', priority: 'STAFF' });

    await db.query(`UPDATE importer_staff_requests SET status='CANCELLED' WHERE work_id=$1 AND status='QUEUED'`, [ids.staff]);
    // One active row remains, so finishing a running STAFF task must retain it.
    await repo.finish(staff!, 'staff-worker', { state: 'RETRY_WAIT' });
    expect((await db.query(`SELECT priority FROM importer_lite_tasks WHERE id=$1`, [staff!.id])).rows[0].priority).toBe(0);
    await db.query(`UPDATE importer_staff_requests SET status='CANCELLED' WHERE work_id=$1`, [ids.staff]);
    expect((await db.query(`SELECT priority,base_priority FROM importer_lite_tasks WHERE id=$1`, [staff!.id])).rows[0])
      .toEqual({ priority: 2, base_priority: 2 });
  });

  it('keeps an active STAFF work monitorable without turning its check into a normal claim loop', async () => {
    await db.query(`INSERT INTO importer_staff_requests(work_id,status) VALUES($1,'ACTIVE')`, [ids.staff]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.staff, workKey: `work:${ids.staff}`, priority: 'P1',
      idempotencyKey: 'staff-recurring-catalog', payload: { source: 'source-a', sourceWorkId: 'staff-work' } });
    const catalog = await repo.claim('staff-worker', 120);
    expect(catalog).toMatchObject({ priority: 'STAFF', monitoring: false });
    await repo.finish(catalog!, 'staff-worker', { state: 'RETRY_WAIT', priority: 'P2', monitoring: true, nextRunAt: new Date() });
    await db.query(`UPDATE importer_lite_tasks SET state='READY',next_run_at=now() WHERE id=$1`, [catalog!.id]);
    // The still-active overlay remains STAFF, but its cheap recurring check
    // cannot occupy a chapter slot or become permanently unclaimable.
    expect(await repo.claim('normal-worker', 120)).toBeNull();
    expect(await repo.claimMonitor('monitor-worker', 120)).toMatchObject({ id: catalog!.id, priority: 'STAFF', monitoring: true });
  });

  it('reserves recurring P0 monitoring for one lightweight lane while normal P1/P2 work advances', async () => {
    await repo.enqueueMany(Array.from({ length: 1300 }, (_, index) => ({
      taskType: 'CATALOG' as const, workKey: `source:source-a:watch:${index}`, priority: 'P2' as const,
      idempotencyKey: `watch:${index}`, nextRunAt: new Date(), monitoring: true,
      payload: { source: 'source-a', sourceWorkId: `watch-${index}` },
    })));
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'p1-healthy', priority: 'P1', idempotencyKey: 'p1-healthy' });
    await repo.enqueue({ taskType: 'DISCOVER', workKey: 'source:source-b:page:1', priority: 'P2', idempotencyKey: 'p2-healthy' });

    // A normal worker does not scan or consume any monitor rows.
    expect((await repo.claim('chapter-worker', 120))?.idempotencyKey).toBe('p1-healthy');
    const monitor = await repo.claimMonitor('p0-monitor', 120);
    expect(monitor).toMatchObject({ taskType: 'CATALOG', monitoring: true, priority: 'P2' });
    expect((await repo.claim('discovery-worker', 120))?.idempotencyKey).toBe('p2-healthy');
  });

  it('enrols an existing published ongoing mapping into the isolated P0 monitor lane', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
      VALUES('00000000-0000-4000-8000-000000000111','source-a','coverage-work-a',$1,'ACTIVE')`, [ids.p1a]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      chapterKey: 'p1-stays-runnable', priority: 'P1', idempotencyKey: 'p1-stays-runnable' });

    expect(await repo.refillP0MonitorCoverage(8)).toEqual({ source: 'source-a', admitted: 1, revived: 0 });
    const watch = await db.query(`SELECT monitoring,priority,base_priority,payload->>'source' AS source,
      payload->>'sourceWorkId' AS source_work_id
      FROM importer_lite_tasks WHERE idempotency_key='lite:p0-monitor:source-a:coverage-work-a'`);
    expect(watch.rows[0]).toEqual({ monitoring: true, priority: 3, base_priority: 3,
      source: 'source-a', source_work_id: 'coverage-work-a' });
    // The new watch has no ready-work projection, so P1 keeps its normal
    // claim capacity while the dedicated monitor consumer owns observation.
    expect((await repo.claim('p1-worker', 120))?.idempotencyKey).toBe('p1-stays-runnable');
    expect((await repo.claimMonitor('p0-monitor', 120))?.idempotencyKey)
      .toBe('lite:p0-monitor:source-a:coverage-work-a');
  });

  it('rotates P0 coverage enrollment by source instead of filling a hot provider first', async () => {
    await db.query(`UPDATE works SET published=true WHERE id IN ($1,$2)`, [ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000112','source-a','coverage-source-a',$1,'ACTIVE'),
      ('00000000-0000-4000-8000-000000000113','source-b','coverage-source-b',$2,'ACTIVE')`, [ids.p1a, ids.p1b]);

    expect(await repo.refillP0MonitorCoverage(1)).toEqual({ source: 'source-a', admitted: 1, revived: 0 });
    expect(await repo.refillP0MonitorCoverage(1)).toEqual({ source: 'source-b', admitted: 1, revived: 0 });
    expect((await db.query(`SELECT last_source FROM importer_lite_metadata_source_cursor
      WHERE cursor_name='p0_monitor_coverage'`)).rows).toEqual([{ last_source: 'source-b' }]);
  });

  it('advances a durable per-source coverage cursor instead of rescanning covered mappings', async () => {
    await db.query(`UPDATE importer_sources SET enabled=false WHERE id='source-b'`);
    // The shared production settings table has no updated_at column.  The
    // P0 coverage cursor is durable state, so this legacy schema must remain
    // fully supported instead of failing the background monitor refill.
    await db.query(`ALTER TABLE settings DROP COLUMN updated_at`);
    const workIds = Array.from({ length: 9 }, (_, index) =>
      `00000000-0000-4000-8000-${String(1200 + index).padStart(12, '0')}`,
    );
    for (const [index, workId] of workIds.entries()) {
      const sourceWorkId = `coverage-cursor-${String(index + 1).padStart(2, '0')}`;
      await db.query(`INSERT INTO works(id,title,slug,cover_id,published)
        VALUES($1::uuid,$2,$3,$4::uuid,true)`, [workId, `Cursor ${index}`, `cursor-${index}`, ids.cover]);
      await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
        VALUES($1::uuid,'source-a',$2,$3::uuid,'ACTIVE')`, [
        `00000000-0000-4000-8000-${String(1300 + index).padStart(12, '0')}`, sourceWorkId, workId,
      ]);
      if (index < 8) {
        await repo.enqueue({ taskType: 'CATALOG', workId, workKey: `work:${workId}`, priority: 'P2', monitoring: true,
          idempotencyKey: `lite:p0-monitor:source-a:${sourceWorkId}`,
          payload: { source: 'source-a', sourceWorkId, p0MonitorCoverage: true } });
      }
    }

    // The first 8-row inspection window is already covered. It advances the
    // durable mapping cursor rather than repeatedly joining those same rows.
    expect(await repo.refillP0MonitorCoverage(1)).toEqual({ source: 'source-a', admitted: 0, revived: 0 });
    expect((await db.query(`SELECT value FROM settings
      WHERE key='p0_monitor_coverage_mapping:source-a'`)).rows[0].value).toBe('coverage-cursor-08');

    // The next turn resumes at row 9 and enrols it without a global scan.
    expect(await repo.refillP0MonitorCoverage(1)).toEqual({ source: 'source-a', admitted: 1, revived: 0 });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key='lite:p0-monitor:source-a:coverage-cursor-09'`)).rows[0].count).toBe(1);
  });

  it('turns a genuinely new chapter observed by an enrolled watch into P0 while P1 remains separate', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
      VALUES('00000000-0000-4000-8000-000000000116','source-a','coverage-release',$1,'ACTIVE')`, [ids.p1a]);
    await repo.refillP0MonitorCoverage(8);
    const watch = await repo.claimMonitor('coverage-monitor', 120);
    expect(watch).toMatchObject({ monitoring: true, payload: { source: 'source-a', sourceWorkId: 'coverage-release' } });
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => ({
        id: 'source-a',
        fetchWorkSnapshot: async () => ({
          details: { sourceWorkId: 'coverage-release', title: 'P1 A', slug: 'p1-a', status: 'ONGOING' as const, language: 'pt-BR' },
          chapters: [{ sourceChapterId: 'coverage-release-2', number: 2, displayNumber: '2', title: 'New release' }],
        }),
      }) } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'CONFIRMED_EXISTING' }) };

    await (runtime as any).catalog(watch, 'coverage-monitor');

    expect((await db.query(`SELECT priority,base_priority FROM importer_lite_tasks
      WHERE idempotency_key='lite:chapter:' || $1 || ':pt-br|default|chapter|2|-'`, [ids.p1a])).rows[0])
      .toEqual({ priority: 1, base_priority: 1 });
    expect((await db.query(`SELECT monitoring,state FROM importer_lite_tasks WHERE id=$1`, [watch!.id])).rows[0])
      .toMatchObject({ monitoring: true, state: 'RETRY_WAIT' });
  });

  it('leaves a pending staff catalog request untouched until it completes', async () => {
    await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
      VALUES('00000000-0000-4000-8000-000000000120','source-a','staff-coverage',$1,'ACTIVE')`, [ids.p1a]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P2',
      idempotencyKey: 'lite:catalog:source-a:staff-coverage', payload: { source: 'source-a', sourceWorkId: 'staff-coverage' } });
    await db.query(`UPDATE importer_lite_tasks SET priority=0 WHERE idempotency_key='lite:catalog:source-a:staff-coverage'`);
    expect(await repo.refillP0MonitorCoverage(8)).toEqual({ source: 'source-a', admitted: 0, revived: 0 });
    expect((await db.query(`SELECT state,priority,monitoring FROM importer_lite_tasks WHERE idempotency_key='lite:catalog:source-a:staff-coverage'`)).rows[0])
      .toEqual({state:'READY',priority:0,monitoring:false});
  });

  it('does not demote a pending P1 catalog to a P2 monitor', async () => {
    await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
      VALUES('00000000-0000-4000-8000-000000000121','source-a','p1-coverage',$1,'ACTIVE')`, [ids.p1a]);
    await repo.enqueue({ taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P1',
      idempotencyKey:'lite:catalog:source-a:p1-coverage',payload:{source:'source-a',sourceWorkId:'p1-coverage'} });
    expect(await repo.refillP0MonitorCoverage(8)).toEqual({source:'source-a',admitted:0,revived:0});
    expect((await db.query(`SELECT state,priority,monitoring FROM importer_lite_tasks WHERE idempotency_key='lite:catalog:source-a:p1-coverage'`)).rows[0])
      .toEqual({state:'READY',priority:2,monitoring:false});
  });

  it('enrols a separate monitor without clearing a blocked catalog diagnostic', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
      VALUES('00000000-0000-4000-8000-000000000119','source-a','blocked-coverage',$1,'ACTIVE')`, [ids.p1a]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P2',
      idempotencyKey: 'lite:catalog:source-a:blocked-coverage', payload: { source: 'source-a', sourceWorkId: 'blocked-coverage' } });
    await db.query(`UPDATE importer_lite_tasks SET state='BLOCKED',reason='SOURCE_ERROR' WHERE idempotency_key='lite:catalog:source-a:blocked-coverage'`);
    expect(await repo.refillP0MonitorCoverage(8)).toEqual({ source: 'source-a', admitted: 1, revived: 0 });
    const rows = (await db.query(`SELECT idempotency_key,state,monitoring,reason FROM importer_lite_tasks WHERE work_id=$1 ORDER BY idempotency_key`, [ids.p1a])).rows;
    expect(rows).toEqual([
      { idempotency_key: 'lite:catalog:source-a:blocked-coverage', state: 'BLOCKED', monitoring: false, reason: 'SOURCE_ERROR' },
      { idempotency_key: 'lite:p0-monitor:source-a:blocked-coverage', state: 'READY', monitoring: true, reason: null },
    ]);
  });

  it('revives only a completed first catalog pass and never races an active normal catalog task', async () => {
    await db.query(`UPDATE works SET published=true WHERE id IN ($1,$2)`, [ids.p1a, ids.p1b]);
    await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000114','source-a','coverage-revive',$1,'ACTIVE'),
      ('00000000-0000-4000-8000-000000000115','source-b','coverage-active',$2,'ACTIVE')`, [ids.p1a, ids.p1b]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P1',
      idempotencyKey: 'lite:catalog:source-a:coverage-revive', payload: { source: 'source-a', sourceWorkId: 'coverage-revive' } });
    await db.query(`UPDATE importer_lite_tasks SET state='DONE' WHERE idempotency_key='lite:catalog:source-a:coverage-revive'`);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1b, workKey: `work:${ids.p1b}`, priority: 'P1',
      idempotencyKey: 'lite:catalog:source-b:coverage-active', payload: { source: 'source-b', sourceWorkId: 'coverage-active' } });
    await db.query(`UPDATE importer_lite_tasks SET state='RUNNING',lease_owner='fixture',lease_until=now()+interval '2 minutes' WHERE idempotency_key='lite:catalog:source-b:coverage-active'`);

    expect(await repo.refillP0MonitorCoverage(8)).toEqual({ source: 'source-a', admitted: 0, revived: 1 });
    expect((await db.query(`SELECT state,monitoring,work_key FROM importer_lite_tasks
      WHERE idempotency_key='lite:catalog:source-a:coverage-revive'`)).rows[0])
      .toEqual({ state: 'READY', monitoring: true, work_key: `work:${ids.p1a}` });
    // The next source has a normal catalog task in flight; coverage must not
    // create a duplicate source snapshot beside it.
    expect(await repo.refillP0MonitorCoverage(8)).toEqual({ source: 'source-b', admitted: 0, revived: 0 });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE payload->>'source'='source-b' AND payload->>'sourceWorkId'='coverage-active'`)).rows[0].count).toBe(1);
  });

  it('rotates the one-seat P0 monitor lane across due sources instead of draining a hot source first', async () => {
    await db.query(`INSERT INTO importer_sources (id) VALUES ('source-c') ON CONFLICT DO NOTHING`);
    await repo.enqueueMany([
      ...Array.from({ length: 12 }, (_, index) => ({
        taskType: 'CATALOG' as const, workKey: `source:source-a:watch:${index}`, priority: 'P2' as const,
        idempotencyKey: `source-a-watch:${index}`, nextRunAt: new Date(), monitoring: true,
        payload: { source: 'source-a', sourceWorkId: `source-a-watch-${index}` },
      })),
      ...['source-b', 'source-c'].map((source) => ({
        taskType: 'CATALOG' as const, workKey: `source:${source}:watch`, priority: 'P2' as const,
        idempotencyKey: `${source}-watch`, nextRunAt: new Date(), monitoring: true,
        payload: { source, sourceWorkId: `${source}-watch` },
      })),
    ]);

    const first = await repo.claimMonitor('p0-monitor-a', 120);
    expect(first?.payload.source).toBe('source-a');
    await repo.finish(first!, 'p0-monitor-a', { state: 'DONE' });

    const second = await repo.claimMonitor('p0-monitor-b', 120);
    expect(second?.payload.source).toBe('source-b');
    await repo.finish(second!, 'p0-monitor-b', { state: 'DONE' });

    const third = await repo.claimMonitor('p0-monitor-c', 120);
    expect(third?.payload.source).toBe('source-c');
    expect((await db.query(`SELECT last_source FROM importer_lite_metadata_source_cursor
      WHERE cursor_name='p0_monitor'`)).rows).toEqual([{ last_source: 'source-c' }]);
  });

  it('enrols only owned confirmed alternate mappings and checks them when the origin is disabled', async () => {
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000351','source-a','origin',$1,'SYNCED'),
      ('00000000-0000-4000-8000-000000000352','source-b','alternate',$1,'SYNCED'),
      ('00000000-0000-4000-8000-000000000353','source-b','similar-other',$2,'SYNCED')`, [ids.p1a, ids.p1b]);
    await db.query(`UPDATE importer_sources SET enabled=false WHERE id='source-a'`);
    const mappings = [{source:'source-a',sourceWorkId:'origin'}, {source:'source-b',sourceWorkId:'alternate'},
      {source:'source-b',sourceWorkId:'similar-other'}];
    expect(await repo.enrollConfirmedP0Mappings(ids.p1a,mappings)).toBe(1);
    expect(await repo.enrollConfirmedP0Mappings(ids.p1a,mappings)).toBe(0);
    await db.query(`UPDATE importer_lite_tasks SET next_run_at=now()-interval '1 second' WHERE idempotency_key='lite:p0-monitor:source-b:alternate'`);
    const watch = await repo.claimMonitor('alternate-worker',120);
    expect(watch).toMatchObject({workId:ids.p1a,monitoring:true,payload:{source:'source-b',sourceWorkId:'alternate'}});
    expect((await db.query(`SELECT count(*)::int AS n FROM importer_lite_tasks WHERE idempotency_key LIKE '%similar-other'`)).rows[0].n).toBe(0);
  });

  it('claims a bounded source batch once and rotates to the next source', async () => {
    await repo.enqueueMany(['source-a', 'source-a', 'source-a', 'source-b'].map((source, i) => ({
      taskType: 'CATALOG' as const, workKey: `source:${source}:batch:${i}`, priority: 'P2' as const,
      idempotencyKey: `p0-batch:${i}`, monitoring: true, payload: { source, sourceWorkId: `work-${i}` },
    })));
    const batch = await repo.claimMonitorBatch('batch-worker', 120, 2);
    expect(batch).toHaveLength(2);
    expect(batch.every((task) => task.payload.source === 'source-a')).toBe(true);
    expect(new Set(batch.map((task) => task.id)).size).toBe(2);
    expect((await repo.claimMonitorBatch('next-worker', 120, 4)).map((task) => task.payload.source)).toEqual(['source-b']);
    expect(await repo.finish(batch[0], 'wrong-owner', { state: 'DONE' })).toBe(false);
    expect(await repo.finish(batch[0], 'batch-worker', { state: 'DONE' })).toBe(true);
  });

  it('batches unchanged successes while fencing expired or wrong-owner leases', async () => {
    await repo.enqueueMany([0,1].map((i)=>({taskType:'CATALOG' as const,workId:ids.p1a,workKey:`batch-finish-${i}`,
      idempotencyKey:`batch-finish-${i}`,priority:'P2' as const,monitoring:true,
      payload:{source:'source-a',sourceWorkId:`batch-finish-${i}`}})));
    const batch=await repo.claimMonitorBatch('owner',120,2);
    expect(batch).toHaveLength(2);
    const done=await repo.finishUnchangedMonitors(batch.map((task,i)=>({task,workerId:i?'wrong':'owner',
      nextRunAt:new Date(Date.now()+1_800_000),checkpoint:{lastCheckedAt:'2026-10-31T23:00:00.000Z'}})));
    expect(done).toEqual(new Set([batch[0].id]));
    const rows=await db.query(`SELECT id::text,state,checkpoint->>'lastCheckedAt' checked FROM importer_lite_tasks WHERE id=ANY($1::uuid[])`,[batch.map(t=>t.id)]);
    expect(rows.rows.find((r:any)=>r.id===batch[0].id)).toMatchObject({state:'RETRY_WAIT',checked:'2026-10-31T23:00:00.000Z'});
    expect(rows.rows.find((r:any)=>r.id===batch[1].id)).toMatchObject({state:'RUNNING',checked:null});
  });

  it('can favor the oldest source head while retaining normal round-robin turns', async () => {
    await repo.enqueueMany([
      ...Array.from({length: 3}, (_, i) => ({taskType:'CATALOG' as const, workKey:`a-old-${i}`,
        idempotencyKey:`a-old-${i}`, priority:'P2' as const, monitoring:true,
        nextRunAt:new Date(Date.now()-3_600_000), payload:{source:'source-a',sourceWorkId:`old-${i}`}})),
      {taskType:'CATALOG' as const,workKey:'b-new',idempotencyKey:'b-new',priority:'P2' as const,
        monitoring:true,nextRunAt:new Date(Date.now()-1_000),payload:{source:'source-b',sourceWorkId:'new'}},
    ]);
    expect((await repo.claimMonitorBatch('first',120,1))[0].payload.source).toBe('source-a');
    expect((await repo.claimMonitorBatch('oldest',120,1,true))[0].payload.source).toBe('source-a');
    expect((await repo.claimMonitorBatch('fair',120,1))[0].payload.source).toBe('source-b');
  });

  it('reads the established P0 cursor without rewriting it for every monitor claim', async () => {
    const queries: string[] = [];
    const trackedQuery = async (text: string, values?: unknown[]) => {
      queries.push(text);
      return db.query(text, values);
    };
    const trackedPool = {
      query: trackedQuery,
      connect: async () => ({ query: trackedQuery, release: () => undefined }),
    } as unknown as Pool;
    const trackedRepo = new LiteRepository(trackedPool);
    await trackedRepo.enqueueMany(['source-a', 'source-b'].map((source) => ({
      taskType: 'CATALOG' as const, workKey: `source:${source}:watch`, priority: 'P2' as const,
      idempotencyKey: `cursor-read:${source}`, nextRunAt: new Date(), monitoring: true,
      payload: { source, sourceWorkId: `${source}-watch` },
    })));

    const first = await trackedRepo.claimMonitor('cursor-monitor-a', 120);
    expect(first).not.toBeNull();
    await trackedRepo.finish(first!, 'cursor-monitor-a', { state: 'DONE' });
    queries.length = 0;

    const second = await trackedRepo.claimMonitor('cursor-monitor-b', 120);
    expect(second).not.toBeNull();
    expect(queries.some((query) => query.includes('INSERT INTO importer_lite_metadata_source_cursor'))).toBe(false);
  });

  it('recovers an interrupted P0 monitor through the bounded lease reconciler', async () => {
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:source-a:restart-watch', priority: 'P2', monitoring: true,
      idempotencyKey: 'restart-watch', payload: { source: 'source-a', sourceWorkId: 'restart-watch' } });
    const running = await repo.claimMonitor('monitor-before-restart', 30);
    expect(running).toMatchObject({ monitoring: true, state: 'RUNNING' });
    await db.query(`UPDATE importer_lite_tasks SET lease_until=now()-interval '1 second' WHERE id=$1`, [running!.id]);
    expect((await repo.reconcile(10)).leases).toBe(1);
    await db.query(`UPDATE importer_lite_tasks SET next_run_at=now()-interval '1 second' WHERE id=$1`, [running!.id]);
    expect((await repo.reconcile(10)).retries).toBe(1);
    expect(await repo.claim('chapter-worker', 30)).toBeNull();
    expect((await repo.claimMonitor('monitor-after-restart', 30))?.id).toBe(running!.id);
  });

  it('keeps one failed P0 observation recoverable at a capped cadence without blocking normal work', async () => {
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:source-a:long-outage', priority: 'P2', monitoring: true,
      idempotencyKey: 'long-outage-watch', payload: { source: 'source-a', sourceWorkId: 'long-outage' } });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'healthy-during-p0-outage', priority: 'P1', idempotencyKey: 'healthy-during-p0-outage' });
    await db.query(`UPDATE importer_lite_tasks SET attempts=max_attempts WHERE idempotency_key='long-outage-watch'`);
    const watch = await repo.claimMonitor('p0-monitor', 30);
    const adapter = { id: 'source-a', fetchWorkSnapshot: vi.fn(async () => { throw new Error('UPSTREAM_TIMEOUT'); }) };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    await (runtime as any).handle(watch, 'p0-monitor');
    expect((await db.query(`SELECT state,monitoring,attempts,reason FROM importer_lite_tasks WHERE id=$1`, [watch!.id])).rows[0])
      .toMatchObject({ state: 'RETRY_WAIT', monitoring: true, attempts: 9, reason: 'UPSTREAM_TIMEOUT' });
    // The outage remains an observation problem, not an execution barrier.
    expect((await repo.claim('p1-worker', 30))?.idempotencyKey).toBe('healthy-during-p0-outage');
    await db.query(`UPDATE importer_lite_tasks SET next_run_at=now()-interval '1 second' WHERE id=$1`, [watch!.id]);
    expect((await repo.reconcile(10)).exhausted).toBe(0);
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [watch!.id])).rows[0].state).toBe('READY');
    expect((await repo.claimMonitor('recovered-p0-monitor', 30))?.id).toBe(watch!.id);
  });

  it('schedules eligible P0 watches within 30 minutes after successful checks', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    const interval = (checkpoint: Record<string, unknown>, status: string, releases: number) =>
      (runtime as any).monitorIntervalMs({ checkpoint }, status, releases);
    expect(interval({}, 'ONGOING', 1)).toBe(30 * 60_000);
    expect(interval({ emptyChecks: 0 }, 'ONGOING', 0)).toBe(30 * 60_000);
    expect(interval({ emptyChecks: 5 }, 'ONGOING', 0)).toBe(30 * 60_000);
    expect(interval({}, 'HIATUS', 0)).toBe(30 * 60_000);
    expect(interval({}, 'COMPLETED', 0)).toBe(30 * 60_000);
    const lastWeek = new Date(Date.now() - 7 * 86400_000).toISOString();
    expect(interval({ lastReleaseAt: lastWeek }, 'ONGOING', 0)).toBe(30 * 60_000);
  });

  it('fails closed before Lite acquires leadership when the Reader still uses numeric identity', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    await expect((runtime as any).assertReaderCanonicalIdentitySchema()).resolves.toBeUndefined();
    await db.exec(`CREATE UNIQUE INDEX chapters_work_id_number_key ON chapters(work_id,number)`);
    await expect((runtime as any).assertReaderCanonicalIdentitySchema())
      .rejects.toThrow('READER_CANONICAL_IDENTITY_MIGRATION_REQUIRED:legacy_numeric_uniqueness_active');
  });

  it('fences concurrent workers so one durable task has only one claim', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:1', priority: 'P1', idempotencyKey: 'single-claim' });
    const claims = await Promise.all([repo.claim('worker-a', 120), repo.claim('worker-b', 120)]);
    expect(claims.filter(Boolean)).toHaveLength(1);
    expect(claims.find(Boolean)?.idempotencyKey).toBe('single-claim');
    expect((await db.query(`SELECT attempts,state FROM importer_lite_tasks WHERE idempotency_key='single-claim'`)).rows[0])
      .toMatchObject({ attempts: 1, state: 'RUNNING' });
  });

  it('skips in-flight works before the bounded fair-claim probe and reaches another ready work', async () => {
    // Keep the broad historical fixture on its original migration boundary;
    // this test alone verifies the production claim replacement chain.
    await db.exec(multiWorkClaimMigration);
    await db.exec(boundedClaimMigration);
    await db.exec(earliestPendingChapterClaimMigration);
    const activeWorkIds = Array.from({ length: 5 }, (_, index) =>
      `00000000-0000-4000-8000-${String(700 + index).padStart(12, '0')}`,
    );
    for (const [index, workId] of activeWorkIds.entries()) {
      await db.query(`INSERT INTO works(id,title,slug,cover_id) VALUES($1::uuid,$2,$3,$4::uuid)`,
        [workId, `Active ${index}`, `active-${index}`, ids.cover]);
      await repo.enqueue({
        taskType: 'IMPORT_CHAPTER', workId, workKey: `work:${workId}`,
        chapterKey: `active:${index}`, priority: 'P1', idempotencyKey: `active-work-${index}`,
        payload: { orderKey: `n|${String(index).padStart(12, '0')}|000000000000|||` },
      });
      await db.query(`UPDATE importer_lite_tasks SET state='RUNNING',lease_owner='active-fixture',lease_until=now()+interval '5 minutes'
        WHERE idempotency_key=$1`, [`active-work-${index}`]);
    }
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'claimable-after-active', priority: 'P1', idempotencyKey: 'claimable-after-active',
      payload: { orderKey: 'n|000000000999|000000000000|||' },
    });

    const claimed = await repo.claim('bounded-claim-worker', 120);
    expect(claimed?.idempotencyKey).toBe('claimable-after-active');
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_ready_works
      WHERE work_key = ANY($1::text[]) AND next_run_at <= now()`,
      [activeWorkIds.map((workId) => `work:${workId}`)])).rows[0].count).toBe(5);
  });

  it('does not let 1,300 blocked rows hide a ready chapter and fairly rotates ready works', async () => {
    await db.query(`
      INSERT INTO importer_lite_tasks(task_type,work_key,priority,base_priority,state,idempotency_key,reason)
      SELECT 'IMPORT_CHAPTER','blocked-work',2,2,'BLOCKED','blocked-' || n,'SOURCE_UNAVAILABLE'
      FROM generate_series(1,1300) n
    `);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:1', priority: 'P1', idempotencyKey: 'a-1' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:2', priority: 'P1', idempotencyKey: 'a-2' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`, chapterKey: 'chapter:1', priority: 'P1', idempotencyKey: 'b-1' });

    const first = await repo.claim('worker-a', 120);
    expect(['a-1', 'a-2']).toContain(first?.idempotencyKey);
    await repo.finish(first!, 'worker-a', { state: 'DONE' });
    const second = await repo.claim('worker-a', 120);
    expect(second?.idempotencyKey).toBe('b-1');
  });

  it('does not let a 1,300-chapter work monopolize same-priority workers', async () => {
    await db.query(`
      INSERT INTO importer_lite_tasks(task_type,work_id,work_key,chapter_key,priority,base_priority,idempotency_key)
      SELECT 'IMPORT_CHAPTER',$1::uuid,'work:' || $1::text,'bulk:' || n,2,2,'bulk-' || n FROM generate_series(1,1300) n
    `, [ids.p1a]);
    await db.query(`SELECT importer_lite_refresh_ready_work('work:' || $1)`, [ids.p1a]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`, chapterKey: 'chapter:1', priority: 'P1', idempotencyKey: 'small-work' });

    const giant = await repo.claim('worker-a', 120);
    expect(giant?.workId).toBe(ids.p1a);
    await repo.finish(giant!, 'worker-a', { state: 'DONE' });
    expect((await repo.claim('worker-b', 120))?.idempotencyKey).toBe('small-work');
  });

  it('claims same-work manifest repairs in canonical order before retry time when all are due', async () => {
    const entries = [
      { number: '26', key: 'manifest-26', orderKey: 'n|000000000026|000000000000|||chapter%2026' },
      { number: '1', key: 'manifest-1', orderKey: 'n|000000000001|000000000000|||chapter%201' },
      { number: '20', key: 'manifest-20', orderKey: 'n|000000000020|000000000000|||chapter%2020' },
    ];
    for (const entry of entries) {
      await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
        chapterKey: entry.key, priority: 'P1', idempotencyKey: entry.key,
        payload: { displayNumber: entry.number, orderKey: entry.orderKey, manifestP1Repair: true } });
    }
    await db.query(`UPDATE importer_lite_tasks SET created_at='2020-01-01T00:00:00Z',next_run_at=CASE idempotency_key
        WHEN 'manifest-26' THEN now()-interval '60 seconds'
        WHEN 'manifest-20' THEN now()-interval '30 seconds'
        ELSE now()-interval '1 second' END
      WHERE work_id=$1::uuid AND idempotency_key LIKE 'manifest-%'`, [ids.p1a]);

    const claimed: string[] = [];
    for (let i = 0; i < entries.length; i += 1) {
      const task = await repo.claim(`manifest-order-${i}`, 120);
      expect(task).not.toBeNull();
      claimed.push(task!.idempotencyKey);
      await repo.finish(task!, `manifest-order-${i}`, { state: 'DONE' });
    }
    expect(claimed).toEqual(['manifest-1', 'manifest-20', 'manifest-26']);
  });

  it('keeps a deep confirmed chapter backlog ordered without scanning every later predecessor', async () => {
    await db.query(`
      INSERT INTO importer_lite_tasks(task_type,work_id,work_key,chapter_key,priority,base_priority,state,idempotency_key,payload,next_run_at)
      SELECT 'IMPORT_CHAPTER',$1::uuid,'work:' || $1::text,'ordered-bulk:' || n,2,2,'READY','ordered-bulk-' || n,
        jsonb_build_object('orderKey','n|' || lpad(n::text,12,'0') || '|000000000000|||'),now()-interval '1 second'
      FROM generate_series(1,1300) n
    `, [ids.p1a]);
    await db.query(`SELECT importer_lite_refresh_ready_work('work:' || $1)`, [ids.p1a]);

    const first = await repo.claim('ordered-bulk-first', 120);
    expect(first?.idempotencyKey).toBe('ordered-bulk-1');
    await repo.finish(first!, 'ordered-bulk-first', { state: 'DONE' });
    const second = await repo.claim('ordered-bulk-second', 120);
    expect(second?.idempotencyKey).toBe('ordered-bulk-2');
  });

  it('materializes a 1,300-chapter catalog batch in bounded inserts and keeps its queue durable', async () => {
    const result = await repo.enqueueMany(Array.from({ length: 1300 }, (_, index) => ({
      taskType: 'IMPORT_CHAPTER' as const, workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: `batch:${index + 1}`, priority: 'P1' as const, idempotencyKey: `catalog-batch-${index + 1}`,
      payload: { displayNumber: String(index + 1), orderKey: String(index + 1) },
    })));
    expect(result.inserted).toBe(1300);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks WHERE idempotency_key LIKE 'catalog-batch-%'`)).rows[0].count).toBe(1300);
    expect((await repo.claim('worker-a', 120))?.workId).toBe(ids.p1a);
  });

  it('recovers an expired lease and materializes a publish task from completed media without a global scan', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:1', priority: 'P1', idempotencyKey: 'recover' });
    const claimed = await repo.claim('worker-a', 30);
    expect(claimed).not.toBeNull();
    await db.query(`UPDATE importer_lite_tasks SET lease_until=now()-interval '1 second' WHERE id=$1`, [claimed!.id]);
    expect((await repo.reconcile(10)).leases).toBe(1);
    await db.query(`UPDATE importer_lite_tasks SET next_run_at=now()-interval '1 second' WHERE id=$1`, [claimed!.id]);
    expect((await repo.reconcile(10)).retries).toBe(1);
    expect((await repo.claim('worker-b', 30))?.id).toBe(claimed!.id);
    // Release the recovered same-work lease before claiming chapter 2.
    await repo.finish(claimed!, 'worker-b', { state: 'DONE' });

    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:2', priority: 'P1', idempotencyKey: 'needs-publish' });
    const imported = await repo.claim('worker-a', 30);
    await repo.finish(imported!, 'worker-a', { state: 'DONE', checkpoint: { mediaIds: [ids.page] } });
    expect((await repo.reconcile(10)).publishable).toBe(1);
    const publish = await db.query(`SELECT task_type,state FROM importer_lite_tasks WHERE idempotency_key='lite:publish:' || $1 || ':chapter:2'`, [ids.p1a]);
    expect(publish.rows).toEqual([{ task_type: 'PUBLISH', state: 'READY' }]);
  });

  it('does not repeatedly insert a publication already blocked on work presentation during recovery', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'presentation-blocked', priority: 'P1', idempotencyKey: 'presentation-blocked-import' });
    const imported = await repo.claim('worker-a', 30);
    await repo.finish(imported!, 'worker-a', { state: 'DONE', checkpoint: { mediaIds: [ids.page] } });
    expect((await repo.reconcile(10)).publishable).toBe(1);
    await db.query(`UPDATE importer_lite_tasks SET state='BLOCKED',reason='WORK_PRESENTATION_INCOMPLETE'
      WHERE idempotency_key='lite:publish:' || $1 || ':presentation-blocked'`, [ids.p1a]);

    const queries: string[] = [];
    const tracedPool = {
      query: (text: string, values?: unknown[]) => { queries.push(text); return db.query(text, values); },
      connect: async () => ({
        query: (text: string, values?: unknown[]) => { queries.push(text); return db.query(text, values); },
        release: () => undefined,
      }),
    } as unknown as Pool;

    expect((await new LiteRepository(tracedPool).reconcile(10)).publishable).toBe(0);
    expect(queries.some((text) => text.includes('INSERT INTO importer_lite_tasks'))).toBe(false);
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks
      WHERE idempotency_key='lite:publish:' || $1 || ':presentation-blocked'`, [ids.p1a])).rows)
      .toEqual([{ state: 'BLOCKED', reason: 'WORK_PRESENTATION_INCOMPLETE' }]);
  });

  it('fences stale owners from settling or publishing after their lease expires', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:stale', priority: 'P1', idempotencyKey: 'stale-finish' });
    const staleFinish = await repo.claim('old-runtime', 30);
    await db.query(`UPDATE importer_lite_tasks SET lease_until=now()-interval '1 second' WHERE id=$1`, [staleFinish!.id]);
    expect(await repo.finish(staleFinish!, 'old-runtime', { state: 'DONE' })).toBe(false);
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [staleFinish!.id])).rows[0].state).toBe('RUNNING');

    await repo.enqueue({ taskType: 'PUBLISH', workId: ids.publish, workKey: `work:${ids.publish}`, chapterKey: 'chapter:stale-publish', priority: 'P1', idempotencyKey: 'stale-publish' });
    const stalePublish = await repo.claim('old-runtime', 30);
    await db.query(`UPDATE importer_lite_tasks SET lease_until=now()-interval '1 second' WHERE id=$1`, [stalePublish!.id]);
    await expect(repo.publish({ task: stalePublish!, workerId: 'old-runtime', chapterId: ids.chapter,
      workId: ids.publish, mediaIds: [ids.page], fresh: false })).rejects.toThrow('LEASE_LOST_BEFORE_PUBLISH');
    expect((await db.query(`SELECT published_at FROM chapters WHERE id=$1`, [ids.chapter])).rows[0].published_at).toBeNull();
    expect((await db.query(`SELECT count(*)::int AS count FROM pages WHERE chapter_id=$1`, [ids.chapter])).rows[0].count).toBe(0);
  });

  it('uses one deterministic Reader chapter row when two recovered workers race the same canonical identity', async () => {
    const task = {
      id: '00000000-0000-4000-8000-000000000099', taskType: 'IMPORT_CHAPTER', workId: ids.p1a,
      workKey: `work:${ids.p1a}`, chapterKey: 'pt-br|default|chapter|race|-', priority: 'P1', basePriority: 'P1', state: 'RUNNING',
      idempotencyKey: 'canonical-race', nextRunAt: new Date(), attempts: 1, maxAttempts: 8,
      leaseOwner: 'worker-a', leaseUntil: new Date(Date.now() + 60_000), reason: null, monitoring: false,
      payload: { displayNumber: '9', chapterTitle: 'Race Safe', language: 'pt-BR', edition: 'default', orderKey: '9' }, checkpoint: {},
    } as any;
    const executor = new LiteExecutor(poolFor(db) as Pool, repo, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(1, 'test-downloads'));
    const [first, second] = await Promise.all([
      (executor as any).createOrLoadChapter(task),
      (executor as any).createOrLoadChapter(task),
    ]);
    expect(first).toBe(second);
    expect((await db.query(`SELECT count(*)::int AS count FROM chapters WHERE work_id=$1 AND title='Race Safe'`, [ids.p1a])).rows[0].count).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_chapters WHERE work_id=$1 AND canonical_chapter_key=$2`, [ids.p1a, task.chapterKey])).rows[0].count).toBe(1);
  });

  it('fuses fresh Reader chapter materialization with the Lite identity fence', async () => {
    const task = {
      id: '00000000-0000-4000-8000-000000000090', taskType: 'IMPORT_CHAPTER', workId: ids.p1a,
      workKey: `work:${ids.p1a}`, chapterKey: 'pt-br|default|chapter|fused|-', priority: 'P1', basePriority: 'P1', state: 'RUNNING',
      idempotencyKey: 'canonical-fused', nextRunAt: new Date(), attempts: 1, maxAttempts: 8,
      leaseOwner: 'worker-a', leaseUntil: new Date(Date.now() + 60_000), reason: null, monitoring: false,
      payload: { displayNumber: '10', chapterTitle: 'Fused identity', language: 'pt-BR', edition: 'default', orderKey: '10' }, checkpoint: {},
    } as any;
    const queries: string[] = [];
    const tracedPool = {
      query: (text: string, values?: unknown[]) => { queries.push(text); return db.query(text, values); },
      connect: async () => ({
        query: (text: string, values?: unknown[]) => { queries.push(text); return db.query(text, values); },
        release: () => undefined,
      }),
    } as unknown as Pool;
    const executor = new LiteExecutor(tracedPool, repo, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(1, 'test-downloads'));

    await (executor as any).createOrLoadChapter(task, null);

    const materialization = queries.filter((query) => query.includes('INSERT INTO chapters'));
    expect(materialization).toHaveLength(1);
    expect(materialization[0]).toContain('WITH existing_mapping');
    expect(materialization[0]).toContain('INSERT INTO importer_lite_chapters');
    expect(queries.some((query) => /^\s*SELECT chapter_id FROM importer_lite_chapters/i.test(query))).toBe(false);
  });

  it('blocks a Reader numeric collision instead of merging distinct canonical chapters', async () => {
    // Some deployed Reader schemas still constrain a work to one numeric
    // chapter number.  `12.1` and `12.10` are distinct source identities but
    // parse to the same numeric value, so Lite must surface the schema gap
    // rather than point both identities at one Reader row.
    await db.exec(`CREATE UNIQUE INDEX chapters_work_number_reader_compat_idx ON chapters(work_id,number)`);
    const base = {
      id: '00000000-0000-4000-8000-000000000098', taskType: 'IMPORT_CHAPTER', workId: ids.p1a,
      workKey: `work:${ids.p1a}`, priority: 'P1', basePriority: 'P1', state: 'RUNNING',
      idempotencyKey: 'numeric-collision', nextRunAt: new Date(), attempts: 1, maxAttempts: 8,
      leaseOwner: 'worker-a', leaseUntil: new Date(Date.now() + 60_000), reason: null, monitoring: false,
      payload: { displayNumber: '12.1', chapterTitle: 'Same rendered title', language: 'pt-BR', edition: 'default', orderKey: '12.1' }, checkpoint: {},
    } as any;
    const executor = new LiteExecutor(poolFor(db) as Pool, repo, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(1, 'test-downloads'));
    await (executor as any).createOrLoadChapter({ ...base, chapterKey: 'pt-br|default|chapter|12.1|-' });
    await expect((executor as any).createOrLoadChapter({
      ...base, id: '00000000-0000-4000-8000-000000000097', chapterKey: 'pt-br|default|chapter|12.10|-',
      payload: { ...base.payload, displayNumber: '12.10', orderKey: '12.10' },
    })).rejects.toThrow('CHAPTER_NUMBER_COLLISION_REQUIRES_READER_MIGRATION');
    expect((await db.query(`SELECT count(*)::int AS count FROM chapters WHERE work_id=$1 AND title='Same rendered title'`, [ids.p1a])).rows[0].count).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_chapters WHERE work_id=$1`, [ids.p1a])).rows[0].count).toBe(1);
  });

  it('persists textual Reader identities for trailing decimals, specials, and editions', async () => {
    const base = {
      id: '00000000-0000-4000-8000-000000000096', taskType: 'IMPORT_CHAPTER', workId: ids.p1a,
      workKey: `work:${ids.p1a}`, priority: 'P1', basePriority: 'P1', state: 'RUNNING',
      idempotencyKey: 'reader-text-identity', nextRunAt: new Date(), attempts: 1, maxAttempts: 8,
      leaseOwner: 'worker-a', leaseUntil: new Date(Date.now() + 60_000), reason: null, monitoring: false,
      checkpoint: {},
    } as any;
    const executor = new LiteExecutor(poolFor(db) as Pool, repo, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(1, 'test-downloads'));

    await (executor as any).createOrLoadChapter({ ...base,
      chapterKey: 'pt-br|default|chapter|12.1|-',
      payload: { displayNumber: '12.1', chapterTitle: 'Doze ponto um', chapterKind: 'chapter', language: 'pt-BR', edition: 'default', orderKey: 'n|000000000012|100000000000|1||' },
    });
    await (executor as any).createOrLoadChapter({ ...base, id: '00000000-0000-4000-8000-000000000095',
      chapterKey: 'pt-br|default|chapter|12.10|-',
      payload: { displayNumber: '12.10', chapterTitle: 'Doze ponto dez', chapterKind: 'chapter', language: 'pt-BR', edition: 'default', orderKey: 'n|000000000012|100000000000|10||' },
    });
    await (executor as any).createOrLoadChapter({ ...base, id: '00000000-0000-4000-8000-000000000094',
      chapterKey: 'pt-br|default|special|pr%C3%B3logo|pr%C3%B3logo',
      payload: { displayNumber: 'Prólogo', chapterTitle: 'Prólogo', chapterKind: 'special', language: 'pt-BR', edition: 'default', orderKey: 'z|pr%C3%B3logo|pr%C3%B3logo' },
    });
    await (executor as any).createOrLoadChapter({ ...base, id: '00000000-0000-4000-8000-000000000093',
      chapterKey: 'en|official|chapter|12.1|-',
      payload: { displayNumber: '12.1', chapterTitle: 'Twelve point one', chapterKind: 'chapter', language: 'en', edition: 'official', orderKey: 'n|000000000012|100000000000|1||' },
    });

    const rows = await db.query(`SELECT number::text,display_number,canonical_chapter_key,chapter_kind,edition,language
      FROM chapters WHERE work_id=$1 ORDER BY canonical_chapter_key`, [ids.p1a]);
    expect(rows.rows).toEqual(expect.arrayContaining([
      expect.objectContaining({ display_number: '12.1', canonical_chapter_key: 'pt-br|default|chapter|12.1|-', edition: 'default', language: 'pt-BR' }),
      expect.objectContaining({ display_number: '12.10', canonical_chapter_key: 'pt-br|default|chapter|12.10|-' }),
      expect.objectContaining({ display_number: 'Prólogo', canonical_chapter_key: 'pt-br|default|special|pr%C3%B3logo|pr%C3%B3logo', chapter_kind: 'special', number: null }),
      expect.objectContaining({ display_number: '12.1', canonical_chapter_key: 'en|official|chapter|12.1|-', edition: 'official', language: 'en' }),
    ]));
    expect(rows.rows).toHaveLength(4);
  });

  it('reuses an exactly mapped legacy published chapter without rewriting its pages', async () => {
    const key = 'pt-br|default|chapter|legacy-published|-';
    await db.query(`INSERT INTO chapters(id,work_id,number,title,published_at) VALUES($1,$2,11,'Legacy published',now())`, [ids.legacy, ids.p1a]);
    await db.query(`INSERT INTO importer_chapter_mappings(source,source_chapter_id,chapter_id) VALUES('source-a','legacy-source-chapter',$1)`, [ids.legacy]);
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES($1,$2,'source-a','legacy-source-chapter','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'legacy-published',
      payload: { displayNumber: '11', chapterTitle: 'Legacy published', language: 'pt-BR', edition: 'default', orderKey: '11' } });
    const task = await repo.claim('worker-a', 120);
    const adapter = { id: 'source-a', fetchChapterPages: vi.fn(async () => { throw new Error('published chapter must not download'); }) };
    const storage: StorageProvider = { upload: vi.fn(async () => 'must-not-upload'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const legacyLookups: string[] = [];
    const tracedPool = {
      query: (text: string, values?: unknown[]) => {
        if (text.includes('FROM importer_lite_chapter_sources s')) legacyLookups.push(text);
        return db.query(text, values);
      },
      connect: async () => ({
        query: (text: string, values?: unknown[]) => {
          if (text.includes('FROM importer_lite_chapter_sources s')) legacyLookups.push(text);
          return db.query(text, values);
        },
        release: () => undefined,
      }),
    } as unknown as Pool;
    const executor = new LiteExecutor(tracedPool, repo, { get: () => adapter } as unknown as SourceRegistry, storage,
      '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(1, 'test-downloads'));
    await executor.importChapter(task!, 'worker-a');
    expect(adapter.fetchChapterPages).not.toHaveBeenCalled();
    expect(storage.upload).not.toHaveBeenCalled();
    expect(legacyLookups).toHaveLength(1);
    expect((await db.query(`SELECT state,checkpoint->>'publishedAt' AS published_at,checkpoint->>'legacyPublishedReuse' AS reused
      FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toMatchObject({ state: 'DONE', reused: 'true' });
    expect((await db.query(`SELECT chapter_id FROM importer_lite_chapters WHERE work_id=$1 AND canonical_chapter_key=$2`, [ids.p1a, key])).rows[0].chapter_id).toBe(ids.legacy);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks WHERE task_type='PUBLISH' AND work_id=$1 AND chapter_key=$2`, [ids.p1a, key])).rows[0].count).toBe(0);
  });

  it('does not enqueue publication when the import lease expires after page storage', async () => {
    const key = 'pt-br|default|chapter|lease-after-media|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','lease-after-media','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'lease-after-media',
      payload: { displayNumber: '10', chapterTitle: 'Lease after media', language: 'pt-BR', edition: 'default', orderKey: '10' } });
    const task = await repo.claim('worker-a', 120);
    const registry = { get: () => ({ id: 'source-a', fetchChapterPages: async () => ['https://source-a.test/page.png'] }) } as unknown as SourceRegistry;
    const storage: StorageProvider = { upload: async () => 'telegram-file', healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalComplete = repo.completeImportAndEnqueuePublish.bind(repo);
    // Expire after every page receipt is durable but just before the atomic
    // completion fence. The replacement keeps the production path free of a
    // redundant final checkpoint query solely for this test.
    (repo as any).completeImportAndEnqueuePublish = async (...args: any[]) => {
      await db.query(`UPDATE importer_lite_tasks SET lease_until=now()-interval '1 second' WHERE id=$1`, [task!.id]);
      return originalComplete(...args);
    };
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(1, 'test-downloads'), 1);
      await executor.importChapter(task!, 'worker-a');
    } finally {
      (repo as any).completeImportAndEnqueuePublish = originalComplete;
      vi.stubGlobal('fetch', originalFetch);
    }
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0].state).toBe('RUNNING');
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks WHERE task_type='PUBLISH' AND work_id=$1 AND chapter_key=$2`, [ids.p1a, key])).rows[0].count).toBe(0);
    expect((await repo.reconcile(10)).leases).toBe(1);
  });

  it('rolls back Reader writes if publication loses its fence before task settlement', async () => {
    await repo.enqueue({ taskType: 'PUBLISH', workId: ids.publish, workKey: `work:${ids.publish}`, chapterKey: 'chapter:lost-during-publish', priority: 'P1', idempotencyKey: 'lost-during-publish' });
    const task = await repo.claim('publisher', 120);
    const basePool = poolFor(db) as Pool;
    const fencedPool = {
      query: basePool.query.bind(basePool),
      connect: async () => {
        const client = await basePool.connect();
        return {
          ...client,
          query: async (text: string, values?: unknown[]) => {
            if (text.includes("UPDATE importer_lite_tasks t SET state='DONE'")) return { rows: [{ done: 0 }], rowCount: 1 } as any;
            return client.query(text, values);
          },
        };
      },
    } as unknown as Pool;
    const fencedRepository = new LiteRepository(fencedPool);
    await expect(fencedRepository.publish({ task: task!, workerId: 'publisher', chapterId: ids.chapter,
      workId: ids.publish, mediaIds: [ids.page], fresh: true })).rejects.toThrow('LEASE_LOST_DURING_PUBLISH');
    expect((await db.query(`SELECT published_at FROM chapters WHERE id=$1`, [ids.chapter])).rows[0].published_at).toBeNull();
    expect((await db.query(`SELECT count(*)::int AS count FROM pages WHERE chapter_id=$1`, [ids.chapter])).rows[0].count).toBe(0);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_publication_events WHERE chapter_id=$1`, [ids.chapter])).rows[0].count).toBe(0);
  });

  it('uses the shared runtime lease to gate a Lite contender without gating in-flight finalization', async () => {
    const makeRuntime = (workerId: string) => new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { WORKER_ID: workerId, IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    const first = makeRuntime('lite-a');
    const second = makeRuntime('lite-b');
    expect(await (first as any).tryAcquireRuntimeLeadership()).toBe(true);
    expect(await (second as any).tryAcquireRuntimeLeadership()).toBe(false);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:leader', priority: 'P1', idempotencyKey: 'leader-gate' });
    expect(await (second as any).claimNext('standby-worker')).toBeNull();
    // A task already leased by the leader may still complete: runtime
    // leadership is an admission fence, never a global finalization lock.
    const leaderWorker = `${(first as any).runtimeInstanceId}:lite:1`;
    expect(leaderWorker).not.toContain((second as any).runtimeInstanceId);
    const inFlight = await (first as any).claimNext(leaderWorker);
    expect(inFlight?.leaseOwner).toBe(leaderWorker);
    expect(await repo.finish(inFlight!, leaderWorker, { state: 'DONE' })).toBe(true);
    await (first as any).releaseRuntimeLeadership();
    expect(await (second as any).tryAcquireRuntimeLeadership()).toBe(true);
    await (second as any).releaseRuntimeLeadership();
  });

  it('coalesces an overlapping operational snapshot instead of adding diagnostic pool work', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { WORKER_ID: 'snapshot', IMPORTER_USER_ID: ids.staff, DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, TELEGRAM_MEDIA_CONCURRENCY: 1,
        UPLOAD_RATE_LIMIT_BYTES_PER_SEC: 1 } as any,
    );
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const queue = (runtime as any).queue;
    queue.operationalSnapshot = vi.fn(async () => { await gate; return []; });
    queue.progressSnapshot = async () => ({ publishedLastMinute: 0, oldestReadyAt: null, oldestStoredUnpublishedAt: null, nextMonitorAt: null });
    queue.blockedReasonSnapshot = async () => [];
    queue.sourceHealthSnapshot = async () => [];
    queue.publicationDiversitySnapshot = async () => ({ chapters: 0 });

    const first = (runtime as any).emitOperationalSnapshot();
    await new Promise<void>((resolve) => setImmediate(resolve));
    await (runtime as any).emitOperationalSnapshot();
    expect(queue.operationalSnapshot).toHaveBeenCalledTimes(1);
    release();
    await first;
    expect((runtime as any).operationalSnapshotRunning).toBe(false);
  });

  it('does not admit a chapter into a full snapshot that began while idle', () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { WORKER_ID: 'snapshot-admission-fence', IMPORTER_USER_ID: ids.staff,
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, LITE_CHAPTER_WORKERS: 1,
        MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );
    expect((runtime as any).shouldPauseChapterAdmission()).toBe(false);
    (runtime as any).operationalSnapshotRunning = true;
    expect((runtime as any).shouldPauseChapterAdmission()).toBe(true);
  });

  it('does not hold chapter admission while persisting a deferred busy-pool heartbeat', async () => {
    const pool = poolFor(db) as Pool;
    const runtime = new ImporterLiteRuntime(
      pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { WORKER_ID: 'snapshot-busy-pool', IMPORTER_USER_ID: ids.staff,
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, TELEGRAM_MEDIA_CONCURRENCY: 1,
        UPLOAD_RATE_LIMIT_BYTES_PER_SEC: 1 } as any,
    );
    Object.defineProperty(pool, 'waitingCount', { configurable: true, get: () => 1 });
    await (runtime as any).emitOperationalSnapshot();
    expect((runtime as any).operationalSnapshotRunning).toBe(false);
    expect((runtime as any).shouldPauseChapterAdmission()).toBe(false);
  });

  it('uses a compact heartbeat while a chapter admission is active instead of racing its YSQL commits', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { WORKER_ID: 'snapshot-chapter-busy', IMPORTER_USER_ID: ids.staff, DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, TELEGRAM_MEDIA_CONCURRENCY: 1,
        UPLOAD_RATE_LIMIT_BYTES_PER_SEC: 1, LITE_CHAPTER_WORKERS: 1,
        MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );
    const queue = (runtime as any).queue;
    queue.operationalSnapshot = vi.fn();
    queue.progressSnapshot = vi.fn();
    const admission = (runtime as any).chapterAdmissionGate;
    expect(admission.tryAcquire()).toBe(true);
    telemetryCollector.setRuntimeFingerprint({
      gitSha: 'test-git', artifactSha256: 'test-artifact', rolloutRevision: 'r840',
    });

    try {
      await (runtime as any).emitOperationalSnapshot();
      expect(queue.operationalSnapshot).not.toHaveBeenCalled();
      expect(queue.progressSnapshot).not.toHaveBeenCalled();
      const stored = await db.query(`SELECT value FROM settings WHERE key='importer_heartbeat'`);
      const heartbeat = typeof stored.rows[0]?.value === 'string'
        ? JSON.parse(stored.rows[0].value)
        : stored.rows[0]?.value;
      expect(heartbeat).toMatchObject({
        snapshotDeferred: 'CHAPTER_BUSY',
        runtimeFingerprint: {
          gitSha: 'test-git',
          artifactSha256: 'test-artifact',
          rolloutRevision: 'r840',
        },
        liveness: { chapterAdmission: { active: 1 } },
        pipeline: {
          limits: {
            configuredPagesPerChapter: 1,
            runtimeMaximumPagesPerChapter: 1,
            sourcePagesPerChapter: { mangaflix: 1, manhastro: 1, default: 1 },
          },
          chapterStages: { completed: expect.any(Number) },
        },
      });
      expect((runtime as any).heartbeatCompletedAt).not.toBeNull();
    } finally {
      admission.release();
      telemetryCollector.setRuntimeFingerprint({});
    }
  });

  it('records bounded historical P1 producer progress in the heartbeat without changing admission', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { WORKER_ID: 'snapshot-p1-producer', IMPORTER_USER_ID: ids.staff, DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, TELEGRAM_MEDIA_CONCURRENCY: 1,
        UPLOAD_RATE_LIMIT_BYTES_PER_SEC: 1, LITE_CHAPTER_WORKERS: 1,
        MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );
    const queue = (runtime as any).queue;
    queue.refillHistoricalP1Catalog = vi.fn().mockResolvedValue(2);
    queue.promoteDeferredHistoricalP1Catalog = vi.fn().mockResolvedValue(true);
    (runtime as any).isRuntimeLeader = true;

    await (runtime as any).refillHistoricalP1Catalog();
    expect((runtime as any).historicalP1Producer).toEqual({
      observedAt: expect.any(String), admitted: 2, promoted: true, error: null,
    });

    await (runtime as any).emitOperationalSnapshot();
    const stored = await db.query(`SELECT value FROM settings WHERE key='importer_heartbeat'`);
    const heartbeat = typeof stored.rows[0]?.value === 'string'
      ? JSON.parse(stored.rows[0].value)
      : stored.rows[0]?.value;
    expect(heartbeat.sourceFairness.historicalP1Producer).toMatchObject({
      admitted: 2, promoted: true, error: null,
    });
  });

  it('defers costly historical P1 catalog production behind live YSQL wait without starving it', async () => {
    const lanePool = Object.assign(poolFor(db), { waitingCount: 2 });
    const runtime = new ImporterLiteRuntime(
      lanePool as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: ids.staff, DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, TELEGRAM_MEDIA_CONCURRENCY: 1,
        UPLOAD_RATE_LIMIT_BYTES_PER_SEC: 1, LITE_CHAPTER_WORKERS: 1,
        MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1 } as any,
    );
    const queue = (runtime as any).queue;
    queue.refillHistoricalP1Catalog = vi.fn().mockResolvedValue(2);
    queue.promoteDeferredHistoricalP1Catalog = vi.fn().mockResolvedValue(true);
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).lastHistoricalP1ProducerAttemptAt = Date.now();

    await (runtime as any).refillHistoricalP1Catalog();
    expect(queue.refillHistoricalP1Catalog).not.toHaveBeenCalled();
    expect(queue.promoteDeferredHistoricalP1Catalog).not.toHaveBeenCalled();
    expect((runtime as any).historicalP1Producer).toMatchObject({
      admitted: 0, promoted: false, error: 'DEFERRED_FOR_LIVE_YSQL',
    });

    // A sustained productive wave still gets a bounded historical turn.
    (runtime as any).lastHistoricalP1ProducerAttemptAt = Date.now() - 120_001;
    await (runtime as any).refillHistoricalP1Catalog();
    expect(queue.refillHistoricalP1Catalog).toHaveBeenCalledTimes(1);
    expect(queue.promoteDeferredHistoricalP1Catalog).toHaveBeenCalledTimes(1);
  });

  it('fences a legacy runtime and Lite during cutover without stranding an in-flight durable task', async () => {
    // This is deliberately cross-runtime rather than a second Lite instance:
    // a rolling DIScloud deployment must never leave both v1 and Lite able to
    // admit claims. The shared settings row is the fence, while a task that
    // was leased before the handoff may still settle its durable result.
    const legacy = new ImporterEngine(
      { getPool: () => poolFor(db) } as any,
      {} as any,
      {} as any,
      {} as any,
      {
        WORKER_ID: 'legacy-cutover', MAX_CONCURRENT_CHAPTERS: 1,
        TESTED_CONCURRENCY_CEILING: 1, ADAPTIVE_INITIAL_CONCURRENCY: 1,
        DIRECT_DB_POOL_MAX: 1, TELEGRAM_MEDIA_CONCURRENCY: 1,
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1,
        QUEUE_LEASE_DURATION_SECONDS: 30, QUEUE_HEARTBEAT_INTERVAL_SECONDS: 10,
        NOX_MANGA_URL: 'https://example.test',
      } as any,
    );
    const lite = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { WORKER_ID: 'lite-cutover', IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );

    expect(await (legacy as any).tryAcquireRuntimeLeadership()).toBe(true);
    expect(await (lite as any).tryAcquireRuntimeLeadership()).toBe(false);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'cross-runtime-inflight', priority: 'P1', idempotencyKey: 'cross-runtime-inflight' });
    expect(await (lite as any).claimNext('lite-standby')).toBeNull();
    const inFlight = await repo.claim('legacy-inflight', 30);
    expect(inFlight?.leaseOwner).toBe('legacy-inflight');
    expect(await repo.finish(inFlight!, 'legacy-inflight', { state: 'DONE' })).toBe(true);

    await (legacy as any).releaseRuntimeLeadership();
    expect(await (lite as any).tryAcquireRuntimeLeadership()).toBe(true);
    await (lite as any).releaseRuntimeLeadership();
  });

  it('turns a durable unknown Telegram outcome into a visible per-task block without touching other work', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:unknown', priority: 'P1', idempotencyKey: 'unknown-recovery' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`, chapterKey: 'chapter:healthy', priority: 'P1', idempotencyKey: 'healthy-neighbour' });
    const unknown = await db.query<{ id: string }>(`SELECT id FROM importer_lite_tasks WHERE idempotency_key='unknown-recovery'`);
    await repo.reserveUpload(unknown.rows[0].id, 1, 'unknown-sha');
    await repo.markUploadUnknown(unknown.rows[0].id, 1);

    expect((await repo.reconcile(10)).uploads).toBe(1);
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks WHERE id=$1`, [unknown.rows[0].id])).rows[0])
      .toMatchObject({ state: 'BLOCKED', reason: 'UPLOAD_OUTCOME_UNKNOWN:1' });
    expect((await repo.claim('worker-a', 30))?.idempotencyKey).toBe('healthy-neighbour');
  });

  it('reopens an unknown upload only when a matching durable Telegram media receipt exists', async () => {
    await repo.enqueue({ taskType:'IMPORT_CHAPTER',workId:ids.p1a,workKey:`work:${ids.p1a}`,
      chapterKey:'chapter:receipt-proof',priority:'P1',idempotencyKey:'unknown-with-receipt' });
    const task = await db.query<{id:string}>(`SELECT id FROM importer_lite_tasks WHERE idempotency_key='unknown-with-receipt'`);
    const intent = await repo.reserveUpload(task.rows[0].id,1,'verified-sha');
    await db.query(`INSERT INTO media(id,provider,provider_key,bot_reference,mime,bytes,sha256,storage_ready,purpose)
      VALUES($1::uuid,'telegram','telegram-file-id','bot-1','image/jpeg',4096,'verified-sha',true,'chapter')`,[intent.mediaId]);
    await repo.markUploadUnknown(task.rows[0].id,1);
    await db.query(`UPDATE importer_lite_tasks SET state='BLOCKED',reason='UPLOAD_OUTCOME_UNKNOWN:1' WHERE id=$1::uuid`,[task.rows[0].id]);

    const deferred = await repo.reconcile(10, { recoverUploadOutcomes: false });
    expect(deferred.uploadReceiptRecoveries).toBe(0);
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1::uuid`, [task.rows[0].id])).rows[0].state)
      .toBe('BLOCKED');

    const result = await repo.reconcile(10);
    expect(result.uploadReceiptRecoveries).toBe(1);
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks WHERE id=$1`,[task.rows[0].id])).rows[0])
      .toMatchObject({state:'READY',reason:null});
    expect((await db.query(`SELECT state FROM importer_lite_uploads WHERE task_id=$1`,[task.rows[0].id])).rows[0].state)
      .toBe('PERSISTED');
  });

  it('repairs only an invalid persisted page receipt and preserves its historical media row', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:repair-page', priority: 'P1', idempotencyKey: 'repair-page' });
    const task = await db.query<{ id: string }>(`SELECT id FROM importer_lite_tasks WHERE idempotency_key='repair-page'`);
    await db.query(`INSERT INTO importer_lite_uploads(task_id,page_position,media_id,sha256,state)
      VALUES($1,1,$2,'old-page-sha','PERSISTED')`, [task.rows[0].id, ids.page]);
    await db.query(`UPDATE media SET storage_ready=false WHERE id=$1`, [ids.page]);
    const replacement = await repo.reserveUpload(task.rows[0].id, 1, 'replacement-page-sha');
    expect(replacement).toMatchObject({ state: 'RETRYABLE' });
    expect(replacement.mediaId).not.toBe(ids.page);
    expect((await db.query(`SELECT media_id,state,sha256 FROM importer_lite_uploads WHERE task_id=$1 AND page_position=1`, [task.rows[0].id])).rows[0])
      .toEqual({ media_id: replacement.mediaId, state: 'RETRYABLE', sha256: 'replacement-page-sha' });
    expect((await db.query(`SELECT id FROM media WHERE id=$1`, [ids.page])).rows[0].id).toBe(ids.page);
  });

  it('persists a Telegram receipt and its media row atomically after an intent reservation', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'chapter:atomic-receipt', priority: 'P1', idempotencyKey: 'atomic-receipt' });
    const task = await db.query<{ id: string }>(`SELECT id FROM importer_lite_tasks WHERE idempotency_key='atomic-receipt'`);
    const reserved = await repo.reserveUpload(task.rows[0].id, 1, 'atomic-page-sha');
    expect(reserved.state).toBe('INTENT');

    await repo.recordUploadReceipt({
      taskId: task.rows[0].id, pagePosition: 1, mediaId: reserved.mediaId, sha256: 'atomic-page-sha',
      bytes: 4096, mime: 'image/png', width: 64, height: 64, providerKey: 'telegram-file',
      botReference: 'bot-a', shardId: 'shard-a', channelId: 'channel-a', messageId: 42, createdBy: ids.staff,
    });
    expect((await db.query(`SELECT state,bytes,mime FROM importer_lite_uploads WHERE task_id=$1`, [task.rows[0].id])).rows[0])
      .toEqual({ state: 'PERSISTED', bytes: 4096, mime: 'image/png' });
    expect((await db.query(`SELECT storage_ready,sha256 FROM media WHERE id=$1`, [reserved.mediaId])).rows[0])
      .toEqual({ storage_ready: true, sha256: 'atomic-page-sha' });
  });

  it('reserves a bounded wave of durable pre-upload intents before Telegram work starts', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'chapter:batched-intents', priority: 'P1', idempotencyKey: 'batched-intents' });
    const task = await db.query<{ id: string }>(`SELECT id FROM importer_lite_tasks WHERE idempotency_key='batched-intents'`);
    const reservations = await repo.reserveUploads([
      { taskId: task.rows[0].id, pagePosition: 1, sha256: 'intent-page-one' },
      { taskId: task.rows[0].id, pagePosition: 2, sha256: 'intent-page-two' },
    ]);
    expect(reservations).toHaveLength(2);
    expect(reservations.map((entry) => entry.state)).toEqual(['INTENT', 'INTENT']);
    expect(new Set(reservations.map((entry) => entry.mediaId)).size).toBe(2);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads
      WHERE task_id=$1 AND state='INTENT'`, [task.rows[0].id])).rows[0].count).toBe(2);

    // The bulk fast path delegates a pre-existing intent to the established
    // single-row recovery rule: no second Telegram attempt is ever allowed.
    const resumed = await repo.reserveUploads([{ taskId: task.rows[0].id, pagePosition: 1, sha256: 'intent-page-one' }]);
    expect(resumed[0]).toMatchObject({ mediaId: reservations[0].mediaId, state: 'UNKNOWN' });
  });

  it('persists a bounded wave of confirmed Telegram receipts in one durable batch', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'chapter:batched-receipts', priority: 'P1', idempotencyKey: 'batched-receipts' });
    const task = await db.query<{ id: string }>(`SELECT id FROM importer_lite_tasks WHERE idempotency_key='batched-receipts'`);
    const first = await repo.reserveUpload(task.rows[0].id, 1, 'batch-page-one');
    const second = await repo.reserveUpload(task.rows[0].id, 2, 'batch-page-two');

    await repo.recordUploadReceipts([
      { taskId: task.rows[0].id, pagePosition: 1, mediaId: first.mediaId, sha256: 'batch-page-one', bytes: 1024,
        mime: 'image/png', width: 16, height: 16, providerKey: 'file-one', botReference: 'bot-a', shardId: 'shard-a', channelId: 'channel-a', messageId: 1, createdBy: ids.staff },
      { taskId: task.rows[0].id, pagePosition: 2, mediaId: second.mediaId, sha256: 'batch-page-two', bytes: 2048,
        mime: 'image/png', width: 32, height: 32, providerKey: 'file-two', botReference: 'bot-b', shardId: '00000000-0000-4000-8000-000000000014', channelId: 'channel-b', messageId: 2, createdBy: ids.staff },
    ]);

    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads WHERE task_id=$1 AND state='PERSISTED'`, [task.rows[0].id])).rows[0].count).toBe(2);
    expect((await db.query(`SELECT count(*)::int AS count FROM media WHERE id=ANY($1::uuid[])`, [[first.mediaId, second.mediaId]])).rows[0].count).toBe(2);
    const mediaShards = (await db.query(`SELECT storage_shard_id FROM media WHERE id=ANY($1::uuid[]) ORDER BY id`, [[first.mediaId, second.mediaId]])).rows
      .map((row) => row.storage_shard_id);
    expect(mediaShards).toContain(null);
    expect(mediaShards).toContain('00000000-0000-4000-8000-000000000014');
  });

  it('loads durable page receipts once for a chapter without including another task', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'chapter:receipt-snapshot', priority: 'P1', idempotencyKey: 'receipt-snapshot' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      chapterKey: 'chapter:receipt-snapshot-other', priority: 'P1', idempotencyKey: 'receipt-snapshot-other' });
    const tasks = await db.query<{ id: string; idempotency_key: string }>(`SELECT id,idempotency_key FROM importer_lite_tasks
      WHERE idempotency_key IN ('receipt-snapshot','receipt-snapshot-other')`);
    const mine = tasks.rows.find((row) => row.idempotency_key === 'receipt-snapshot')!;
    const other = tasks.rows.find((row) => row.idempotency_key === 'receipt-snapshot-other')!;
    const reserved = await repo.reserveUpload(mine.id, 1, 'snapshot-page-sha');
    await repo.recordUploadReceipt({
      taskId: mine.id, pagePosition: 1, mediaId: reserved.mediaId, sha256: 'snapshot-page-sha',
      bytes: 4096, mime: 'image/png', width: 64, height: 64, providerKey: 'telegram-file',
      botReference: 'bot-a', shardId: 'shard-a', channelId: 'channel-a', messageId: 43, createdBy: ids.staff,
    });
    await repo.reserveUpload(other.id, 1, 'other-snapshot-page-sha');
    const snapshot = await repo.uploadReceipts(mine.id);
    expect(snapshot).toHaveLength(1);
    expect(snapshot.get(1)).toMatchObject({ mediaId: reserved.mediaId, state: 'PERSISTED', sha256: 'snapshot-page-sha', storageReady: true });
  });

  it('reuses an already validated media hash without creating a Telegram intent', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'chapter:hash-reuse', priority: 'P1', idempotencyKey: 'hash-reuse' });
    const task = await db.query<{ id: string }>(`SELECT id FROM importer_lite_tasks WHERE idempotency_key='hash-reuse'`);
    const reserved = await repo.reserveUpload(task.rows[0].id, 1, 'page-sha');
    expect(reserved).toEqual({ mediaId: ids.page, state: 'PERSISTED' });
  });

  it('reuses ready media set-wise while reserving a mixed page wave', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'chapter:batch-hash-reuse', priority: 'P1', idempotencyKey: 'batch-hash-reuse' });
    const task = await db.query<{ id: string }>(`SELECT id FROM importer_lite_tasks WHERE idempotency_key='batch-hash-reuse'`);
    const reserved = await repo.reserveUploads([
      { taskId: task.rows[0].id, pagePosition: 1, sha256: 'page-sha' },
      { taskId: task.rows[0].id, pagePosition: 2, sha256: 'new-batch-page-sha' },
    ]);
    expect(reserved[0]).toEqual({ mediaId: ids.page, state: 'PERSISTED' });
    expect(reserved[1].state).toBe('INTENT');
    expect(reserved[1].mediaId).not.toBe(ids.page);
  });

  it('repairs a legacy exhausted publication with repeated valid page media exactly once', async () => {
    const key = 'pt-br|default|chapter|duplicate-pages|-';
    await db.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key,edition,language)
      VALUES($1,$2,$3,'1','n|000000000001','default','pt-BR')`, [ids.publish, key, ids.chapter]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.publish, workKey: `work:${ids.publish}`,
      chapterKey: key, priority: 'P1', idempotencyKey: 'duplicate-pages-import' });
    const imported = await repo.claim('importer', 60);
    await repo.finish(imported!, 'importer', { state: 'DONE', checkpoint: { chapterId: ids.chapter, mediaIds: [ids.page, ids.page] } });
    expect((await repo.reconcile(10)).publishable).toBe(1);
    await db.query(`UPDATE importer_lite_tasks SET state='BLOCKED',attempts=max_attempts,reason='MAX_ATTEMPTS_EXHAUSTED'
      WHERE idempotency_key='lite:publish:' || $1 || ':' || $2`, [ids.publish, key]);

    expect((await repo.reconcile(10)).publishRepairs).toBe(1);
    const publish = await repo.claim('publisher', 60);
    expect(publish).toMatchObject({ taskType: 'PUBLISH', reason: 'PUBLISH_INTEGRITY_REPAIRED' });
    expect(await repo.publish({ task: publish!, workerId: 'publisher', chapterId: ids.chapter,
      workId: ids.publish, mediaIds: [ids.page, ids.page], fresh: false })).toBe(true);
    expect((await db.query(`SELECT count(*)::int AS count FROM pages WHERE chapter_id=$1`, [ids.chapter])).rows[0].count).toBe(2);
    expect((await repo.reconcile(10)).publishRepairs).toBe(0);
  });

  it('does not retry a repeatedly crashed lease beyond its durable attempt limit', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:crash', priority: 'P1', idempotencyKey: 'crash-exhausted', maxAttempts: 1 });
    const claimed = await repo.claim('worker-a', 30);
    await db.query(`UPDATE importer_lite_tasks SET lease_until=now()-interval '1 second' WHERE id=$1`, [claimed!.id]);

    const recovered = await repo.reconcile(10);
    expect(recovered).toMatchObject({ leases: 0, exhausted: 1 });
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks WHERE id=$1`, [claimed!.id])).rows[0])
      .toMatchObject({ state: 'BLOCKED', reason: 'LEASE_EXPIRED_MAX_ATTEMPTS' });
  });

  it('persists discovery failure health and clears it after a confirmed healthy source operation', async () => {
    const adapter = { id: 'source-a', fetchUpdatedWorks: vi.fn(async () => { throw new Error('SOURCE_A_TIMEOUT'); }) };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    await repo.enqueue({ taskType: 'DISCOVER', workKey: 'source:source-a', priority: 'P2', idempotencyKey: 'unhealthy-discovery', payload: { source: 'source-a' } });
    const task = await repo.claim('discover', 30);
    await (runtime as any).handle(task, 'discover');
    expect((await db.query(`SELECT health,consecutive_failures FROM importer_lite_source_health WHERE source='source-a'`)).rows[0])
      .toMatchObject({ health: 'DEGRADED', consecutive_failures: 1 });
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0].state).toBe('RETRY_WAIT');

    await repo.recordSourceSuccess('source-a');
    expect((await db.query(`SELECT health,cooldown_until,consecutive_failures,last_error FROM importer_lite_source_health WHERE source='source-a'`)).rows[0])
      .toMatchObject({ health: 'HEALTHY', cooldown_until: null, consecutive_failures: 0, last_error: null });
  });

  it('does not let a concurrent source success clear an active durable rate-limit cooldown', async () => {
    await repo.recordSourceFailure('source-a', { retryAfter: 30, message: 'HTTP_429' });
    await repo.recordSourceSuccess('source-a');
    const row = (await db.query(`SELECT health,cooldown_until,consecutive_failures,last_error
      FROM importer_lite_source_health WHERE source='source-a'`)).rows[0];
    expect(row).toMatchObject({ health: 'RATE_LIMITED', consecutive_failures: 1, last_error: 'HTTP_429' });
    expect(new Date(row.cooldown_until).getTime()).toBeGreaterThan(Date.now());
  });

  it('bounds a hanging discovery call and returns its worker to durable retry', async () => {
    const adapter = { id: 'source-a', fetchUpdatedWorks: vi.fn(() => new Promise<never>(() => {})) };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, SOURCE_OPERATION_TIMEOUT_MS: 5_000 } as any,
    );
    await repo.enqueue({ taskType: 'DISCOVER', workKey: 'source:source-a', priority: 'P2', idempotencyKey: 'hanging-discovery', payload: { source: 'source-a' } });
    const task = await repo.claim('hanging-discovery-worker', 30);
    const started = Date.now();
    await (runtime as any).handle(task, 'hanging-discovery-worker');
    expect(Date.now() - started).toBeLessThan(7_000);
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toMatchObject({ state: 'RETRY_WAIT', reason: 'DISCOVERY_SOURCE_TIMEOUT' });
  }, 10_000);

  it('keeps explicitly disabled sources blocked without attempting catalog networking', async () => {
    const adapter = { fetchWorkSnapshot: vi.fn() };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    const finish = vi.fn().mockResolvedValue(undefined);
    (runtime as any).queue = { sourcePolicy: vi.fn().mockResolvedValue({ runnable:false, enabled:false, status:'DISABLED', chapterIngestionEnabled:true }), finish };
    await (runtime as any).catalog({ payload:{source:'source-a',sourceWorkId:'disabled-work'}, monitoring:false, attempts:1, maxAttempts:8 }, 'worker');
    expect(finish).toHaveBeenCalledWith(expect.anything(), 'worker', {state:'BLOCKED',reason:'SOURCE_DISABLED'});
    expect(adapter.fetchWorkSnapshot).not.toHaveBeenCalled();
  });

  it('uses the real source administration contract before discovery or catalog networking', async () => {
    await db.query(`UPDATE importer_sources
      SET enabled=false,status='PAUSED',sync_interval_minutes=7,cooldown_until=now()+interval '11 minutes'
      WHERE id='source-a'`);
    const policy = await repo.sourcePolicy('source-a');
    expect(policy).toMatchObject({ enabled: false, status: 'PAUSED', runnable: false, syncIntervalMs: 7 * 60_000,
      catalogDiscoveryEnabled: true, chapterIngestionEnabled: true });
    expect(policy?.cooldownUntil?.getTime()).toBeGreaterThan(Date.now());

    const adapter = {
      id: 'source-a',
      fetchUpdatedWorks: vi.fn(async () => ({ works: [], nextCursor: null })),
      fetchWorkSnapshot: vi.fn(async () => { throw new Error('must not fetch a paused source'); }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    await repo.enqueue({ taskType: 'DISCOVER', workKey: 'source:source-a', priority: 'P2', idempotencyKey: 'paused-discover', payload: { source: 'source-a' } });
    const discovery = await repo.claim('discovery-worker', 30);
    await (runtime as any).handle(discovery, 'discovery-worker');
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:source-a:paused-work', priority: 'P2', idempotencyKey: 'paused-catalog', payload: { source: 'source-a', sourceWorkId: 'paused-work' } });
    const catalog = await repo.claim('catalog-worker', 30);
    await (runtime as any).handle(catalog, 'catalog-worker');

    expect(adapter.fetchUpdatedWorks).not.toHaveBeenCalled();
    expect(adapter.fetchWorkSnapshot).not.toHaveBeenCalled();
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks WHERE id=$1`, [discovery!.id])).rows[0])
      .toMatchObject({ state: 'RETRY_WAIT', reason: 'SOURCE_NOT_READY:PAUSED' });
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks WHERE id=$1`, [catalog!.id])).rows[0])
      .toMatchObject({ state: 'RETRY_WAIT', reason: 'SOURCE_NOT_READY:PAUSED' });

    // These controls are optional in older schemas. The Lite query reads them
    // safely when present without requiring them for migration compatibility.
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS catalog_discovery_enabled boolean NOT NULL DEFAULT true`);
    await db.query(`UPDATE importer_sources SET catalog_discovery_enabled=false WHERE id='source-b'`);
    expect((await repo.sourcePolicy('source-b'))?.catalogDiscoveryEnabled).toBe(false);
    await (runtime as any).seedDiscovery();
    expect((await db.query(`SELECT idempotency_key FROM importer_lite_tasks WHERE idempotency_key LIKE 'lite:discover:%' ORDER BY idempotency_key`)).rows)
      .toEqual([]);

    // The source-wide switch must also fence a previously durable P2
    // discovery row. It is not a blanket ban on known-work CATALOG tasks.
    await repo.enqueue({ taskType: 'DISCOVER', workKey: 'source:source-b', priority: 'P2',
      idempotencyKey: 'disabled-source-discover', payload: { source: 'source-b' } });
    const disabledDiscovery = await repo.claim('disabled-discovery-worker', 30);
    await (runtime as any).handle(disabledDiscovery, 'disabled-discovery-worker');
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks WHERE id=$1`, [disabledDiscovery!.id])).rows[0])
      .toMatchObject({ state: 'RETRY_WAIT', reason: 'SOURCE_DISCOVERY_DISABLED' });
  });

  it('releases an expired administrative cooldown for P2 without overriding a live cooldown', async () => {
    await db.query(`UPDATE importer_sources
      SET enabled=true,status='COOLDOWN',cooldown_until=now()-interval '1 second'
      WHERE id='source-a'`);
    expect(await repo.sourcePolicy('source-a')).toMatchObject({ status: 'COOLDOWN', runnable: true });

    await db.query(`UPDATE importer_sources SET cooldown_until=now()+interval '1 minute' WHERE id='source-a'`);
    expect(await repo.sourcePolicy('source-a')).toMatchObject({ status: 'COOLDOWN', runnable: false });

    await db.query(`UPDATE importer_sources SET cooldown_until=now()-interval '1 second' WHERE id='source-a'`);
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    await (runtime as any).seedDiscovery();
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks WHERE idempotency_key='lite:discover:source-a'`)).rows[0].count)
      .toBe(1);

    await db.query(`UPDATE importer_sources SET cooldown_until=NULL WHERE id='source-a'`);
    expect(await repo.sourcePolicy('source-a')).toMatchObject({ status: 'COOLDOWN', runnable: false });
  });

  it('keeps startup alive when a discovery seed enqueue hits an idle transaction timeout', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    const enqueue = vi.fn()
      .mockRejectedValueOnce(new Error('terminating connection due to idle-in-transaction timeout'))
      .mockResolvedValue({ inserted: true, task: null });
    (runtime as any).queue = { enqueue };

    await expect((runtime as any).seedDiscovery()).resolves.toEqual({ sources: 2, inserted: 1, failed: 1 });
    expect(enqueue).toHaveBeenCalledTimes(2);
    expect((runtime as any).nextDiscoverySeedRetryAt).toBeGreaterThan(Date.now());
    expect((runtime as any).discoverySeed).toMatchObject({
      sources: 2, inserted: 1, failed: 1,
      error: expect.stringContaining('idle-in-transaction timeout'),
    });
  });

  it('fails closed for chapter admission until the durable protective-stop setting is read', async () => {
    const unavailablePool = {
      query: vi.fn().mockRejectedValue(new Error('temporary YSQL timeout')),
      connect: vi.fn(),
    } as unknown as Pool;
    const runtime = new ImporterLiteRuntime(
      unavailablePool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );

    expect((runtime as any).shouldPauseChapterAdmission()).toBe(false);
    (runtime as any).armStartupProtectiveStop();
    expect((runtime as any).shouldPauseChapterAdmission()).toBe(true);
    await (runtime as any).refreshManualProtectiveStop();
    expect((runtime as any).shouldPauseChapterAdmission()).toBe(true);
  });

  it('reopens only source-blocked work after fresh catalog evidence, never an unknown upload outcome', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:1', priority: 'P1', idempotencyKey: 'source-blocked' });
    const sourceBlocked = await repo.claim('worker-a', 30);
    await repo.finish(sourceBlocked!, 'worker-a', { state: 'BLOCKED', reason: 'NO_CONFIRMED_ROUTE' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:2', priority: 'P1', idempotencyKey: 'unknown-upload' });
    const unknownUpload = await repo.claim('worker-a', 30);
    await repo.finish(unknownUpload!, 'worker-a', { state: 'BLOCKED', reason: 'UPLOAD_OUTCOME_UNKNOWN:1' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:3', priority: 'P1', idempotencyKey: 'changed-manifest' });
    const changedManifest = await repo.claim('worker-a', 30);
    await repo.finish(changedManifest!, 'worker-a', { state: 'BLOCKED', reason: 'SOURCE_PAGE_MANIFEST_CHANGED_AFTER_PARTIAL_STORAGE' });

    expect(await repo.wakeCatalogBlocked(ids.p1a, `work:${ids.p1a}`)).toBe(1);
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [sourceBlocked!.id])).rows[0].state).toBe('READY');
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [unknownUpload!.id])).rows[0].state).toBe('BLOCKED');
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [changedManifest!.id])).rows[0].state).toBe('BLOCKED');
  });

  it('publishes only after durable media validation and leaves a Reader-compatible page manifest', async () => {
    await repo.enqueue({ taskType: 'PUBLISH', workId: ids.publish, workKey: `work:${ids.publish}`, chapterKey: 'chapter:1', priority: 'P1', idempotencyKey: 'publish' });
    const task = await repo.claim('publisher', 120);
    const published = await repo.publish({ task: task!, workerId: 'publisher', chapterId: ids.chapter, workId: ids.publish, mediaIds: [ids.page], fresh: true });
    expect(published).toBe(true);
    const chapter = await db.query(`SELECT published_at,is_fresh_release FROM chapters WHERE id=$1`, [ids.chapter]);
    expect(chapter.rows[0].published_at).toBeTruthy();
    expect(chapter.rows[0].is_fresh_release).toBe(true);
    expect((await db.query(`SELECT media_id,position FROM pages WHERE chapter_id=$1`, [ids.chapter])).rows)
      .toEqual([{ media_id: ids.page, position: 1 }]);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_publication_events WHERE chapter_id=$1`, [ids.chapter])).rows[0].count).toBe(1);
    expect(await repo.progressSnapshot()).toMatchObject({ publishedLastMinute: 1, oldestReadyAt: null, oldestStoredUnpublishedAt: null });
  });

  it('keeps the chapter non-public when the atomic page replacement rejects an incomplete manifest', async () => {
    await repo.enqueue({ taskType: 'PUBLISH', workId: ids.publish, workKey: `work:${ids.publish}`,
      chapterKey: 'chapter:incomplete-media', priority: 'P1', idempotencyKey: 'publish-incomplete-media' });
    const task = await repo.claim('publisher', 120);
    await expect(repo.publish({ task: task!, workerId: 'publisher', chapterId: ids.chapter, workId: ids.publish,
      mediaIds: ['00000000-0000-4000-8000-000000000009'], fresh: true })).rejects.toThrow('Incomplete stored page manifest');
    expect((await db.query(`SELECT published_at FROM chapters WHERE id=$1`, [ids.chapter])).rows[0].published_at).toBeNull();
    expect((await db.query(`SELECT count(*)::int AS count FROM pages WHERE chapter_id=$1`, [ids.chapter])).rows[0].count).toBe(0);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_publication_events WHERE chapter_id=$1`, [ids.chapter])).rows[0].count).toBe(0);
  });

  it('atomically settles stored media and materializes exactly one staff-aware publish task', async () => {
    const key = 'pt-br|default|chapter|atomic-completion|-';
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: key, priority: 'P1', idempotencyKey: 'atomic-completion' });
    const task = await repo.claim('importer', 120);
    expect(task).toBeTruthy();
    // A panel request can arrive after the import claim. Its overlay must be
    // applied to the newly-created PUBLISH task without another round trip.
    await db.query(`INSERT INTO importer_staff_requests(work_id,status) VALUES($1,'ACTIVE')`, [ids.p1a]);
    expect(await repo.completeImportAndEnqueuePublish(task!, 'importer', {
      chapterId: ids.chapter, mediaIds: [ids.page], pageCount: 1,
      selectedSource: 'source-a', mediaStoredAt: new Date().toISOString(),
    })).toBe(true);
    const imported = await db.query(`SELECT state,lease_owner,checkpoint->>'chapterId' AS chapter_id,
      checkpoint->>'selectedSource' AS source FROM importer_lite_tasks WHERE id=$1`, [task!.id]);
    expect(imported.rows[0]).toMatchObject({ state: 'DONE', lease_owner: null, chapter_id: ids.chapter, source: 'source-a' });
    const publish = await db.query(`SELECT priority,base_priority,payload->>'importTaskId' AS import_task_id,
      payload->>'source' AS source,payload->>'chapterId' AS chapter_id,payload->'mediaIds' AS media_ids
      FROM importer_lite_tasks WHERE idempotency_key=$1`, [`lite:publish:${ids.p1a}:${key}`]);
    expect(publish.rows).toEqual([expect.objectContaining({ priority: 0, base_priority: 2, import_task_id: task!.id,
      source: 'source-a', chapter_id: ids.chapter, media_ids: [ids.page] })]);
    expect((await db.query(`SELECT health,consecutive_failures FROM importer_lite_source_health WHERE source='source-a'`)).rows[0])
      .toMatchObject({ health: 'HEALTHY', consecutive_failures: 0 });
    // A repeat cannot create another public task, and an expired/lost fence
    // never acknowledges media that this worker no longer owns.
    expect(await repo.completeImportAndEnqueuePublish(task!, 'importer', {
      chapterId: ids.chapter, mediaIds: [ids.page], pageCount: 1,
      selectedSource: 'source-a', mediaStoredAt: new Date().toISOString(),
    })).toBe(false);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key=$1`, [`lite:publish:${ids.p1a}:${key}`])).rows[0].count).toBe(1);
  });

  it('does not clear a newer source cooldown while completing an already-uploaded chapter', async () => {
    const key = 'pt-br|default|chapter|completion-cooldown|-';
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: key, priority: 'P1', idempotencyKey: 'completion-cooldown' });
    const task = await repo.claim('importer-cooldown', 120);
    expect(task).toBeTruthy();
    await db.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
      VALUES('source-a','RATE_LIMITED',now()+interval '1 hour',2,'provider 429')
      ON CONFLICT(source) DO UPDATE SET health=excluded.health,cooldown_until=excluded.cooldown_until,
        consecutive_failures=excluded.consecutive_failures,last_error=excluded.last_error`);

    expect(await repo.completeImportAndEnqueuePublish(task!, 'importer-cooldown', {
      chapterId: ids.chapter, mediaIds: [ids.page], pageCount: 1,
      selectedSource: 'source-a', mediaStoredAt: new Date().toISOString(),
    })).toBe(true);
    expect((await db.query(`SELECT health,cooldown_until>now() AS cooling
      FROM importer_lite_source_health WHERE source='source-a'`)).rows[0])
      .toMatchObject({ health: 'RATE_LIMITED', cooling: true });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE idempotency_key=$1`, [`lite:publish:${ids.p1a}:${key}`])).rows[0].count).toBe(1);
  });

  it('publishes an atomic completion payload without rereading its import checkpoint', async () => {
    await repo.enqueue({ taskType: 'PUBLISH', workId: ids.publish, workKey: `work:${ids.publish}`,
      chapterKey: 'chapter:payload', priority: 'P1', idempotencyKey: 'publish-payload',
      payload: { importTaskId: '00000000-0000-4000-8000-000000000099', source: 'source-a',
        chapterId: ids.chapter, mediaIds: [ids.page] } });
    const task = await repo.claim('publisher-payload', 120);
    const dbForExecutor = { query: vi.fn(async () => {
      throw new Error('PUBLISH_PAYLOAD_SHOULD_NOT_REREAD_IMPORT_CHECKPOINT');
    }) } as unknown as Pool;
    const executor = new LiteExecutor(dbForExecutor, repo, {} as SourceRegistry, {} as StorageProvider, ids.staff,
      new AsyncSemaphore(1, 'publish-payload-downloads'));

    await executor.publish(task!, 'publisher-payload');

    expect((dbForExecutor.query as any)).not.toHaveBeenCalled();
    expect((await db.query(`SELECT published_at IS NOT NULL AS published FROM chapters WHERE id=$1`, [ids.chapter])).rows[0].published).toBe(true);
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0].state).toBe('DONE');
  });

  it('keeps the checkpoint lookup for a legacy recovery publish task', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.publish, workKey: `work:${ids.publish}`,
      chapterKey: 'chapter:legacy-publish', priority: 'P1', idempotencyKey: 'legacy-publish-origin' });
    const imported = await repo.claim('legacy-importer', 120);
    await repo.finish(imported!, 'legacy-importer', { state: 'DONE', checkpoint: { chapterId: ids.chapter, mediaIds: [ids.page] } });
    expect((await repo.reconcile(10)).publishable).toBe(1);
    const task = await repo.claim('legacy-publisher', 120);
    expect(task?.payload).toEqual({ importTaskId: imported!.id, orderKey: null });
    const executor = new LiteExecutor(poolFor(db) as Pool, repo, {} as SourceRegistry, {} as StorageProvider, ids.staff,
      new AsyncSemaphore(1, 'legacy-publish-downloads'));

    await executor.publish(task!, 'legacy-publisher');

    expect((await db.query(`SELECT published_at IS NOT NULL AS published FROM chapters WHERE id=$1`, [ids.chapter])).rows[0].published).toBe(true);
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0].state).toBe('DONE');
  });

  it('settles the originating import after publication so bounded recovery does not rematerialize it', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.publish, workKey: `work:${ids.publish}`, chapterKey: 'chapter:1', priority: 'P1', idempotencyKey: 'origin-import' });
    const imported = await repo.claim('importer', 120);
    await repo.finish(imported!, 'importer', { state: 'DONE', checkpoint: { chapterId: ids.chapter, mediaIds: [ids.page] } });
    expect((await repo.reconcile(10)).publishable).toBe(1);
    const publish = await repo.claim('publisher', 120);
    expect(publish?.taskType).toBe('PUBLISH');
    expect(await repo.publish({ task: publish!, workerId: 'publisher', chapterId: ids.chapter, workId: ids.publish, mediaIds: [ids.page], fresh: false })).toBe(true);
    expect((await db.query(`SELECT checkpoint ? 'publishedAt' AS settled FROM importer_lite_tasks WHERE id=$1`, [imported!.id])).rows[0].settled).toBe(true);
    expect((await repo.reconcile(10)).publishable).toBe(0);
  });

  it('falls back within the same P0 claim when the preferred source fails before media storage', async () => {
    await db.query(`
      INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,source_rank,confirmed)
      VALUES ($1,'number:1|language:pt-BR|edition:default|kind:normal','source-a','a-1','pt-BR','default',10,true),
             ($1,'number:1|language:pt-BR|edition:default|kind:normal','source-b','b-1','pt-BR','default',20,true)
    `, [ids.p1a]);
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'number:1|language:pt-BR|edition:default|kind:normal', priority: 'P0', idempotencyKey: 'fallback',
      payload: { displayNumber: '1', chapterTitle: 'One', language: 'pt-BR', edition: 'default', orderKey: '1' },
    });
    const task = await repo.claim('worker-a', 120);
    const sourceA = { id: 'source-a', fetchChapterPages: vi.fn(async () => { throw new Error('SOURCE_A_TIMEOUT'); }) };
    const sourceB = { id: 'source-b', fetchChapterPages: vi.fn(async () => ['https://source-b.test/page.png']) };
    const registry = { get: (id: string) => id === 'source-a' ? sourceA : id === 'source-b' ? sourceB : undefined } as unknown as SourceRegistry;
    const storage: StorageProvider = {
      upload: vi.fn(async () => 'telegram-file-b'), healthCheck: async () => true, getProviderKey: () => 'telegram',
      getLastBotReference: () => 'bot-b', getLastShardId: () => 'shard-b', getLastChannelId: () => 'channel-b',
    };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200, headers: { 'content-type': 'image/png' } })));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 1);
      await executor.importChapter(task!, 'worker-a');
    } finally {
      vi.stubGlobal('fetch', originalFetch);
    }
    expect(sourceA.fetchChapterPages).toHaveBeenCalledOnce();
    expect(sourceB.fetchChapterPages).toHaveBeenCalledOnce();
    expect((await db.query(`SELECT health FROM importer_lite_source_health WHERE source='source-a'`)).rows[0].health).toBe('DEGRADED');
    expect((await db.query(`SELECT state,checkpoint->>'selectedSource' AS source FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toMatchObject({ state: 'DONE', source: 'source-b' });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads WHERE task_id=$1 AND state='PERSISTED'`, [task!.id])).rows[0].count).toBe(1);
  });

  it('releases a chapter slot when a source stalls before returning its page manifest', async () => {
    const key = 'pt-br|default|chapter|manifest-timeout|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_chapter_id,language,edition,source_rank,confirmed)
      VALUES ($1,$2,'source-a','stalled-manifest','pt-BR','default',10,true),
             ($1,$2,'source-b','healthy-manifest','pt-BR','default',20,true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key,
      priority: 'P1', idempotencyKey: 'manifest-timeout-fallback',
      payload: { displayNumber: '1.1', chapterTitle: 'Manifest timeout', language: 'pt-BR', edition: 'default', orderKey: '1.1' } });
    const task = await repo.claim('worker-a', 120);
    const sourceA = { id: 'source-a', fetchChapterPages: vi.fn(() => new Promise<string[]>(() => {})) };
    const sourceB = { id: 'source-b', fetchChapterPages: vi.fn(async () => ['https://source-b.test/page.png']) };
    const registry = { get: (id: string) => id === 'source-a' ? sourceA : sourceB } as unknown as SourceRegistry;
    const storage: StorageProvider = { upload: vi.fn(async () => 'telegram-file'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200, headers: { 'content-type': 'image/png' } })));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'manifest-timeout-downloads'), 1,
        new AsyncSemaphore(2, 'manifest-timeout-buffers'), new HostRateLimiter(100), 1, undefined, undefined, 20);
      await executor.importChapter(task!, 'worker-a');
    } finally { vi.stubGlobal('fetch', originalFetch); }
    expect(sourceA.fetchChapterPages).toHaveBeenCalledOnce();
    expect(sourceB.fetchChapterPages).toHaveBeenCalledOnce();
    expect((await db.query(`SELECT state,checkpoint->>'selectedSource' AS source FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toMatchObject({ state: 'DONE', source: 'source-b' });
  });

  it('skips a preferred source with chapter ingestion disabled and immediately uses its confirmed alternative', async () => {
    const key = 'pt-br|default|chapter|admin-fallback|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,source_rank,confirmed)
      VALUES ($1,$2,'source-a','disabled-a','pt-BR','default',10,true),
             ($1,$2,'source-b','enabled-b','pt-BR','default',20,true)`, [ids.p1a, key]);
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS chapter_ingestion_enabled boolean NOT NULL DEFAULT true`);
    await db.query(`UPDATE importer_sources SET chapter_ingestion_enabled=false WHERE id='source-a'`);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key,
      priority: 'P1', idempotencyKey: 'admin-source-fallback',
      payload: { displayNumber: '7', chapterTitle: 'Seven', language: 'pt-BR', edition: 'default', orderKey: '7' } });
    const task = await repo.claim('worker-a', 120);
    const sourceA = { id: 'source-a', fetchChapterPages: vi.fn(async () => ['https://source-a.test/forbidden.png']) };
    const sourceB = { id: 'source-b', fetchChapterPages: vi.fn(async () => ['https://source-b.test/page.png']) };
    const registry = { get: (id: string) => id === 'source-a' ? sourceA : id === 'source-b' ? sourceB : undefined } as unknown as SourceRegistry;
    const storage: StorageProvider = { upload: vi.fn(async () => 'telegram-file-b'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 1);
      await executor.importChapter(task!, 'worker-a');
    } finally { vi.stubGlobal('fetch', originalFetch); }
    expect(sourceA.fetchChapterPages).not.toHaveBeenCalled();
    expect(sourceB.fetchChapterPages).toHaveBeenCalledOnce();
    expect((await db.query(`SELECT state,checkpoint->>'selectedSource' AS source FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toMatchObject({ state: 'DONE', source: 'source-b' });
  });

  it('executes a confirmed source after its administrative cooldown deadline has passed', async () => {
    const key = 'pt-br|default|chapter|expired-cooldown|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,source_rank,confirmed)
      VALUES ($1,$2,'source-a','expired-cooldown-a','pt-BR','default',10,true)`, [ids.p1a, key]);
    await db.query(`UPDATE importer_sources SET status='COOLDOWN',cooldown_until=now()-interval '1 second' WHERE id='source-a'`);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key,
      priority: 'P1', idempotencyKey: 'expired-cooldown-source',
      payload: { displayNumber: '8', chapterTitle: 'Eight', language: 'pt-BR', edition: 'default', orderKey: '8' } });
    const task = await repo.claim('worker-a', 120);
    const sourceA = { id: 'source-a', fetchChapterPages: vi.fn(async () => ['https://source-a.test/page.png']) };
    const storage: StorageProvider = { upload: vi.fn(async () => 'telegram-file-a'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, { get: () => sourceA } as unknown as SourceRegistry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 1);
      await executor.importChapter(task!, 'worker-a');
    } finally { vi.stubGlobal('fetch', originalFetch); }
    expect(sourceA.fetchChapterPages).toHaveBeenCalledOnce();
    expect((await db.query(`SELECT state,checkpoint->>'selectedSource' AS source FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toMatchObject({ state: 'DONE', source: 'source-a' });
  });

  it('requires adapter ownership proof before using a fallback route that can verify it', async () => {
    const key = 'pt-br|default|chapter|2|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,source_rank,confirmed)
      VALUES ($1,$2,'source-a','wrong-work','a-2','pt-BR','default',10,true),
             ($1,$2,'source-b','right-work','b-2','pt-BR','default',20,true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'ownership-fallback',
      payload: { displayNumber: '2', chapterTitle: 'Two', language: 'pt-BR', edition: 'default', orderKey: '2' } });
    const task = await repo.claim('worker-a', 120);
    const sourceA = { id: 'source-a', isChapterOwnedByWork: () => false, fetchChapterPages: vi.fn(async () => ['https://source-a.test/wrong.png']) };
    const sourceB = { id: 'source-b', isChapterOwnedByWork: () => true, fetchChapterPages: vi.fn(async () => ['https://source-b.test/right.png']) };
    const registry = { get: (id: string) => id === 'source-a' ? sourceA : sourceB } as unknown as SourceRegistry;
    const storage: StorageProvider = { upload: vi.fn(async () => 'telegram-file'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 1);
      await executor.importChapter(task!, 'worker-a');
    } finally { vi.stubGlobal('fetch', originalFetch); }
    expect(sourceA.fetchChapterPages).not.toHaveBeenCalled();
    expect(sourceB.fetchChapterPages).toHaveBeenCalledOnce();
    expect((await db.query(`SELECT checkpoint->>'selectedSource' AS source FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0].source).toBe('source-b');
  });

  it('never mixes a fallback source into a chapter after the preferred source stored one page', async () => {
    const key = 'pt-br|default|chapter|3|-';
    await db.query(`
      INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,source_rank,confirmed)
      VALUES ($1,$2,'source-a','a-3','pt-BR','default',10,true), ($1,$2,'source-b','b-3','pt-BR','default',20,true)
    `, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'partial-fallback',
      payload: { displayNumber: '3', chapterTitle: 'Three', language: 'pt-BR', edition: 'default', orderKey: '3' } });
    const task = await repo.claim('worker-a', 120);
    const sourceA = { id: 'source-a', fetchChapterPages: vi.fn(async () => ['https://source-a.test/good.png', 'https://source-a.test/bad.png']) };
    const sourceB = { id: 'source-b', fetchChapterPages: vi.fn(async () => ['https://source-b.test/one.png', 'https://source-b.test/two.png']) };
    const registry = { get: (id: string) => id === 'source-a' ? sourceA : sourceB } as unknown as SourceRegistry;
    const storage: StorageProvider = { upload: vi.fn(async () => 'telegram-file'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async (url: string) => url.includes('bad')
      ? new Response('source unavailable', { status: 503 })
      : new Response(png, { status: 200 })));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 1);
      await executor.importChapter(task!, 'worker-a');
    } finally { vi.stubGlobal('fetch', originalFetch); }
    expect(sourceB.fetchChapterPages).not.toHaveBeenCalled();
    expect((await db.query(`SELECT state,checkpoint->>'selectedSource' AS source FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toMatchObject({ state: 'RETRY_WAIT', source: 'source-a' });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads WHERE task_id=$1 AND state='PERSISTED'`, [task!.id])).rows[0].count).toBe(1);
  });

  it('drains in-flight page uploads before source fallback decides whether a chapter is pinned', async () => {
    const key = 'pt-br|default|chapter|3-concurrent|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,source_rank,confirmed)
      VALUES ($1,$2,'source-a','a-3-concurrent','pt-BR','default',10,true), ($1,$2,'source-b','b-3-concurrent','pt-BR','default',20,true)
    `, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'partial-concurrent-fallback',
      payload: { displayNumber: '3.1', chapterTitle: 'Three point one', language: 'pt-BR', edition: 'default', orderKey: '3.1' } });
    const task = await repo.claim('worker-a', 120);
    const sourceA = { id: 'source-a', fetchChapterPages: vi.fn(async () => ['https://source-a.test/slow-good.png', 'https://source-a.test/fast-bad.png']) };
    const sourceB = { id: 'source-b', fetchChapterPages: vi.fn(async () => ['https://source-b.test/one.png', 'https://source-b.test/two.png']) };
    const registry = { get: (id: string) => id === 'source-a' ? sourceA : sourceB } as unknown as SourceRegistry;
    const storage: StorageProvider = { upload: vi.fn(async () => 'telegram-file'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async (url: string) => {
      if (url.includes('fast-bad')) return new Response('source unavailable', { status: 503 });
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      return new Response(png, { status: 200 });
    }));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 2,
        new AsyncSemaphore(2, 'test-buffers'));
      await executor.importChapter(task!, 'worker-a');
    } finally { vi.stubGlobal('fetch', originalFetch); }
    expect(sourceB.fetchChapterPages).not.toHaveBeenCalled();
    expect((await db.query(`SELECT state,checkpoint->>'selectedSource' AS source FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toMatchObject({ state: 'RETRY_WAIT', source: 'source-a' });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads WHERE task_id=$1 AND state='PERSISTED'`, [task!.id])).rows[0].count).toBe(1);
  });

  it('records an unknown durable upload outcome instead of blindly uploading again after DB receipt failure', async () => {
    const key = 'pt-br|default|chapter|4|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','a-4','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'unknown-receipt',
      payload: { displayNumber: '4', chapterTitle: 'Four', language: 'pt-BR', edition: 'default', orderKey: '4' } });
    const task = await repo.claim('worker-a', 120);
    const registry = { get: () => ({ id: 'source-a', fetchChapterPages: async () => ['https://source-a.test/page.png'] }) } as unknown as SourceRegistry;
    const storage: StorageProvider = { upload: vi.fn(async () => 'telegram-file'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalReceiptBatch = repo.recordUploadReceipts.bind(repo);
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    (repo as any).recordUploadReceipts = async () => {
      throw Object.assign(new Error('DB_RECEIPT_WRITE_FAILED'), { code: '40001' });
    };
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 1);
      await executor.importChapter(task!, 'worker-a');
    } finally {
      (repo as any).recordUploadReceipts = originalReceiptBatch;
      vi.stubGlobal('fetch', originalFetch);
    }
    expect((await db.query(`SELECT state,checkpoint->'lastUploadFailure' AS failure FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toMatchObject({ state: 'BLOCKED', failure: { position: 1, phase: 'RECEIPT_PERSISTENCE', class: 'SQLSTATE_40001' } });
    expect((await db.query(`SELECT state FROM importer_lite_uploads WHERE task_id=$1`, [task!.id])).rows[0].state).toBe('UNKNOWN');
    expect(storage.upload).toHaveBeenCalledOnce();
  });

  it('retries a transient receipt write once without repeating a confirmed Telegram upload', async () => {
    const key = 'pt-br|default|chapter|4.1|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','a-4-1','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'retry-transient-receipt',
      payload: { displayNumber: '4.1', chapterTitle: 'Four Point One', language: 'pt-BR', edition: 'default', orderKey: '4.1' } });
    const task = await repo.claim('worker-a', 120);
    const registry = { get: () => ({ id: 'source-a', fetchChapterPages: async () => ['https://source-a.test/page.png'] }) } as unknown as SourceRegistry;
    const storage: StorageProvider = { upload: vi.fn(async () => 'telegram-file'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalReceiptBatch = repo.recordUploadReceipts.bind(repo);
    const originalFetch = globalThis.fetch;
    let receiptWrites = 0;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    (repo as any).recordUploadReceipts = async (inputs: any[]) => {
      receiptWrites++;
      if (receiptWrites === 1) throw new Error('TRANSIENT_DB_RECEIPT_WRITE_FAILED');
      return originalReceiptBatch(inputs);
    };
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 1);
      await executor.importChapter(task!, 'worker-a');
    } finally {
      (repo as any).recordUploadReceipts = originalReceiptBatch;
      vi.stubGlobal('fetch', originalFetch);
    }
    expect(receiptWrites).toBe(2);
    expect(storage.upload).toHaveBeenCalledOnce();
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0].state).toBe('DONE');
    expect((await db.query(`SELECT state FROM importer_lite_uploads WHERE task_id=$1`, [task!.id])).rows[0].state).toBe('PERSISTED');
  });

  it('keeps an explicit Telegram 429 retryable without switching bots or duplicating its upload intent', async () => {
    const key = 'pt-br|default|chapter|5|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','a-5','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'retryable-429',
      payload: { displayNumber: '5', chapterTitle: 'Five', language: 'pt-BR', edition: 'default', orderKey: '5' } });
    const registry = { get: () => ({ id: 'source-a', fetchChapterPages: async () => ['https://source-a.test/page.png'] }) } as unknown as SourceRegistry;
    const flood = Object.assign(new Error('Too Many Requests'), { statusCode: 429, retryAfter: 1 });
    const storage: StorageProvider = { upload: vi.fn().mockRejectedValueOnce(flood).mockResolvedValueOnce('telegram-file'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 1);
      const first = await repo.claim('worker-a', 120);
      await executor.importChapter(first!, 'worker-a');
      expect((await db.query(`SELECT state FROM importer_lite_uploads WHERE task_id=$1`, [first!.id])).rows[0].state).toBe('RETRYABLE');
      expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [first!.id])).rows[0].state).toBe('RETRY_WAIT');
      await db.query(`UPDATE importer_lite_source_health SET cooldown_until=now()-interval '1 second' WHERE source='source-a'`);
      await db.query(`UPDATE importer_lite_tasks SET next_run_at=now()-interval '1 second' WHERE id=$1`, [first!.id]);
      await repo.reconcile(10);
      const retry = await repo.claim('worker-b', 120);
      await executor.importChapter(retry!, 'worker-b');
      expect((await db.query(`SELECT state FROM importer_lite_uploads WHERE task_id=$1`, [first!.id])).rows[0].state).toBe('PERSISTED');
      expect(storage.upload).toHaveBeenCalledTimes(2);
    } finally { vi.stubGlobal('fetch', originalFetch); }
  });

  it('resumes a partially stored chapter after restart without re-uploading its durable pages', async () => {
    const key = 'pt-br|default|chapter|restart-partial|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','restart-partial','pt-BR','default',true)`, [ids.publish, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.publish, workKey: `work:${ids.publish}`, chapterKey: key, priority: 'P1', idempotencyKey: 'restart-partial',
      payload: { displayNumber: '5.5', chapterTitle: 'Restart partial', language: 'pt-BR', edition: 'default', orderKey: '5.5' } });
    const adapter = { id: 'source-a', fetchChapterPages: async () => ['https://source-a.test/one.png', 'https://source-a.test/two.png'] };
    const storage: StorageProvider = { upload: vi.fn(async () => 'telegram-file'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const secondPng = Uint8Array.from(png);
    secondPng[secondPng.length - 13] ^= 1; // distinct valid payload/hash
    let resumed = false;
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async (url: string) => new Response(url.includes('two') ? secondPng : png, {
      status: url.includes('two') && !resumed ? 502 : 200,
    })));
    try {
      const first = await repo.claim('before-restart', 120);
      const beforeRestart = new LiteExecutor(poolFor(db) as Pool, repo, { get: () => adapter } as unknown as SourceRegistry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(1, 'before-restart-downloads'), 1);
      await beforeRestart.importChapter(first!, 'before-restart');
      expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [first!.id])).rows[0].state).toBe('RETRY_WAIT');
      expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads WHERE task_id=$1 AND state='PERSISTED'`, [first!.id])).rows[0].count).toBe(1);
      expect(storage.upload).toHaveBeenCalledTimes(1);

      // A new repository/executor models a process restart. The source's
      // temporary cooldown has elapsed; durable receipts, not in-memory
      // state, decide what it can reuse.
      await db.query(`UPDATE importer_lite_source_health SET cooldown_until=now()-interval '1 second' WHERE source='source-a'`);
      await db.query(`UPDATE importer_lite_tasks SET next_run_at=now()-interval '1 second' WHERE id=$1`, [first!.id]);
      await repo.reconcile(10);
      resumed = true;
      const restartedRepo = new LiteRepository(poolFor(db));
      const retry = await restartedRepo.claim('after-restart', 120);
      const afterRestart = new LiteExecutor(poolFor(db) as Pool, restartedRepo, { get: () => adapter } as unknown as SourceRegistry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(1, 'after-restart-downloads'), 1);
      await afterRestart.importChapter(retry!, 'after-restart');
      expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads WHERE task_id=$1 AND state='PERSISTED'`, [first!.id])).rows[0].count).toBe(2);
      expect(storage.upload).toHaveBeenCalledTimes(2);

      const publish = await restartedRepo.claim('after-restart-publish', 120);
      expect(publish?.taskType).toBe('PUBLISH');
      await afterRestart.publish(publish!, 'after-restart-publish');
      const visible = await db.query(`SELECT c.published_at,count(p.media_id)::int AS pages
        FROM chapters c JOIN pages p ON p.chapter_id=c.id WHERE c.work_id=$1 AND c.title='Restart partial' GROUP BY c.published_at`, [ids.publish]);
      expect(visible.rows).toHaveLength(1);
      expect(visible.rows[0]).toMatchObject({ pages: 2 });
      expect(visible.rows[0].published_at).toBeTruthy();
    } finally { vi.stubGlobal('fetch', originalFetch); }
  });

  it('tracks a newer confirmed alternate source and deduplicates its releases across sources', async () => {
    await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status) VALUES
      ('00000000-0000-4000-8000-000000000361','source-a','origin-100',$1,'SYNCED'),
      ('00000000-0000-4000-8000-000000000362','source-b','alternate-200',$1,'SYNCED')`, [ids.p1a]);
    await db.query(`UPDATE importer_sources SET enabled=false WHERE id='source-a'`);
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,order_key,confirmed)
      VALUES($1,'pt-br|default|chapter|100|-','source-a','a-100','pt-BR','default','n|000000000100|000000000000||||-',true)`, [ids.p1a]);
    await repo.enrollConfirmedP0Mappings(ids.p1a,[{source:'source-b',sourceWorkId:'alternate-200'}]);
    await db.query(`UPDATE importer_lite_tasks SET next_run_at=now()-interval '1 second' WHERE idempotency_key='lite:p0-monitor:source-b:alternate-200'`);
    let latest = 200;
    const adapter = { id:'source-b', fetchWorkSnapshot: async () => ({
      details:{sourceWorkId:'alternate-200',title:'P1 A',slug:'p1-a',status:'ONGOING' as const,language:'pt-BR'},
      chapters:[{sourceChapterId:`b-${latest}`,number:latest,displayNumber:String(latest),title:`Chapter ${latest}`}],
    }) };
    const runtime = new ImporterLiteRuntime(poolFor(db) as Pool, {},
      { get: (source:string) => source==='source-b' ? adapter : undefined } as unknown as SourceRegistry,
      {upload:async()=>'unused',healthCheck:async()=>true,getProviderKey:()=> 'telegram'},
      {IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,BATCH_PAGE_DOWNLOAD_CONCURRENCY:1} as any);
    (runtime as any).dedupe={resolveWork:async()=>({workId:ids.p1a,status:'CONFIRMED_EXISTING'})};
    const first=await repo.claimMonitor('alternate',120);
    await (runtime as any).catalog(first,'alternate');
    expect((await db.query(`SELECT count(*)::int n FROM importer_lite_tasks WHERE work_id=$1 AND chapter_key='pt-br|default|chapter|200|-'`,[ids.p1a])).rows[0].n).toBe(1);
    latest=201;
    await db.query(`UPDATE importer_lite_tasks SET next_run_at=now()-interval '1 second' WHERE id=$1`,[first!.id]);
    await repo.reconcile(10);
    const second=await repo.claimMonitor('alternate',120);
    await (runtime as any).catalog(second,'alternate');
    expect((await db.query(`SELECT count(*)::int n FROM importer_lite_tasks WHERE work_id=$1 AND chapter_key='pt-br|default|chapter|201|-'`,[ids.p1a])).rows[0].n).toBe(1);
    expect((await db.query(`SELECT source,source_chapter_id FROM importer_lite_chapter_sources WHERE work_id=$1 AND canonical_chapter_key='pt-br|default|chapter|201|-'`,[ids.p1a])).rows)
      .toContainEqual({source:'source-b',source_chapter_id:'b-201'});
    await db.query(`UPDATE importer_lite_tasks SET next_run_at=now()-interval '1 second' WHERE id=$1`,[first!.id]);
    await repo.reconcile(10);
    const third=await repo.claimMonitor('alternate',120);
    await (runtime as any).catalog(third,'alternate');
    const checkpoint=(await db.query(`SELECT checkpoint FROM importer_lite_tasks WHERE id=$1`,[first!.id])).rows[0].checkpoint as any;
    expect(checkpoint.previousCheckedAt).toBeTruthy();
    expect(checkpoint.lastCheckedAt).toBeTruthy();
    expect(checkpoint.successfulChecksSinceTracking).toBe(3);

    // The original source later publishes the same canonical chapter. Its
    // provenance is retained, but the importer must not enqueue it twice.
    await db.query(`UPDATE importer_sources SET enabled=true WHERE id='source-a'`);
    expect(await repo.enrollConfirmedP0Mappings(ids.p1a,[{source:'source-a',sourceWorkId:'origin-100'}])).toBe(1);
    await db.query(`UPDATE importer_lite_tasks SET next_run_at=now()-interval '1 second' WHERE idempotency_key='lite:p0-monitor:source-a:origin-100'`);
    (runtime as any).registry={get:(source:string)=>source==='source-a'?{
      id:'source-a',fetchWorkSnapshot:async()=>({details:{sourceWorkId:'origin-100',title:'P1 A',slug:'p1-a',status:'ONGOING',language:'pt-BR'},
        chapters:[{sourceChapterId:'a-201',number:201,displayNumber:'201',title:'Chapter 201'}]})}:adapter};
    const origin=await repo.claimMonitor('original',120);
    await (runtime as any).catalog(origin,'original');
    expect((await db.query(`SELECT count(*)::int n FROM importer_lite_tasks WHERE work_id=$1 AND chapter_key='pt-br|default|chapter|201|-'`,[ids.p1a])).rows[0].n).toBe(1);
    expect((await db.query(`SELECT count(DISTINCT source)::int n FROM importer_lite_chapter_sources WHERE work_id=$1 AND canonical_chapter_key='pt-br|default|chapter|201|-'`,[ids.p1a])).rows[0].n).toBe(2);
  });

  it('materializes newly observed chapters from a P0 monitoring pass as P0, without rebucketing initial P1 work', async () => {
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P0', idempotencyKey: 'p0-catalog', payload: { source: 'source-a', sourceWorkId: 'work-a' } });
    const task = await repo.claim('monitor', 120);
    const adapter = {
      id: 'source-a',
      fetchWorkSnapshot: async () => ({
        details: { sourceWorkId: 'work-a', title: 'P1 A', slug: 'p1-a', status: 'ONGOING' as const, language: 'pt-BR' },
        chapters: [{ sourceChapterId: 'chapter-2', number: 2, displayNumber: '2', title: 'Two' }],
      }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 2, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'CONFIRMED_EXISTING' }) };
    await (runtime as any).catalog(task, 'monitor');
    const created = await db.query(`SELECT priority,base_priority FROM importer_lite_tasks WHERE idempotency_key='lite:chapter:' || $1 || ':pt-br|default|chapter|2|-'`, [ids.p1a]);
    expect(created.rows[0]).toEqual({ priority: 1, base_priority: 1 });
  });

  it('repairs only manifest-proven historical gaps and preserves textual chapter identities', async () => {
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P1',
      idempotencyKey: 'manifest-evidence-only', payload: { source: 'source-a', sourceWorkId: 'manifest-evidence-only' } });
    const task = await repo.claim('catalog-worker', 120);
    const adapter = {
      id: 'source-a',
      fetchWorkSnapshot: async () => ({
        details: { sourceWorkId: 'manifest-evidence-only', title: 'P1 A', slug: 'p1-a', status: 'COMPLETED' as const, language: 'pt-BR' },
        chapters: [
          { sourceChapterId: 'one', number: 1, displayNumber: '1', title: 'One' },
          { sourceChapterId: 'decimal-one', number: 12.1, displayNumber: '12.1', title: 'Part one' },
          { sourceChapterId: 'decimal-ten', number: 12.1, displayNumber: '12.10', title: 'Part ten' },
          { sourceChapterId: 'two-hundred', number: 200, displayNumber: '200', title: 'Two hundred' },
        ],
      }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'CONFIRMED_EXISTING' }) };

    await (runtime as any).catalog(task, 'catalog-worker');

    const materialized = await db.query(`SELECT chapter_key FROM importer_lite_tasks
      WHERE task_type='IMPORT_CHAPTER' AND work_id=$1 ORDER BY chapter_key`, [ids.p1a]);
    expect(materialized.rows.map((row: any) => row.chapter_key)).toEqual([
      'pt-br|default|chapter|12.10|-',
      'pt-br|default|chapter|12.1|-',
      'pt-br|default|chapter|1|-',
      'pt-br|default|chapter|200|-',
    ]);
    const orphanedConfirmed = await db.query(`SELECT count(*)::int AS count
      FROM importer_lite_chapter_sources source
      LEFT JOIN importer_lite_chapters chapter
        ON chapter.work_id=source.work_id AND chapter.canonical_chapter_key=source.canonical_chapter_key
      LEFT JOIN importer_lite_tasks task
        ON task.idempotency_key='lite:chapter:' || source.work_id::text || ':' || source.canonical_chapter_key
       AND task.task_type='IMPORT_CHAPTER'
      WHERE source.confirmed AND source.has_chapter AND chapter.work_id IS NULL AND task.id IS NULL`);
    expect(orphanedConfirmed.rows[0].count).toBe(0);
    // A numerical span from 1 to 200 is not evidence for chapters 2–199.
    expect(materialized.rows.some((row: any) => row.chapter_key.includes('|2|-'))).toBe(false);
  });

  it('keeps a manifest durable but defers chapter admission until the work has a public presentation', async () => {
    await db.query(`UPDATE works SET cover_id=NULL WHERE id=$1`, [ids.p1a]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P1',
      idempotencyKey: 'presentation-gated-catalog', payload: { source: 'source-a', sourceWorkId: 'presentation-gated' } });
    const task = await repo.claim('catalog-worker', 120);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'already-queued', priority: 'P1', idempotencyKey: 'presentation-already-queued' });
    const adapter = {
      id: 'source-a',
      fetchWorkSnapshot: async () => ({
        details: { sourceWorkId: 'presentation-gated', title: 'P1 A', slug: 'p1-a', status: 'COMPLETED' as const, language: 'pt-BR' },
        chapters: [{ sourceChapterId: 'presentation-gated-1', number: 1, displayNumber: '1', title: 'One' }],
      }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'EXISTING_MAPPING' }) };

    await (runtime as any).catalog(task, 'catalog-worker');

    expect((await db.query(`SELECT state,reason,checkpoint->>'presentationIncomplete' AS incomplete
      FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toEqual({ state: 'RETRY_WAIT', reason: 'WORK_PRESENTATION_INCOMPLETE', incomplete: 'true' });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_chapter_sources
      WHERE work_id=$1 AND source_chapter_id='presentation-gated-1'`, [ids.p1a])).rows[0].count).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE task_type='IMPORT_CHAPTER' AND work_id=$1`, [ids.p1a])).rows[0].count).toBe(1);
    expect((await db.query(`SELECT state,reason,checkpoint->>'presentationPreviousReason' AS previous_reason
      FROM importer_lite_tasks WHERE idempotency_key='presentation-already-queued'`)).rows[0])
      .toEqual({ state: 'BLOCKED', reason: 'WORK_PRESENTATION_INCOMPLETE', previous_reason: null });
  });

  it('admits a presentable unpublished work while leaving stale unpresentable P1 imports parked', async () => {
    await db.query(`UPDATE works SET published=false,cover_id=NULL WHERE id=$1`, [ids.p1a]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'stale-unpublishable', priority: 'P1', idempotencyKey: 'stale-unpublishable' });
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, workKey: `work:${ids.p1b}`,
      chapterKey: 'presentable-unpublished', priority: 'P1', idempotencyKey: 'presentable-unpublished' });

    const claimed = await repo.claim('presentable-unpublished-worker', 120);
    expect(claimed).toMatchObject({ taskType: 'IMPORT_CHAPTER', workId: ids.p1b, idempotencyKey: 'presentable-unpublished' });
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE idempotency_key='stale-unpublishable'`)).rows[0])
      .toEqual({ state: 'READY' });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_ready_works WHERE work_key=$1`, [`work:${ids.p1a}`])).rows[0])
      .toEqual({ count: 0 });
  });

  it('coalesces duplicate canonical legacy mappings without collapsing distinct decimal chapters', async () => {
    await db.query(`INSERT INTO chapters(id,work_id,number,display_number,published_at) VALUES
      ('00000000-0000-4000-8000-000000000011',$1,12.1,'12.10',now()),
      ('00000000-0000-4000-8000-000000000012',$1,12.1,'12.10',now())`, [ids.p1a]);
    await db.query(`INSERT INTO importer_chapter_mappings(source,source_chapter_id,chapter_id) VALUES
      ('source-a','source-12-10-a','00000000-0000-4000-8000-000000000011'),
      ('source-a','source-12-10-b','00000000-0000-4000-8000-000000000012')`);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P1', idempotencyKey: 'duplicate-canonical-catalog', payload: { source: 'source-a', sourceWorkId: 'duplicate-canonical-work' } });
    const task = await repo.claim('catalog-worker', 120);
    const adapter = {
      id: 'source-a',
      fetchWorkSnapshot: async () => ({
        details: { sourceWorkId: 'duplicate-canonical-work', title: 'P1 A', slug: 'p1-a', status: 'COMPLETED' as const, language: 'pt-BR' },
        chapters: [
          { sourceChapterId: 'source-12-10-a', number: 12.1, displayNumber: '12.10', title: 'First provider label' },
          { sourceChapterId: 'source-12-10-b', number: 12.1, displayNumber: '12.10', title: 'Second provider label' },
          { sourceChapterId: 'source-12-1', number: 12.1, displayNumber: '12.1', title: 'Decimal chapter' },
        ],
      }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 2, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'CONFIRMED_EXISTING' }) };
    await (runtime as any).catalog(task, 'catalog-worker');

    expect((await db.query(`SELECT state,monitoring FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toEqual({ state: 'RETRY_WAIT', monitoring: true });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_chapter_sources WHERE work_id=$1 AND source='source-a'`, [ids.p1a])).rows[0].count).toBe(3);
    expect((await db.query(`SELECT chapter_id FROM importer_lite_chapters WHERE work_id=$1 AND canonical_chapter_key='pt-br|default|chapter|12.10|-'`, [ids.p1a])).rows[0].chapter_id)
      .toBe('00000000-0000-4000-8000-000000000011');
    expect(new Set((await db.query(`SELECT chapter_key FROM importer_lite_tasks WHERE work_id=$1 AND task_type='IMPORT_CHAPTER'`, [ids.p1a])).rows.map((row: any) => row.chapter_key)))
      .toEqual(new Set(['pt-br|default|chapter|12.1|-']));
  });

  it('binds a zero-decimal legacy Reader chapter before another source can publish a duplicate', async () => {
    await db.query(`INSERT INTO chapters
      (id,work_id,number,display_number,canonical_chapter_key,chapter_order_key,chapter_kind,edition,language,title,published_at)
      VALUES ($1::uuid,$2::uuid,28,'28.00','legacy:whole-28','n|000000000028|000000000000|00||-','chapter','default','und','Capítulo 28',now())`,
    [ids.legacy, ids.p1a]);
    await db.query(`INSERT INTO importer_chapter_mappings(source,source_chapter_id,chapter_id)
      VALUES('source-a','legacy-whole-28',$1::uuid)`, [ids.legacy]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      priority: 'P1', idempotencyKey: 'zero-decimal-alternative-source',
      payload: { source: 'source-b', sourceWorkId: 'alternative-work' } });
    const task = await repo.claim('catalog-worker', 120);
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => ({ id: 'source-b', fetchWorkSnapshot: async () => ({
        details: { sourceWorkId: 'alternative-work', title: 'P1 A', slug: 'p1-a', status: 'COMPLETED' as const, language: 'pt-BR' },
        chapters: [{ sourceChapterId: 'alternative-whole-28', number: 28, displayNumber: '28', title: 'A different provider label' }],
      }) }) } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 2, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'CONFIRMED_EXISTING' }) };
    await (runtime as any).catalog(task, 'catalog-worker');

    expect((await db.query(`SELECT chapter_id FROM importer_lite_chapters
      WHERE work_id=$1::uuid AND canonical_chapter_key='pt-br|default|chapter|28|-'`, [ids.p1a])).rows[0].chapter_id)
      .toBe(ids.legacy);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE work_id=$1::uuid AND task_type='IMPORT_CHAPTER'`, [ids.p1a])).rows[0].count).toBe(0);
  });

  it('retires only a corroborated published X.00 duplicate without deleting either page manifest', async () => {
    const canonical = '00000000-0000-4000-8000-000000000028';
    const retired = '00000000-0000-4000-8000-000000000029';
    const canonicalMedia = '00000000-0000-4000-8000-000000000030';
    const retiredMedia = '00000000-0000-4000-8000-000000000031';
    await db.query(`INSERT INTO chapters
      (id,work_id,number,display_number,canonical_chapter_key,chapter_order_key,chapter_kind,edition,language,title,published_at)
      VALUES
      ($1::uuid,$3::uuid,28,'28','pt-br|default|chapter|28|-','n|000000000028|000000000000||||-','chapter','default','pt-BR','Capítulo 28',now()),
      ($2::uuid,$3::uuid,28,'28.00','legacy:whole-28','n|000000000028|000000000000|00||-','chapter','default','und','Capítulo 28',now())`, [canonical,retired,ids.p1a]);
    await db.query(`INSERT INTO media(id,mime,width,height,bytes,sha256,storage_ready,status) VALUES
      ($1::uuid,'image/jpeg',800,1200,3000,'canonical',true,'ACTIVE'),
      ($2::uuid,'image/jpeg',800,1200,3000,'retired',true,'ACTIVE')`, [canonicalMedia,retiredMedia]);
    await db.query(`INSERT INTO pages(chapter_id,position,media_id,width,height) VALUES
      ($1::uuid,1,$3::uuid,800,1200),($2::uuid,1,$4::uuid,800,1200)`, [canonical,retired,canonicalMedia,retiredMedia]);
    await db.query(`INSERT INTO importer_chapter_mappings(source,source_chapter_id,chapter_id)
      VALUES('source-a','legacy-zero-28',$1::uuid)`, [retired]);

    await db.exec(zeroDecimalChapterIdentityMigration);

    expect((await db.query(`SELECT published_at IS NOT NULL AS published FROM chapters WHERE id=$1::uuid`, [canonical])).rows[0].published).toBe(true);
    expect((await db.query(`SELECT published_at IS NULL AS retired FROM chapters WHERE id=$1::uuid`, [retired])).rows[0].retired).toBe(true);
    expect((await db.query(`SELECT count(*)::int AS count FROM pages WHERE chapter_id IN ($1::uuid,$2::uuid)`, [canonical,retired])).rows[0].count).toBe(2);
    expect((await db.query(`SELECT chapter_id FROM importer_chapter_mappings WHERE source='source-a' AND source_chapter_id='legacy-zero-28'`)).rows[0].chapter_id)
      .toBe(canonical);
    expect((await db.query(`SELECT canonical_page_count,retired_page_count FROM importer_chapter_identity_merges WHERE retired_chapter_id=$1::uuid`, [retired])).rows[0])
      .toEqual({ canonical_page_count: 1, retired_page_count: 1 });
  });

  it('reopens only legacy P0 monitors blocked by the discovery-only switch', async () => {
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS catalog_discovery_enabled boolean NOT NULL DEFAULT true`);
    await db.query(`UPDATE importer_sources SET catalog_discovery_enabled=false WHERE id='source-a'`);
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:source-a:legacy-p0', priority: 'P2', monitoring: true,
      idempotencyKey: 'legacy-p0-monitor', payload: { source: 'source-a', sourceWorkId: 'legacy-p0' } });
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:source-b:unrelated', priority: 'P2', monitoring: true,
      idempotencyKey: 'unrelated-monitor', payload: { source: 'source-b', sourceWorkId: 'unrelated' } });
    await db.query(`UPDATE importer_lite_tasks SET state='BLOCKED',attempts=4,reason='SOURCE_NOT_READY:ACTIVE',
      next_run_at=now()+interval '1 hour' WHERE idempotency_key IN ('legacy-p0-monitor','unrelated-monitor')`);

    expect(await repo.reopenP0MonitorsBlockedByDiscoveryPolicy()).toBe(1);
    expect((await db.query(`SELECT state,attempts,reason FROM importer_lite_tasks WHERE idempotency_key='legacy-p0-monitor'`)).rows[0])
      .toEqual({ state: 'READY', attempts: 0, reason: null });
    expect((await db.query(`SELECT state,attempts,reason FROM importer_lite_tasks WHERE idempotency_key='unrelated-monitor'`)).rows[0])
      .toEqual({ state: 'BLOCKED', attempts: 4, reason: 'SOURCE_NOT_READY:ACTIVE' });
  });

  it('turns only a genuinely new monitor observation into P0 and returns the watch to its bounded lane', async () => {
    // This switch applies to source-wide P2 discovery.  It must not suppress
    // an existing-work P0 observation with a known provider mapping.
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS catalog_discovery_enabled boolean NOT NULL DEFAULT true`);
    await db.query(`UPDATE importer_sources SET catalog_discovery_enabled=false WHERE id='source-a'`);
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,order_key,confirmed)
      VALUES($1,'pt-br|default|chapter|10|-','source-a','known-10','pt-BR','default','n|000000000010|000000000000||||-',true)`, [ids.p1a]);
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:source-a:monitored-work', priority: 'P2', monitoring: true,
      idempotencyKey: 'p0-lane-watch', payload: { source: 'source-a', sourceWorkId: 'monitored-work' } });
    // This is the historical P0 regression: no normal chapter slot is spent
    // merely checking an unchanged/new-release watch.
    expect(await repo.claim('normal-worker', 120)).toBeNull();
    const watch = await repo.claimMonitor('p0-monitor', 120);
    expect(watch).toMatchObject({ idempotencyKey: 'p0-lane-watch', monitoring: true, priority: 'P2' });
    const adapter = {
      id: 'source-a',
      fetchWorkSnapshot: async () => ({
        details: { sourceWorkId: 'monitored-work', title: 'P1 A', slug: 'p1-a', status: 'ONGOING' as const, language: 'pt-BR' },
        chapters: [{ sourceChapterId: 'release-11', number: 11, displayNumber: '11', title: 'Eleven' }],
      }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'CONFIRMED_EXISTING' }) };
    await (runtime as any).catalog(watch, 'p0-monitor');
    expect((await db.query(`SELECT priority,base_priority FROM importer_lite_tasks
      WHERE idempotency_key='lite:chapter:' || $1 || ':pt-br|default|chapter|11|-'`, [ids.p1a])).rows[0])
      .toEqual({ priority: 1, base_priority: 1 });
    expect((await db.query(`SELECT state,priority,base_priority,monitoring,checkpoint->>'lastNewReleaseCount' AS releases
      FROM importer_lite_tasks WHERE idempotency_key='p0-lane-watch'`)).rows[0])
      .toEqual({ state: 'RETRY_WAIT', priority: 3, base_priority: 3, monitoring: true, releases: '1' });
    // Discovery created this catalog task before there was a canonical work.
    // Catalog must attach that identity so a later panel action can wake it.
    expect((await db.query(`SELECT work_id FROM importer_lite_tasks WHERE idempotency_key='p0-lane-watch'`)).rows[0].work_id).toBe(ids.p1a);
    await db.query(`INSERT INTO importer_staff_requests(work_id,status) VALUES($1,'ACTIVE')`, [ids.p1a]);
    expect((await db.query(`SELECT state,priority,monitoring FROM importer_lite_tasks WHERE idempotency_key='p0-lane-watch'`)).rows[0])
      .toEqual({ state: 'READY', priority: 0, monitoring: false });
    expect((await db.query(`SELECT priority FROM importer_lite_ready_works WHERE work_key='source:source-a:monitored-work'`)).rows[0])
      .toEqual({ priority: 0 });
    // The newly materialised chapter is also STAFF, so either work may win
    // fair selection; the essential property is that the awakened monitor no
    // longer hides behind a null work id or delays the panel request.
    expect(await repo.claim('staff-panel-worker', 120)).toMatchObject({ priority: 'STAFF' });
  });

  it('keeps a newly revealed historical chapter as P1 while P0 monitors a newer release', async () => {
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,order_key,confirmed)
      VALUES($1,'pt-br|default|chapter|100|-','source-a','known-100','pt-BR','default','n|000000000100|000000000000||||-',true)`, [ids.p1a]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P0', idempotencyKey: 'p0-repair-catalog', payload: { source: 'source-a', sourceWorkId: 'work-a' } });
    const task = await repo.claim('monitor', 120);
    const adapter = {
      id: 'source-a',
      fetchWorkSnapshot: async () => ({
        details: { sourceWorkId: 'work-a', title: 'P1 A', slug: 'p1-a', status: 'ONGOING' as const, language: 'pt-BR' },
        chapters: [
          { sourceChapterId: 'revealed-10', number: 10, displayNumber: '10', title: 'Ten' },
          { sourceChapterId: 'release-101', number: 101, displayNumber: '101', title: 'One Hundred One' },
        ],
      }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 2, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'CONFIRMED_EXISTING' }) };
    await (runtime as any).catalog(task, 'monitor');
    const priorities = await db.query(`SELECT payload->>'displayNumber' AS display_number,priority,base_priority
      FROM importer_lite_tasks WHERE idempotency_key IN ('lite:chapter:' || $1 || ':pt-br|default|chapter|10|-',
        'lite:chapter:' || $1 || ':pt-br|default|chapter|101|-') ORDER BY display_number`, [ids.p1a]);
    expect(priorities.rows).toEqual([
      { display_number: '10', priority: 2, base_priority: 2 },
      { display_number: '101', priority: 1, base_priority: 1 },
    ]);
  });

  it('keeps a first full manifest from turning chapters 2–36 into moving P0 gaps', async () => {
    await db.query(`INSERT INTO chapters(id,work_id,number,display_number,canonical_chapter_key)
      VALUES('00000000-0000-4000-8000-000000000001',$1,1,'1','pt-br|default|chapter|1|-')`, [ids.p1a]);
    await db.query(`UPDATE chapters SET published_at=now() WHERE id='00000000-0000-4000-8000-000000000001'`);
    await db.query(`INSERT INTO importer_chapter_mappings(source,source_chapter_id,chapter_id)
      VALUES('source-a','release-1','00000000-0000-4000-8000-000000000001')`);
    await db.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key)
      VALUES($1,'pt-br|default|chapter|1|-','00000000-0000-4000-8000-000000000001','1','n|000000000001|000000000000|||')`, [ids.p1a]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P0',
      idempotencyKey: 'full-manifest-p0', payload: { source: 'source-a', sourceWorkId: 'work-a' } });
    const task = await repo.claim('manifest-frontier', 120);
    const adapter = {
      id: 'source-a',
      fetchWorkSnapshot: async () => ({
        details: { sourceWorkId: 'work-a', title: 'P1 A', slug: 'p1-a', status: 'ONGOING' as const, language: 'pt-BR' },
        chapters: Array.from({ length: 37 }, (_, index) => ({
          sourceChapterId: `release-${index + 1}`, number: index + 1,
          displayNumber: String(index + 1), title: `Chapter ${index + 1}`,
        })),
      }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'CONFIRMED_EXISTING' }) };
    await (runtime as any).catalog(task, 'manifest-frontier');

    const p1 = await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE work_id=$1 AND task_type='IMPORT_CHAPTER' AND priority=2
        AND payload ? 'historicalManifestRepair'`, [ids.p1a]);
    expect(p1.rows[0].count).toBe(35);
    expect((await db.query(`SELECT priority FROM importer_lite_tasks
      WHERE idempotency_key='lite:chapter:' || $1 || ':pt-br|default|chapter|37|-'`, [ids.p1a])).rows[0].priority).toBe(2);
    expect((await repo.claim('manifest-gap-worker', 120))?.chapterKey).toBe('pt-br|default|chapter|2|-');
  });

  it('passes complete, sanitized source metadata to the conservative catalog engine', async () => {
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:source-a:metadata', priority: 'P2', idempotencyKey: 'metadata-catalog', payload: { source: 'source-a', sourceWorkId: 'metadata-work' } });
    const task = await repo.claim('catalog', 120);
    const adapter = {
      id: 'source-a',
      fetchWorkSnapshot: async () => ({
        details: {
          sourceWorkId: 'metadata-work', title: 'Metadata Work', slug: 'metadata-work',
          synopsis: '<p>Primeiro parágrafo seguro.</p><p>Segundo parágrafo.</p>', author: 'Author', artist: 'Artist',
          kind: 'PORNHWA' as const, status: 'ONGOING' as const, year: 2024, ageRating: 18,
          alternativeTitles: ['Título alternativo', 'Original title'], genres: ['MILF', 'Yaoi', 'Hentai'],
          raw: { content_rating: 'ADULT_18' }, language: 'pt-BR',
        }, chapters: [],
      }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    let candidate: any;
    (runtime as any).dedupe = { resolveWork: async (value: any) => { candidate = value; return { workId: ids.p1a, status: 'CONFIRMED_EXISTING' }; } };
    await (runtime as any).catalog(task, 'catalog');
    expect(candidate).toMatchObject({
      kind: 'PORNHWA', status: 'ONGOING', year: 2024, ageRating: 18, contentRating: 'ADULT_18',
      aliases: ['Título alternativo', 'Original title'], genres: ['MILF', 'Yaoi', 'Hentai'],
    });
    expect(candidate.synopsis).toContain('Primeiro parágrafo seguro.');
    expect(candidate.synopsis).toContain('\n');
  });

  it('complements a new work from a second healthy source during onboarding without a retroactive task', async () => {
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS catalog_discovery_enabled boolean NOT NULL DEFAULT true`);
    await db.query(`UPDATE importer_sources SET catalog_discovery_enabled=false WHERE id='source-b'`);
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:source-a:solo', priority: 'P2',
      idempotencyKey: 'new-work-cross-source', payload: { source: 'source-a', sourceWorkId: 'solo' } });
    const task = await repo.claim('cross-source-catalog', 120);
    const searched: string[] = [];
    let attached: any;
    const sourceA = {
      id: 'source-a',
      fetchWorkSnapshot: async () => ({
        details: {
          sourceWorkId: 'solo', title: 'Nivelamento Solo', slug: 'nivelamento-solo',
          alternativeTitles: ['Solo Leveling'], genres: ['Ação'], status: 'ONGOING' as const,
        }, chapters: [],
      }),
    };
    const sourceB = {
      id: 'source-b',
      searchWorks: async (query: string) => {
        searched.push(query);
        return query === 'Nivelamento Solo' ? [{ sourceWorkId: 'only-i-level-up', title: 'Solo Leveling', slug: 'solo-leveling' }] : [];
      },
      fetchWorkDetails: async () => ({
        sourceWorkId: 'only-i-level-up', title: 'Solo Leveling', slug: 'solo-leveling',
        alternativeTitles: ['Solo Leveling', 'Only I Level Up'], genres: ['Ação', 'Fantasia'], status: 'ONGOING' as const,
      }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: (id: string) => id === 'source-a' ? sourceA : id === 'source-b' ? sourceB : undefined,
        getAll: () => [sourceA, sourceB] } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = {
      resolveWork: async () => ({ workId: ids.p1a, status: 'NEW_WORK', metadataDelta: { aliasesAdded: 1, tagsAdded: 1 } }),
      attachVerifiedMapping: async (candidate: any, workId: string, source: string) => {
        attached = { candidate, workId, source };
        return { workId, status: 'EXISTING_MAPPING', metadataDelta: { aliasesAdded: 1, tagsAdded: 1 } };
      },
    };

    await (runtime as any).catalog(task, 'cross-source-catalog');

    expect(searched).toContain('Nivelamento Solo');
    expect(attached).toMatchObject({
      workId: ids.p1a, source: 'source-b',
      candidate: { sourceWorkId: 'only-i-level-up', aliases: ['Only I Level Up'], genres: ['Ação', 'Fantasia'] },
    });
    const complementary = await db.query(`SELECT aliases_state,tags_state,aliases_added,tags_added
      FROM importer_lite_metadata_enrichment WHERE source='source-b' AND source_work_id='only-i-level-up'`);
    expect(complementary.rows[0]).toMatchObject({ aliases_state: 'ENRICHED', tags_state: 'ENRICHED', aliases_added: 1, tags_added: 1 });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE payload ? 'metadataSourceRefresh' AND payload->>'source'='source-b'`)).rows[0].count).toBe(0);
    const onboardingV7 = await db.query(`SELECT state,checkpoint->>'coverage' AS coverage,checkpoint->'checkedSources' AS checked,checkpoint->'sourceProbes' AS probes
      FROM importer_lite_tasks WHERE idempotency_key=$1`, [`lite:metadata:cross-source:v7:${ids.p1a}`]);
    expect(onboardingV7.rows[0]).toMatchObject({
      state: 'DONE',
      coverage: 'CROSS_SOURCE_PROBES_COMPLETE',
      checked: ['source-a', 'source-b'],
      probes: {
        'source-b': expect.objectContaining({
          outcome: 'MATCHED_ENRICHED',
          sourceWorkId: 'only-i-level-up',
          primaryTitle: 'Solo Leveling',
        }),
      },
    });
  });

  it('enriches explicit historical mapping aliases and tags without calling the source adapter', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000112','source-a','stored-metadata',$1::uuid,
        'p1-a','P1 A',$2::jsonb,'SYNCED')`, [ids.p1a, JSON.stringify({
      // Hipercool historically stored the adapter's explicit arrays as JSON
      // strings. Enrichment must recover them without source networking.
      alternative_titles: '["P1 Historical Alias", "P1 A"]',
      categories: '[{"name":"MILF"}, {"name":"Vanilla"}]',
    })]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P2',
      idempotencyKey: 'stored-metadata-enrichment',
      payload: { source: 'source-a', sourceWorkId: 'stored-metadata', metadataEnrichment: true } });
    const task = await repo.claim('metadata-worker', 120);
    const registry = { get: () => { throw new Error('metadata enrichment must not call an adapter'); } } as unknown as SourceRegistry;
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, registry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    let candidate: any;
    (runtime as any).dedupe = { resolveWork: async (value: any) => {
      candidate = value;
      return { workId: ids.p1a, status: 'EXISTING_MAPPING', metadataDelta: { aliasesAdded: 1, tagsAdded: 2 } };
    } };

    await (runtime as any).catalog(task, 'metadata-worker');
    expect(candidate).toMatchObject({ aliases: ['P1 Historical Alias'], genres: ['MILF', 'Vanilla'] });
    expect((await db.query(`SELECT state,checkpoint FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0]).toMatchObject({
      state: 'DONE', checkpoint: { aliasesAdded: 1, tagsAdded: 2 },
    });
  });

  it('retries transient isolated-pool contention without consuming the mapping retry budget', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    const outcomes: any[] = [];
    (runtime as any).sourceMetadataPool = { query: async () => { throw new Error('timeout exceeded when trying to connect'); } };
    (runtime as any).queue.finish = async (_task: any, _worker: string, outcome: any) => { outcomes.push(outcome); return true; };
    await (runtime as any).enrichStoredMappingMetadata({ attempts: 4, maxAttempts: 4, checkpoint: {},
      payload: { metadataEnrichmentVersion: 4 } } as any, 'metadata-worker', 'source-a', 'transient');

    expect(outcomes[0]).toMatchObject({ state: 'RETRY_WAIT', reason: 'METADATA_ENRICHMENT_DB_CONTENTION',
      resetAttempts: true, checkpoint: { metadataDbContentionCount: 1 } });
  });

  it('treats an already-attached secondary source title as an additive alias', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000113','source-a','secondary-title',$1::uuid,
        'p1-secondary','P1 Secondary Title','{}'::jsonb,'SYNCED')`, [ids.p1a]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P2',
      idempotencyKey: 'stored-secondary-title-enrichment',
      payload: { source: 'source-a', sourceWorkId: 'secondary-title', metadataEnrichment: true } });
    const task = await repo.claim('metadata-worker', 120);
    const registry = { get: () => { throw new Error('metadata enrichment must not call an adapter'); } } as unknown as SourceRegistry;
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, registry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    let candidate: any;
    (runtime as any).dedupe = { resolveWork: async (value: any) => {
      candidate = value;
      return { workId: ids.p1a, status: 'EXISTING_MAPPING' };
    } };

    await (runtime as any).catalog(task, 'metadata-worker');

    expect(candidate).toMatchObject({ title: 'P1 A', aliases: ['P1 Secondary Title'], genres: [] });
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0].state).toBe('DONE');
  });

  it('does not let a V4 machine source title bypass the alias admission guard', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000114','source-a','machine-title',$1::uuid,
        '10123','10123',$2::jsonb,'SYNCED')`, [ids.p1a, JSON.stringify({ alternativeTitles: ['Explicit Human Title'] })]);
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P2',
      idempotencyKey: 'lite:metadata:v4:source-a:machine-title',
      payload: { source: 'source-a', sourceWorkId: 'machine-title', metadataEnrichment: true, metadataEnrichmentVersion: 4 } });
    const task = await repo.claimStoredMetadataReplay('metadata-worker', 120, 4);
    const registry = { get: () => { throw new Error('metadata enrichment must not call an adapter'); } } as unknown as SourceRegistry;
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, registry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    let candidate: any;
    (runtime as any).historicalMetadataDedupe = { resolveWork: async (value: any) => {
      candidate = value;
      return { workId: ids.p1a, status: 'EXISTING_MAPPING', metadataDelta: { aliasesAdded: 1, tagsAdded: 0 } };
    } };

    await (runtime as any).catalog(task, 'metadata-worker');

    expect(candidate).toMatchObject({ title: 'P1 A', aliases: ['Explicit Human Title'] });
    expect(candidate.aliases).not.toContain('10123');
  });

  it('re-fetches source metadata, persists raw provider fields, and classifies empty fields per mapping', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000211','source-a','source-refetch',$1::uuid,
        'p1-a','P1 A','{}'::jsonb,'SYNCED')`, [ids.p1a]);
    expect(await repo.refillSourceMetadataEnrichment(2)).toBe(1);
    const task = await repo.claimSourceMetadataRefresh('metadata-source-worker',60);
    expect(task?.payload).toMatchObject({ metadataSourceRefresh:true,source:'source-a',sourceWorkId:'source-refetch' });
    let fetched = 0;
    const adapter = { id:'source-a',baseUrl:'https://source-a.example',fetchWorkDetails:async () => {
      fetched++;
      return { sourceWorkId:'source-refetch',title:'P1 A',slug:'p1-a',alternativeTitles:['Título nativo'],
        genres:['MILF','Vanilla'],raw:{provider_id:'source-refetch',genres:['MILF','Vanilla']} };
    } };
    await db.query(`INSERT INTO tags(id,name,slug) VALUES
      -- Existing production taxonomies can carry an older slug for the same
      -- normalized display name. Historical refetch must attach it instead
      -- of treating that local uniqueness collision as a source failure.
      ('00000000-0000-4000-8000-000000000301','MILF','legacy-milf'),
      ('00000000-0000-4000-8000-000000000302','Vanilla','vanilla')`);
    const directCatalogClient = { sql: (text: string, values?: unknown[]) => db.query(text, values) };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, directCatalogClient, { get: () => adapter } as unknown as SourceRegistry,
      { upload:async ()=>'unused',healthCheck:async()=>true,getProviderKey:()=>'telegram' },
      { IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY:1,SOURCE_OPERATION_TIMEOUT_MS:2_000 } as any,
    );
    await (runtime as any).catalog(task,'metadata-source-worker',{ claimSqlMs: 1, queueDwellMs: 0 });

    const mapping = await db.query(`SELECT metadata,last_synced_at FROM importer_work_mappings WHERE source='source-a' AND source_work_id='source-refetch'`);
    const classification = await db.query(`SELECT aliases_state,tags_state,aliases_added,tags_added,version FROM importer_lite_metadata_enrichment
      WHERE source='source-a' AND source_work_id='source-refetch'`);
    const completed = (await db.query(`SELECT state,checkpoint FROM importer_lite_tasks WHERE id=$1`,[task!.id])).rows[0];
    expect(fetched).toBe(1);
    expect(mapping.rows[0].metadata).toMatchObject({ provider_id:'source-refetch',alternativeTitles:['Título nativo'],genres:['MILF','Vanilla'] });
    expect(mapping.rows[0].last_synced_at).not.toBeNull();
    expect(classification.rows[0]).toMatchObject({ aliases_state:'ENRICHED',tags_state:'ENRICHED',version:3 });
    expect(completed.state).toBe('DONE');
    expect(completed.checkpoint.metadataTiming).toMatchObject({
      claimSqlMs: expect.any(Number), mappingAndPolicyQueryMs: expect.any(Number),
      rateLimitWaitMs: expect.any(Number), httpMs: expect.any(Number), parseNormalizeMs: expect.any(Number),
      metadataPersistenceWaitMs: expect.any(Number), mergePersistenceMs: expect.any(Number),
      totalBeforeFinalizeMs: expect.any(Number),
    });
    expect((await db.query(`SELECT aliases FROM works WHERE id=$1::uuid`,[ids.p1a])).rows[0].aliases)
      .toContain('Título nativo');
    expect((await db.query(`SELECT count(*)::int AS count FROM work_tags WHERE work_id=$1::uuid`,[ids.p1a])).rows[0].count)
      .toBe(2);
  });

  it('classifies a source alias equal to the primary title as NO_ALIASES, not PARSER_ERROR', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000213','source-a','same-title-alias',$1::uuid,
        'p1-a','P1 A','{}'::jsonb,'SYNCED')`, [ids.p1a]);
    await repo.enqueue({ taskType:'CATALOG',workId:ids.p1a,workKey:`work:${ids.p1a}`,priority:'P2',
      idempotencyKey:'source-same-title-alias',payload:{source:'source-a',sourceWorkId:'same-title-alias',metadataSourceRefresh:true} });
    const task=await repo.claimSourceMetadataRefresh('metadata-same-title-worker',60);
    const adapter={id:'source-a',baseUrl:'https://source-a.example',fetchWorkDetails:async()=>({
      sourceWorkId:'same-title-alias',title:'P1 A',slug:'p1-a',alternativeTitles:['P1 A'],genres:[],
      raw:{alternativeTitles:['P1 A']},
    })};
    const runtime=new ImporterLiteRuntime(
      poolFor(db) as Pool,{}, {get:()=>adapter} as unknown as SourceRegistry,
      {upload:async()=>'unused',healthCheck:async()=>true,getProviderKey:()=>'telegram'},
      {IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY:1,SOURCE_OPERATION_TIMEOUT_MS:2_000} as any,
    );
    (runtime as any).dedupe={mergeKnownMappedWork:async()=>({workId:ids.p1a,status:'EXISTING_MAPPING'})};
    (runtime as any).historicalMetadataDedupe = (runtime as any).dedupe;
    await (runtime as any).catalog(task,'metadata-same-title-worker',{claimSqlMs:1,queueDwellMs:0});
    const result=(await db.query(`SELECT aliases_state,tags_state FROM importer_lite_metadata_enrichment
      WHERE source='source-a' AND source_work_id='same-title-alias'`)).rows[0];
    expect(result).toMatchObject({aliases_state:'NO_ALIASES',tags_state:'NO_TAGS'});
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`,[task!.id])).rows[0].state).toBe('DONE');
  });

  it('classifies source login/challenge pages as external blocks instead of parser errors', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1::uuid`, [ids.p1a]);
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000212','source-a','challenge-page',$1::uuid,
        'p1-a','P1 A','{}'::jsonb,'SYNCED')`, [ids.p1a]);
    expect(await repo.refillSourceMetadataEnrichment(1)).toBe(1);
    const task = await repo.claimSourceMetadataRefresh('metadata-challenge-worker',60);
    const adapter = { id:'source-a',baseUrl:'https://source-a.example',fetchWorkDetails:async () => ({
      sourceWorkId:'challenge-page',title:'Please login',slug:'login',raw:{},
    }) };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload:async ()=>'unused',healthCheck:async()=>true,getProviderKey:()=>'telegram' },
      { IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY:1,SOURCE_OPERATION_TIMEOUT_MS:2_000 } as any,
    );
    (runtime as any).dedupe = { mergeKnownMappedWork:async () => ({ workId:null,status:'FAILED',
      reason:'INVALID_EDITORIAL_TITLE: Title is an authentication or login prompt' }) };
    (runtime as any).historicalMetadataDedupe = (runtime as any).dedupe;

    await (runtime as any).catalog(task,'metadata-challenge-worker');

    const status = await db.query(`SELECT aliases_state,tags_state,next_retry_at FROM importer_lite_metadata_enrichment
      WHERE source='source-a' AND source_work_id='challenge-page'`);
    expect(status.rows[0].aliases_state).toBe('EXTERNAL_BLOCKED');
    expect(status.rows[0].tags_state).toBe('EXTERNAL_BLOCKED');
    expect(status.rows[0].next_retry_at).not.toBeNull();
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks WHERE id=$1`,[task!.id])).rows[0])
      .toMatchObject({ state:'RETRY_WAIT',reason:'METADATA_SOURCE_EXTERNAL:METADATA_SOURCE_MAPPING_CONFLICT:INVALID_EDITORIAL_TITLE: Title is an authentication or login prompt' });
  });

  it('probes one expired Turnstile cooldown and keeps it parked when the provider still challenges', async () => {
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS name text NOT NULL DEFAULT '';
      ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS base_url text NOT NULL DEFAULT 'https://source-a.example';
      ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS last_health_check_at timestamptz;
      ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS blocked_reason text;
      ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS blocked_details jsonb;
      ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now()`);
    await db.query(`UPDATE importer_sources SET status='COOLDOWN',cooldown_until=now()-interval '1 hour',
      blocked_reason='TURNSTILE',blocked_details='{}'::jsonb,last_health_check_at=now()-interval '6 hours'
      WHERE id='source-a'`);
    const adapter = { id:'source-a',name:'Source A',baseUrl:'https://source-a.example' };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload:async ()=>'unused',healthCheck:async()=>true,getProviderKey:()=>'telegram' },
      { IMPORTER_USER_ID:'00000000-0000-0000-0000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY:1,SOURCE_OPERATION_TIMEOUT_MS:2_000 } as any,
    );
    const admission = { executeProdProbe:vi.fn(async () => ({ overallStatus:'FAIL',classification:'TURNSTILE',cfRay:'ray-test',
      stages:[{ stage:'BASE_URL',status:'FAIL',httpStatus:403,durationMs:20,classification:'TURNSTILE' }] })) };
    (runtime as any).sourceAdmissionGate = admission;
    (runtime as any).isRuntimeLeader = true;

    await (runtime as any).probeDueSourceRecovery();

    const source = (await db.query(`SELECT status,blocked_reason,cooldown_until FROM importer_sources WHERE id='source-a'`)).rows[0];
    expect(admission.executeProdProbe).toHaveBeenCalledTimes(1);
    expect(source.status).toBe('COOLDOWN');
    expect(source.blocked_reason).toBe('TURNSTILE');
    expect(new Date(source.cooldown_until).getTime()).toBeGreaterThan(Date.now()+5*60*60_000);
  });

  it('releases an expired source only after every admission probe stage passes', async () => {
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS name text NOT NULL DEFAULT '';
      ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS base_url text NOT NULL DEFAULT 'https://source-a.example';
      ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS last_health_check_at timestamptz;
      ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS blocked_reason text;
      ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS blocked_details jsonb;
      ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now()`);
    await db.query(`UPDATE importer_sources SET status='COOLDOWN',cooldown_until=now()-interval '1 hour',
      blocked_reason='TEMPORARY_WAF',blocked_details='{}'::jsonb,last_health_check_at=now()-interval '6 hours'
      WHERE id='source-a'`);
    const adapter = { id:'source-a',name:'Source A',baseUrl:'https://source-a.example' };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload:async ()=>'unused',healthCheck:async()=>true,getProviderKey:()=>'telegram' },
      { IMPORTER_USER_ID:'00000000-0000-0000-0000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY:1,SOURCE_OPERATION_TIMEOUT_MS:2_000 } as any,
    );
    const admission = { executeProdProbe:vi.fn(async () => ({ overallStatus:'PASS',classification:null,cfRay:null,
      stages:[{ stage:'BASE_URL',status:'PASS',httpStatus:200,durationMs:20,classification:null }] })) };
    (runtime as any).sourceAdmissionGate = admission;
    (runtime as any).isRuntimeLeader = true;

    await (runtime as any).probeDueSourceRecovery();

    const source = (await db.query(`SELECT status,blocked_reason,blocked_details FROM importer_sources WHERE id='source-a'`)).rows[0];
    expect(admission.executeProdProbe).toHaveBeenCalledTimes(1);
    expect(source.status).toBe('ACTIVE');
    expect(source.blocked_reason).toBeNull();
    expect(source.blocked_details).toMatchObject({ probe_success:true });
  });

  it('drains a legacy stored-metadata replay while P1 remains claimable', async () => {
    await db.query(`INSERT INTO importer_work_mappings
      (id,source,source_work_id,work_id,source_slug,source_title,metadata,sync_status)
      VALUES ('00000000-0000-4000-8000-000000000113','source-a','replay-metadata',$1::uuid,
        'p1-a','P1 A',$2::jsonb,'SYNCED')`, [ids.p1a, JSON.stringify({
      alternativeTitles: ['Replay Alias'], tags: ['Vanilla'],
    })]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'pt-br|default|chapter|98|-', priority: 'P1', idempotencyKey: 'p1-keeps-priority' });
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P2',
      idempotencyKey: 'lite:metadata:source-a:replay-metadata',
      payload: { source: 'source-a', sourceWorkId: 'replay-metadata', metadataEnrichment: true } });
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => { throw new Error('stored replay must not call an adapter'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'EXISTING_MAPPING' }) };
    // Recovery is the periodic production path.  A continuously healthy P1
    // reserve must not leave this durable enrichment task READY forever.
    await (runtime as any).runRecoveryTurn();
    expect((await db.query(`SELECT state FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:source-a:replay-metadata'`)).rows[0].state).toBe('DONE');
    expect((await repo.claim('chapter-worker', 120))?.idempotencyKey).toBe('p1-keeps-priority');
  });

  it('does not queue overlapping metadata replay ticks while the chapter pool is busy', async () => {
    const chapterPool = poolFor(db) as Pool;
    const metadataPool = poolFor(db) as Pool;
    Object.defineProperty(chapterPool, 'waitingCount', { value: 4 });
    Object.defineProperty(metadataPool, 'waitingCount', { value: 0 });
    const runtime = new ImporterLiteRuntime(
      chapterPool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
      new HostRateLimiter(2), metadataPool,
    );
    (runtime as any).isRuntimeLeader = true;
    let claims = 0;
    const claimArgs: any[][] = [];
    (runtime as any).queue.claimStoredMetadataReplay = async (...args: any[]) => {
      claims++;
      claimArgs.push(args);
      await new Promise((resolve) => setTimeout(resolve, 30));
      return null;
    };

    await Promise.all([(runtime as any).runStoredMetadataReplay(), (runtime as any).runStoredMetadataReplay()]);
    expect(claims).toBe(1);
    expect(claimArgs[0]).toEqual([expect.stringContaining(':metadata-replay'), 120, 4]);
  });

  it('does not starve one serial v4 replay behind a queued isolated-pool operation', async () => {
    const chapterPool = poolFor(db) as Pool;
    const metadataPool = poolFor(db) as Pool;
    Object.defineProperty(chapterPool, 'waitingCount', { value: 4 });
    Object.defineProperty(metadataPool, 'waitingCount', { value: 1 });
    const runtime = new ImporterLiteRuntime(
      chapterPool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
      new HostRateLimiter(2), metadataPool,
    );
    (runtime as any).isRuntimeLeader = true;
    const claim = vi.fn().mockResolvedValue(null);
    (runtime as any).queue.claimStoredMetadataReplay = claim;
    (runtime as any).queue.reclaimExpiredStoredMetadataReplayV4 = vi.fn().mockResolvedValue(0);

    await (runtime as any).runStoredMetadataReplay();
    expect(claim).toHaveBeenCalledTimes(1);
    expect(claim).toHaveBeenCalledWith(expect.stringContaining(':metadata-replay'), 120, 4);
  });

  it('reclaims one expired v4 metadata lease on the isolated lane', async () => {
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P2',
      idempotencyKey: 'lite:metadata:v4:source-a:expired',
      payload: { source: 'source-a', sourceWorkId: 'expired', metadataEnrichment: true, metadataEnrichmentVersion: 4 } });
    await db.query(`UPDATE importer_lite_tasks SET state='RUNNING',attempts=3,
      lease_owner='dead-worker',lease_until=now()-interval '1 minute'
      WHERE idempotency_key='lite:metadata:v4:source-a:expired'`);

    expect(await repo.reclaimExpiredStoredMetadataReplayV4()).toBe(1);
    expect((await db.query(`SELECT state,reason,lease_owner FROM importer_lite_tasks
      WHERE idempotency_key='lite:metadata:v4:source-a:expired'`)).rows[0]).toMatchObject({
      state: 'READY', reason: 'LEASE_EXPIRED', lease_owner: null,
    });
  });

  it('reclaims a due V4 retry so a transient metadata-pool contention cannot stall the lane', async () => {
    await repo.enqueue({ taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P2',
      idempotencyKey: 'lite:metadata:v4:source-a:due-retry',
      payload: { source: 'source-a', sourceWorkId: 'due-retry', metadataEnrichment: true, metadataEnrichmentVersion: 4 } });
    await db.query(`UPDATE importer_lite_tasks SET state='RETRY_WAIT',reason='METADATA_ENRICHMENT_DB_CONTENTION',
      next_run_at=now()-interval '1 second',attempts=0
      WHERE idempotency_key='lite:metadata:v4:source-a:due-retry'`);

    const claimed = await repo.claimStoredMetadataReplay('v4-retry-worker', 120, 4);
    expect(claimed).toMatchObject({
      idempotencyKey: 'lite:metadata:v4:source-a:due-retry', state: 'RUNNING', attempts: 1,
    });
  });

  it('refills stored metadata less often than the replay worker polls', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    const refill = vi.fn(async () => 0);
    const refillV4 = vi.fn(async () => 0);
    (runtime as any).queue.refillStoredMetadataEnrichment = refill;
    (runtime as any).queue.refillStoredMetadataEnrichmentV4 = refillV4;
    (runtime as any).queue.isStoredMetadataReplayV4Enabled = vi.fn().mockResolvedValue(false);
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-10-14T00:00:00Z'));
    try {
      await (runtime as any).refillStoredMetadataEnrichment();
      await vi.advanceTimersByTimeAsync(14_000);
      await (runtime as any).refillStoredMetadataEnrichment();
      expect(refill).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(1_000);
      await (runtime as any).refillStoredMetadataEnrichment();
      expect(refill).toHaveBeenCalledTimes(2);
      expect(refillV4).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps the bounded v4 producer eligible behind isolated-pool P2 work', async () => {
    const chapterPool = poolFor(db) as Pool;
    const metadataPool = poolFor(db) as Pool;
    Object.defineProperty(chapterPool, 'waitingCount', { value: 4 });
    Object.defineProperty(metadataPool, 'waitingCount', { value: 1 });
    const runtime = new ImporterLiteRuntime(
      chapterPool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
      new HostRateLimiter(2), metadataPool,
    );
    (runtime as any).isRuntimeLeader = true;
    const legacy = vi.fn().mockResolvedValue(0);
    const v4 = vi.fn().mockResolvedValue(0);
    (runtime as any).queue.refillStoredMetadataEnrichment = legacy;
    (runtime as any).queue.refillStoredMetadataEnrichmentV4 = v4;
    (runtime as any).queue.isStoredMetadataReplayV4Enabled = vi.fn().mockResolvedValue(true);

    await (runtime as any).refillStoredMetadataEnrichment();
    expect(legacy).not.toHaveBeenCalled();
    expect(v4).toHaveBeenCalledTimes(1);
    expect(v4).toHaveBeenCalledWith(1, 20_000);
  });

  it('uses the five-second V4 fence only while the chapter pool is idle', async () => {
    const chapterPool = poolFor(db) as Pool;
    Object.defineProperty(chapterPool, 'waitingCount', { value: 0 });
    const runtime = new ImporterLiteRuntime(
      chapterPool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).queue.refillStoredMetadataEnrichment = vi.fn().mockResolvedValue(0);
    const v4 = vi.fn().mockResolvedValue(0);
    (runtime as any).queue.refillStoredMetadataEnrichmentV4 = v4;
    (runtime as any).queue.isStoredMetadataReplayV4Enabled = vi.fn().mockResolvedValue(false);

    await (runtime as any).refillStoredMetadataEnrichment();

    expect(v4).toHaveBeenCalledWith(1, 5_000);
  });

  it('uses the persisted exclusive fence while chapter admission is parked', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).metadataReplayV4Exclusive = { enabled: true, intervalMs: 2_000 };
    (runtime as any).queue.refillStoredMetadataEnrichment = vi.fn().mockResolvedValue(0);
    const v4 = vi.fn().mockResolvedValue(0);
    (runtime as any).queue.refillStoredMetadataEnrichmentV4 = v4;

    await (runtime as any).refillStoredMetadataEnrichment();

    expect((runtime as any).shouldPauseChapterAdmission()).toBe(true);
    expect(v4).toHaveBeenCalledWith(4, 2_000, 12);
  });

  it('schedules the next exclusive V4 turn at its durable admission interval', () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).metadataReplayV4Exclusive = { enabled: true, intervalMs: 1_000 };
    const schedule = vi.spyOn(globalThis, 'setTimeout');

    (runtime as any).scheduleMetadataTurn();

    expect(schedule).toHaveBeenLastCalledWith(expect.any(Function), 1_000);
    runtime.stop();
    schedule.mockRestore();
  });

  it('rearms the exclusive V4 tick before a slow turn completes', () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    let releaseTurn!: () => void;
    (runtime as any).metadataReplayV4Exclusive = { enabled: true, intervalMs: 1_000 };
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).runMetadataTurn = vi.fn(() => new Promise<void>((resolve) => { releaseTurn = resolve; }));
    const schedule = vi.spyOn(globalThis, 'setTimeout');

    (runtime as any).scheduleMetadataTurn();
    const firstTimer = (runtime as any).metadataTurnTimer;
    const callback = schedule.mock.calls[0][0] as () => void;
    clearTimeout(firstTimer);
    callback();

    expect((runtime as any).runMetadataTurn).toHaveBeenCalledTimes(1);
    expect(schedule).toHaveBeenCalledTimes(2);
    releaseTurn();
    (runtime as any).isRuntimeLeader = false;
    runtime.stop();
    schedule.mockRestore();
  });

  it('keeps overlapping metadata ticks from queuing isolated-pool control work', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => null } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    let releaseRefresh!: () => void;
    const refreshV4 = vi.fn(() => new Promise<void>((resolve) => { releaseRefresh = resolve; }));
    (runtime as any).refreshMetadataReplayV4ExclusiveMode = refreshV4;
    (runtime as any).refreshMetadataTurboMode = vi.fn().mockResolvedValue(undefined);
    (runtime as any).refillSourceMetadataEnrichment = vi.fn().mockResolvedValue(undefined);
    (runtime as any).refillCrossSourceMetadataDiscovery = vi.fn().mockResolvedValue(0);
    (runtime as any).runCrossSourceMetadataDiscovery = vi.fn().mockResolvedValue(false);
    (runtime as any).runAliasOnlyEnrichmentTurn = vi.fn().mockResolvedValue(0);
    (runtime as any).runCoverVisionTurn = vi.fn().mockResolvedValue(0);

    const first = (runtime as any).runMetadataTurn();
    await Promise.resolve();
    await (runtime as any).runMetadataTurn();
    expect(refreshV4).toHaveBeenCalledTimes(1);

    releaseRefresh();
    await first;
    expect((runtime as any).metadataTurnGate.active).toBe(0);
  });

  it('claims ready exclusive V4 work before a non-blocking producer refill', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    const order: string[] = [];
    let releaseProducer!: () => void;
    (runtime as any).metadataReplayV4Exclusive = { enabled: true, intervalMs: 1_000 };
    (runtime as any).refreshMetadataReplayV4ExclusiveMode = vi.fn().mockResolvedValue(undefined);
    (runtime as any).runStoredMetadataReplay = vi.fn(async () => { order.push('claim'); });
    (runtime as any).refillStoredMetadataEnrichment = vi.fn(() => {
      order.push('refill');
      return new Promise<void>((resolve) => { releaseProducer = resolve; });
    });

    await (runtime as any).runMetadataTurn();

    expect(order).toEqual(['claim', 'claim', 'claim', 'claim', 'refill']);
    releaseProducer();
  });

  it('fills four exclusive V4 seats through one serialized producer', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).metadataReplayV4Exclusive = { enabled: true, intervalMs: 1_000 };
    (runtime as any).queue.refillStoredMetadataEnrichment = vi.fn().mockResolvedValue(0);
    const v4 = vi.fn().mockResolvedValueOnce(1).mockResolvedValueOnce(1).mockResolvedValueOnce(1).mockResolvedValueOnce(1);
    (runtime as any).queue.refillStoredMetadataEnrichmentV4 = v4;

    await (runtime as any).refillStoredMetadataEnrichment();

    expect(v4).toHaveBeenCalledTimes(4);
    expect(v4).toHaveBeenNthCalledWith(1, 4, 1_000, 12);
    expect(v4).toHaveBeenNthCalledWith(2, 4, 1_000, 12);
    expect(v4).toHaveBeenNthCalledWith(3, 4, 1_000, 12);
    expect(v4).toHaveBeenNthCalledWith(4, 4, 1_000, 12);
  });

  it('continues a bounded exclusive V4 cursor probe without the normal producer throttle', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).metadataReplayV4Exclusive = { enabled: true, intervalMs: 1_000 };
    (runtime as any).queue.refillStoredMetadataEnrichment = vi.fn().mockResolvedValue(0);
    const v4 = vi.fn().mockResolvedValue(0);
    (runtime as any).queue.refillStoredMetadataEnrichmentV4 = v4;

    await (runtime as any).refillStoredMetadataEnrichment();
    await (runtime as any).refillStoredMetadataEnrichment();

    expect(v4).toHaveBeenCalledTimes(2);
    expect(v4).toHaveBeenCalledWith(4, 1_000, 12);
  });

  it('retains V4 ownership after boot when a due retry already occupies its durable seat', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).queue.refillStoredMetadataEnrichment = vi.fn().mockResolvedValue(0);
    (runtime as any).queue.refillStoredMetadataEnrichmentV4 = vi.fn().mockResolvedValue(0);
    (runtime as any).queue.isStoredMetadataReplayV4Enabled = vi.fn().mockResolvedValue(true);

    await (runtime as any).refillStoredMetadataEnrichment();

    expect((runtime as any).sourceMetadataRefreshDeferredForV4).toBe(true);
  });

  it('defers optional source refetches while the stored V4 campaign owns the isolated pool', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => { throw new Error('no source access expected'); } } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).sourceMetadataRefreshDeferredForV4 = true;
    const refill = vi.fn().mockResolvedValue(1);
    const claim = vi.fn().mockResolvedValue(null);
    (runtime as any).queue.refillSourceMetadataEnrichment = refill;
    (runtime as any).queue.claimSourceMetadataRefresh = claim;

    await (runtime as any).refillSourceMetadataEnrichment();
    await (runtime as any).runSourceMetadataRefresh();

    expect(refill).not.toHaveBeenCalled();
    expect(claim).not.toHaveBeenCalled();
    expect((runtime as any).sourceMetadataProducer.outcome).toBe('DEFERRED_V4_BACKLOG');
  });

  it('classifies normal catalog metadata immediately while honoring chapter ingestion disablement', async () => {
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN IF NOT EXISTS chapter_ingestion_enabled boolean NOT NULL DEFAULT true`);
    await db.query(`UPDATE importer_sources SET chapter_ingestion_enabled=false WHERE id='source-a'`);
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:source-a:metadata-only', priority: 'P2',
      idempotencyKey: 'metadata-only-catalog', payload: { source: 'source-a', sourceWorkId: 'metadata-only' } });
    const task = await repo.claim('catalog-worker', 120);
    const adapter = {
      id: 'source-a',
      fetchWorkSnapshot: vi.fn(async () => ({
        details: { sourceWorkId: 'metadata-only', title: 'P1 A', slug: 'p1-a', status: 'ONGOING' as const, language: 'pt-BR',
          genres: ['Hentai'], alternativeTitles: ['Metadata only'] },
        chapters: [{ sourceChapterId: 'not-admitted', number: 1, displayNumber: '1', title: 'One' }],
      })),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'CONFIRMED_EXISTING' }) };
    await (runtime as any).catalog(task, 'catalog-worker');
    expect(adapter.fetchWorkSnapshot).toHaveBeenCalledOnce();
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_chapter_sources
      WHERE source='source-a' AND source_chapter_id='not-admitted'`)).rows[0].count).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks WHERE task_type='IMPORT_CHAPTER'`)).rows[0].count).toBe(0);
    expect((await db.query(`SELECT aliases_state,tags_state,version FROM importer_lite_metadata_enrichment
      WHERE source='source-a' AND source_work_id='metadata-only'`)).rows[0])
      .toMatchObject({ aliases_state:'ENRICHED',tags_state:'ENRICHED',version:3 });
  });

  it('maps an exactly matched published legacy chapter during catalog without admitting a no-op P1 import', async () => {
    await db.query(`UPDATE chapters SET published_at=now() WHERE id=$1`, [ids.chapter]);
    await db.query(`INSERT INTO importer_chapter_mappings(source,source_chapter_id,chapter_id)
      VALUES('source-a','legacy-catalog-1',$1)`, [ids.chapter]);
    await repo.enqueue({ taskType: 'CATALOG', workKey: 'source:source-a:legacy-published', priority: 'P2',
      idempotencyKey: 'legacy-published-catalog', payload: { source: 'source-a', sourceWorkId: 'legacy-published' } });
    const task = await repo.claim('catalog-worker', 120);
    const adapter = {
      id: 'source-a',
      fetchWorkSnapshot: async () => ({
        details: { sourceWorkId: 'legacy-published', title: 'Publish', slug: 'publish', status: 'COMPLETED' as const, language: 'pt-BR' },
        chapters: [{ sourceChapterId: 'legacy-catalog-1', number: 1, displayNumber: '1', title: 'Chapter 1' }],
      }),
    };
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.publish, status: 'CONFIRMED_EXISTING' }) };
    await (runtime as any).catalog(task, 'catalog-worker');
    expect((await db.query(`SELECT chapter_id FROM importer_lite_chapters
      WHERE work_id=$1 AND canonical_chapter_key='pt-br|default|chapter|1|-'`, [ids.publish])).rows[0].chapter_id).toBe(ids.chapter);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE task_type='IMPORT_CHAPTER' AND work_id=$1`, [ids.publish])).rows[0].count).toBe(0);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_chapter_sources
      WHERE source='source-a' AND source_chapter_id='legacy-catalog-1'`, [])).rows[0].count).toBe(1);
  });

  it('runs an isolated durable path from discovery to a Reader-visible published chapter', async () => {
    const adapter = {
      id: 'source-a',
      fetchUpdatedWorks: async () => ({ works: [{ sourceWorkId: 'work-publish', title: 'Publish', slug: 'publish' }], nextCursor: null }),
      fetchWorkSnapshot: async () => ({
        details: { sourceWorkId: 'work-publish', title: 'Publish', slug: 'publish', status: 'COMPLETED' as const, language: 'pt-BR' },
        chapters: [{ sourceChapterId: 'chapter-publish', number: 1, displayNumber: '1', title: 'Chapter One' }],
      }),
      fetchChapterPages: async () => ['https://source-a.test/reader-page.png'],
    };
    const storage: StorageProvider = {
      upload: vi.fn(async () => 'reader-file'), healthCheck: async () => true, getProviderKey: () => 'telegram',
      getLastBotReference: () => 'bot-a', getLastShardId: () => 'shard-a', getLastChannelId: () => 'channel-a',
      getLastMessageId: () => 12345,
    };
    const rateLimiter = new HostRateLimiter(100);
    const acquire = vi.spyOn(rateLimiter, 'acquire');
    const success = vi.spyOn(rateLimiter, 'recordSuccess');
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry, storage,
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 2,
        BUFFERED_PAGE_CONCURRENCY: 2, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any, rateLimiter,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.publish, status: 'CONFIRMED_EXISTING' }) };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    try {
      await repo.enqueue({ taskType: 'DISCOVER', workKey: 'source:source-a', priority: 'P2', idempotencyKey: 'e2e-discover', payload: { source: 'source-a' } });
      const discovery = await repo.claim('worker-discover', 120);
      await (runtime as any).handle(discovery, 'worker-discover');
      const catalog = await repo.claim('worker-catalog', 120);
      expect(catalog?.taskType).toBe('CATALOG');
      await (runtime as any).handle(catalog, 'worker-catalog');
      const chapter = await repo.claim('worker-import', 120);
      expect(chapter?.taskType).toBe('IMPORT_CHAPTER');
      await (runtime as any).handle(chapter, 'worker-import');
      const publish = await repo.claim('worker-publish', 120);
      expect(publish?.taskType).toBe('PUBLISH');
      await (runtime as any).handle(publish, 'worker-publish');
    } finally { vi.stubGlobal('fetch', originalFetch); }
    const visible = await db.query(`SELECT c.published_at,p.position,p.media_id
      FROM chapters c JOIN pages p ON p.chapter_id=c.id WHERE c.work_id=$1 ORDER BY p.position`, [ids.publish]);
    expect(visible.rows).toHaveLength(1);
    expect(visible.rows[0].published_at).toBeTruthy();
    expect(visible.rows[0].position).toBe(1);
    expect(storage.upload).toHaveBeenCalledOnce();
    expect(acquire).toHaveBeenCalledWith('source-a.test', expect.anything());
    expect(success).toHaveBeenCalledWith('source-a.test');
    expect((await db.query(`SELECT provider_key,bot_reference,storage_shard_id,channel_id,telegram_message_id
      FROM importer_lite_uploads ORDER BY created_at LIMIT 1`)).rows[0])
      .toEqual({ provider_key: 'reader-file', bot_reference: 'bot-a', storage_shard_id: 'shard-a', channel_id: 'channel-a', telegram_message_id: 12345 });
    // Diversity accounting is emitted only by the atomic public publication
    // transition, never by discovery, storage or task completion.
    expect((await db.query(`SELECT source,work_id,priority FROM importer_publication_events`)).rows)
      .toEqual([{ source: 'source-a', work_id: ids.publish, priority: 2 }]);
  });

  it('shares idle claim backoff across workers instead of generating an empty-claim storm', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    const claim = vi.spyOn((runtime as any).queue, 'claim');
    await Promise.all(Array.from({ length: 12 }, (_, index) => (runtime as any).claimNext(`idle-${index}`)));
    expect(claim).toHaveBeenCalledTimes(1);
  });

  it('does not spend chapter admission permits while workers wait for claim serialization', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', LITE_CHAPTER_WORKERS: 3,
        MAX_CONCURRENT_CHAPTERS: 3, TESTED_CONCURRENCY_CEILING: 3, DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    let releaseClaim!: () => void;
    let enteredClaim!: () => void;
    const entered = new Promise<void>((resolve) => { enteredClaim = resolve; });
    const claim = vi.fn(async () => {
      enteredClaim();
      await new Promise<void>((resolve) => { releaseClaim = resolve; });
      return null;
    });
    (runtime as any).queue.claim = claim;

    const first = (runtime as any).worker('serialized-claim-a', 1);
    await entered;
    const second = (runtime as any).worker('serialized-claim-b', 2);
    await new Promise((resolve) => setTimeout(resolve, 40));

    // The second worker is waiting before admission, not occupying a chapter
    // slot behind the one durable claim query.
    expect((runtime as any).chapterAdmissionGate.active).toBe(1);
    expect(claim).toHaveBeenCalledTimes(1);

    (runtime as any).stopped = true;
    releaseClaim();
    await Promise.all([first, second]);
    expect((runtime as any).chapterAdmissionGate.active).toBe(0);
  });

  it('keeps a full chapter cap out of the serialized claim lane', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', LITE_CHAPTER_WORKERS: 2,
        MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1, DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    const admission = (runtime as any).chapterAdmissionGate;
    expect(admission.tryAcquire()).toBe(true);
    const serializedClaim = vi.spyOn((runtime as any).claimGate, 'tryAcquire');

    const worker = (runtime as any).worker('full-cap-worker', 1);
    try {
      await new Promise((resolve) => setTimeout(resolve, 125));
      expect(serializedClaim).not.toHaveBeenCalled();
    } finally {
      (runtime as any).stopped = true;
      admission.release();
      await worker;
    }
  });

  it('does not freeze normal chapter admission for a transient local pool waiter', () => {
    const pool = poolFor(db) as Pool & { waitingCount?: number };
    const runtime = new ImporterLiteRuntime(
      pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    pool.waitingCount = 1;
    expect((runtime as any).shouldPauseChapterAdmission()).toBe(false);
    (runtime as any).sitePausedUntil = Date.now() + 1_000;
    expect((runtime as any).shouldPauseChapterAdmission()).toBe(true);
  });

  it('honors the durable manual protective stop for chapter and P2 claims, then resumes after it is cleared', async () => {
    const pool = poolFor(db) as Pool;
    const runtime = new ImporterLiteRuntime(
      pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', NOX_MANGA_URL: 'https://reader.test',
        LITE_CHAPTER_WORKERS: 1, MAX_CONCURRENT_CHAPTERS: 1, TESTED_CONCURRENCY_CEILING: 1,
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    await db.query(`INSERT INTO settings(key,value) VALUES('importer_protective_stop',$1)`, [JSON.stringify({
      active: true, reason: 'site incident', triggered_at: new Date().toISOString(),
    })]);
    const chapterClaim = vi.fn();
    const catalogClaim = vi.fn();
    (runtime as any).queue.claim = chapterClaim;
    (runtime as any).queue.claimNewWorkCatalog = catalogClaim;
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 200 })));
    try {
      await (runtime as any).probeSharedResources();
      expect((runtime as any).shouldPauseChapterAdmission()).toBe(true);
      await expect((runtime as any).claimNext('manual-stop-chapter')).resolves.toBeNull();
      await (runtime as any).runNewWorkCatalog();
      expect(chapterClaim).not.toHaveBeenCalled();
      expect(catalogClaim).not.toHaveBeenCalled();

      await db.query(`UPDATE settings SET value=$1 WHERE key='importer_protective_stop'`, [JSON.stringify({ active: false })]);
      await (runtime as any).probeSharedResources();
      expect((runtime as any).shouldPauseChapterAdmission()).toBe(false);
    } finally {
      vi.stubGlobal('fetch', originalFetch);
    }
  });

  it('applies a durable chapter admission cap and drains existing holders before lowering it', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009',
        LITE_CHAPTER_WORKERS: 8, MAX_CONCURRENT_CHAPTERS: 8, TESTED_CONCURRENCY_CEILING: 8,
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    await db.query(`INSERT INTO settings(key,value) VALUES('importer_chapter_admission_cap','4')`);
    await (runtime as any).refreshManualProtectiveStop();
    const gate = (runtime as any).chapterAdmissionGate;
    const executor = (runtime as any).executor as LiteExecutor;
    executor.setFairSourceDemand(['mangaflix', 'source-b']);
    expect(gate.capacity).toBe(4);
    expect((executor as any).sourceSlot('mangaflix').capacity).toBe(2);
    expect(Array.from({ length: 4 }, () => gate.tryAcquire())).toEqual([true, true, true, true]);
    expect(gate.tryAcquire()).toBe(false);

    await db.query(`UPDATE settings SET value='2' WHERE key='importer_chapter_admission_cap'`);
    await (runtime as any).refreshManualProtectiveStop();
    expect(gate.capacity).toBe(2);
    expect((executor as any).sourceSlot('mangaflix').capacity).toBe(1);
    expect(gate.tryAcquire()).toBe(false);
    gate.release(); gate.release(); gate.release();
    expect(gate.tryAcquire()).toBe(true);
    gate.release(); gate.release();
  });

  it('widens caps five through seven only to the measured page-count fence while reserving terminal-body headroom', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009',
        LITE_CHAPTER_WORKERS: 8, MAX_CONCURRENT_CHAPTERS: 8, TESTED_CONCURRENCY_CEILING: 8,
        DOWNLOAD_INFLIGHT_CONCURRENCY: 24, BUFFERED_PAGE_CONCURRENCY: 48, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 8,
        QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    await db.query(`INSERT INTO settings(key,value) VALUES('importer_chapter_admission_cap','3')`);
    await (runtime as any).refreshManualProtectiveStop();

    expect((runtime as any).chapterAdmissionGate.capacity).toBe(3);
    expect((runtime as any).downloads.capacity).toBe(24);
    expect((runtime as any).bufferedPages.capacity).toBe(20);
    expect((runtime as any).bufferedPageBytes.capacityBytes).toBe(40 * 1024 * 1024);
    expect((runtime as any).memoryPressureGuard.getNormalMediaProfile()).toEqual({
      pageBufferCapacity: 20, pageByteCapacity: 40 * 1024 * 1024,
    });

    await db.query(`UPDATE settings SET value='5' WHERE key='importer_chapter_admission_cap'`);
    await (runtime as any).refreshManualProtectiveStop();
    expect((runtime as any).chapterAdmissionGate.capacity).toBe(5);
    expect((runtime as any).bufferedPages.capacity).toBe(32);
    expect((runtime as any).bufferedPageBytes.capacityBytes).toBe(56 * 1024 * 1024);
    expect((runtime as any).memoryPressureGuard.getNormalMediaProfile()).toEqual({
      pageBufferCapacity: 32, pageByteCapacity: 56 * 1024 * 1024,
    });

    await db.query(`UPDATE settings SET value='6' WHERE key='importer_chapter_admission_cap'`);
    await (runtime as any).refreshManualProtectiveStop();
    expect((runtime as any).chapterAdmissionGate.capacity).toBe(6);
    // Additional chapter seats may overlap prepare/publish work while the
    // byte envelope remains fixed at the measured cap-five value.
    expect((runtime as any).downloads.capacity).toBe(24);
    expect((runtime as any).bufferedPages.capacity).toBe(32);
    expect((runtime as any).bufferedPageBytes.capacityBytes).toBe(56 * 1024 * 1024);
    expect((runtime as any).memoryPressureGuard.getNormalMediaProfile()).toEqual({
      pageBufferCapacity: 32, pageByteCapacity: 56 * 1024 * 1024,
    });

    await db.query(`UPDATE settings SET value='7' WHERE key='importer_chapter_admission_cap'`);
    await (runtime as any).refreshManualProtectiveStop();
    expect((runtime as any).chapterAdmissionGate.capacity).toBe(7);
    expect((runtime as any).downloads.capacity).toBe(24);
    expect((runtime as any).bufferedPages.capacity).toBe(32);
    expect((runtime as any).bufferedPageBytes.capacityBytes).toBe(56 * 1024 * 1024);
    expect((runtime as any).memoryPressureGuard.getNormalMediaProfile()).toEqual({
      pageBufferCapacity: 32, pageByteCapacity: 56 * 1024 * 1024,
    });
  });

  it('confirms a failed site probe before pausing productive Lite claims', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', NOX_MANGA_URL: 'https://reader.test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn()
      .mockRejectedValueOnce(new TypeError('transient egress failure'))
      .mockResolvedValueOnce(new Response(null, { status: 200 })));
    try { await (runtime as any).probeSharedResources(); } finally { vi.stubGlobal('fetch', originalFetch); }
    expect((runtime as any).shouldPauseChapterAdmission()).toBe(false);
    expect((runtime as any).lastSiteProbe).toMatchObject({ state: 'HEALTHY' });
  });

  it('pauses claims when Reader returns a confirmed unhealthy response', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', NOX_MANGA_URL: 'https://reader.test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 503 })));
    try { await (runtime as any).probeSharedResources(); } finally { vi.stubGlobal('fetch', originalFetch); }
    expect((runtime as any).shouldPauseChapterAdmission()).toBe(true);
    expect((runtime as any).lastSiteProbe).toMatchObject({ state: 'PAUSED' });
    for (const timer of (runtime as any).timers) clearTimeout(timer);
  });

  it('does not freeze chapter admission when only importer egress is unavailable', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', NOX_MANGA_URL: 'https://reader.test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new TypeError('egress unavailable')));
    try { await (runtime as any).probeSharedResources(); } finally { vi.stubGlobal('fetch', originalFetch); }
    expect((runtime as any).shouldPauseChapterAdmission()).toBe(false);
    expect((runtime as any).lastSiteProbe).toMatchObject({ state: 'DEGRADED', latencyMs: null });
  });

  it('releases the site protection early only after a confirmed recovery probe', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', NOX_MANGA_URL: 'https://reader.test',
        DOWNLOAD_INFLIGHT_CONCURRENCY: 1, BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1,
        QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn()
      .mockResolvedValueOnce(new Response(null, { status: 503 }))
      .mockResolvedValueOnce(new Response(null, { status: 503 }))
      .mockResolvedValueOnce(new Response(null, { status: 200 })));
    try {
      await (runtime as any).probeSharedResources();
      expect((runtime as any).shouldPauseChapterAdmission()).toBe(true);
      await (runtime as any).probeSharedResources(true);
    } finally {
      vi.stubGlobal('fetch', originalFetch);
      for (const timer of (runtime as any).timers) clearTimeout(timer);
    }
    expect((runtime as any).shouldPauseChapterAdmission()).toBe(false);
    expect((runtime as any).lastSiteProbe).toMatchObject({ state: 'HEALTHY' });
  });

  it('uses the measured three-chapter Hanami fence instead of serializing a healthy source', () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    expect((runtime as any).executor.sourceSlot('hanamiheaven').capacity).toBe(3);
  });

  it('lets an eighth producer fill MangaFlix page-wave gaps without widening the global page fence', () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    expect((runtime as any).executor.sourceSlot('mangaflix').capacity).toBe(8);
  });

  it('bounds per-source page waves under the configured global fences', () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 24,
        BUFFERED_PAGE_CONCURRENCY: 48, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 16, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    expect((runtime as any).downloads.capacity).toBe(24);
    // The buffer and download semaphores remain independent global fences.
    expect((runtime as any).bufferedPages.capacity).toBe(48);
    expect((runtime as any).executor.perChapterPageConcurrency).toBe(10);
    expect((runtime as any).executor.getPageConcurrencyForSource('mangaflix')).toBe(10);
    expect((runtime as any).executor.getPageConcurrencyForSource('manhastro')).toBe(10);
    expect((runtime as any).executor.getPageConcurrencyForSource('unlisted-source')).toBe(6);
  });

  it('lets an eighth chapter worker fill page-wave gaps without widening global media limits', () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', LITE_CHAPTER_WORKERS: 8,
        MAX_CONCURRENT_CHAPTERS: 32, TESTED_CONCURRENCY_CEILING: 32,
        DOWNLOAD_INFLIGHT_CONCURRENCY: 24, BUFFERED_PAGE_CONCURRENCY: 48,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY: 16, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    expect((runtime as any).chapterWorkerCapacity).toBe(8);
    expect((runtime as any).downloads.capacity).toBe(24);
    expect((runtime as any).bufferedPages.capacity).toBe(48);
    expect((runtime as any).executor.perChapterPageConcurrency).toBe(10);
    expect((runtime as any).executor.getPageConcurrencyForSource('mangaflix')).toBe(10);
    expect((runtime as any).executor.getPageConcurrencyForSource('manhastro')).toBe(10);
  });

  it('reports an anonymous bounded source-rate summary for production diagnosis', () => {
    const limiter = new HostRateLimiter(5);
    limiter.setHostRate('one.test', 5);
    limiter.setHostRate('two.test', 8);
    const summary = limiter.getSummary();
    expect(summary).toEqual({
      activeHosts: 2, blockedHosts: 0, minRatePerSecond: 5, maxRatePerSecond: 8, averageRatePerSecond: 6.5,
    });
    expect(JSON.stringify(summary)).not.toContain('one.test');
  });

  it('shares an empty P0 monitor probe and leaves the normal pool untouched', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    const monitorClaim = vi.spyOn((runtime as any).queue, 'claimMonitorBatch');
    const normalClaim = vi.spyOn((runtime as any).queue, 'claim');
    await Promise.all(Array.from({ length: 20 }, () => (runtime as any).runMonitorTurn()));
    await (runtime as any).runMonitorTurn();
    expect(monitorClaim).toHaveBeenCalledTimes(1);
    expect(normalClaim).not.toHaveBeenCalled();
  });

  it('keeps one bounded P0 monitor claim alive while chapter work waits for YSQL', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    // Simulate the production state where P1 has checkout waiters.  The P0
    // lane remains capped by monitorGate/monitorClaimGate; it must not be
    // skipped indefinitely merely because those waiters exist.
    (runtime as any).pool.waitingCount = 3;
    const monitorClaim = vi.spyOn((runtime as any).queue, 'claimMonitorBatch').mockResolvedValue([]);

    await (runtime as any).runMonitorTurn();

    expect(monitorClaim).toHaveBeenCalledTimes(1);
  });

  it('keeps P0 observation alive while an isolated P2 metadata campaign runs', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    (runtime as any).metadataReplayV4Exclusive.enabled = true;
    const monitorClaim = vi.spyOn((runtime as any).queue, 'claimMonitorBatch').mockResolvedValue([]);

    await (runtime as any).runMonitorTurn();

    expect(monitorClaim).toHaveBeenCalledTimes(1);
  });

  it('limits one slow P0 source to two active checks while another source progresses', async () => {
    const runtime=new ImporterLiteRuntime(poolFor(db) as Pool,{},{} as SourceRegistry,
      {upload:async()=>'unused',healthCheck:async()=>true,getProviderKey:()=> 'telegram'},
      {IMPORTER_USER_ID:'00000000-0000-4000-8000-000000000009',DOWNLOAD_INFLIGHT_CONCURRENCY:1,BATCH_PAGE_DOWNLOAD_CONCURRENCY:1,QUEUE_LEASE_DURATION_SECONDS:120} as any);
    (runtime as any).isRuntimeLeader=true;
    const tasks=(source:string,count:number)=>Array.from({length:count},(_,i)=>({id:`${source}-${i}`,taskType:'CATALOG',payload:{source}}));
    const claimMonitorBatch=vi.fn().mockResolvedValueOnce(tasks('slow',4)).mockResolvedValueOnce(tasks('fast',4));
    (runtime as any).queue={claimMonitorBatch};
    let release!:()=>void;
    const blocked=new Promise<void>((resolve)=>{release=resolve;});
    const handled:string[]=[];
    (runtime as any).handle=vi.fn(async (task:any)=>{handled.push(task.payload.source);await blocked;});
    const turns=Array.from({length:4},()=> (runtime as any).runMonitorTurn());
    await vi.waitFor(()=>expect(handled).toHaveLength(4));
    expect(handled.filter(source=>source==='slow')).toHaveLength(2);
    expect(handled.filter(source=>source==='fast')).toHaveLength(2);
    expect(claimMonitorBatch).toHaveBeenCalledTimes(2);
    release(); await Promise.all(turns);
    expect((runtime as any).monitorActiveBySource.size).toBe(0);
  });

  it('keeps two independent P0 source probes in flight while serializing their queue claims', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    const claimMonitor = vi.fn()
      .mockResolvedValueOnce([{ id: 'p0-source-a', taskType: 'CATALOG' }])
      .mockResolvedValueOnce([{ id: 'p0-source-b', taskType: 'CATALOG' }]);
    (runtime as any).queue = { claimMonitorBatch: claimMonitor };
    let releaseSnapshots!: () => void;
    const snapshots = new Promise<void>((resolve) => { releaseSnapshots = resolve; });
    const handle = vi.fn(async () => snapshots);
    (runtime as any).handle = handle;

    const turns = [(runtime as any).runMonitorTurn(), (runtime as any).runMonitorTurn()];
    await vi.waitFor(() => expect(handle).toHaveBeenCalledTimes(2));
    expect(claimMonitor).toHaveBeenCalledTimes(2);
    releaseSnapshots();
    await Promise.all(turns);
  });

  it('releases the page-count permit after durable intent while bytes stay bounded during slow uploads', async () => {
    const key = 'pt-br|default|chapter|6|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','a-6','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'buffered-pages',
      payload: { displayNumber: '6', chapterTitle: 'Six', language: 'pt-BR', edition: 'default', orderKey: '6' } });
    const task = await repo.claim('worker-a', 120);
    const registry = { get: () => ({ id: 'source-a', fetchChapterPages: async () => ['https://source-a.test/one.png', 'https://source-a.test/two.png'] }) } as unknown as SourceRegistry;
    let activeUploads = 0;
    let maxActiveUploads = 0;
    let byteBudget: PageByteBudget | null = null;
    let reservedBytesAtUploadOverlap = 0;
    const storage: StorageProvider = {
      upload: async () => {
        activeUploads++;
        maxActiveUploads = Math.max(maxActiveUploads, activeUploads);
        if (activeUploads === 2) reservedBytesAtUploadOverlap = byteBudget?.snapshot().reservedBytes || 0;
        // The durable pre-upload batch may join for up to 300 ms. Keep the
        // first upload live beyond that point so the test observes whether a
        // freed count permit admits the second download and upload.
        await new Promise<void>((resolve) => setTimeout(resolve, 750));
        activeUploads--;
        return `telegram-${maxActiveUploads}`;
      }, healthCheck: async () => true, getProviderKey: () => 'telegram',
    };
    const pngA = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    // Keep this media unique across the shared integration fixture so the
    // durable media-hash reuse fast path cannot skip the slow storage call.
    pngA[pngA.length - 13] ^= (Date.now() % 251) + 1;
    const pngB = Uint8Array.from(pngA);
    pngB[pngB.length - 14] ^= 1; // different valid PNG payload/hash; parser intentionally does not decode pixels
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async (url: string) => {
      const body = url.includes('one') ? pngA : pngB;
      // Exercise MangaFlix's lengthless-image path.  The first tiny body
      // must shed its 20 MiB terminal reservation before its slow upload,
      // otherwise the second page cannot enter this 22 MiB bounded wave.
      return new Response(body, { status: 200 });
    }));
    try {
      // One initial 1 MiB lengthless admission plus its 19 MiB completion
      // headroom fit in this bounded wave.  Once its exact tiny body is
      // assembled, the second page must overlap its slow upload.
      byteBudget = new PageByteBudget(22 * 1024 * 1024);
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 2,
        new AsyncSemaphore(1, 'test-buffered-pages'), new HostRateLimiter(100), 1, undefined, byteBudget);
      await executor.importChapter(task!, 'worker-a');
      expect(byteBudget.snapshot().reservedBytes).toBe(0);
    } finally { vi.stubGlobal('fetch', originalFetch); }
    // The one-slot count gate protects only download/validation. Once each
    // body is byte-reserved and its pre-upload intent is durable, the next
    // page may download while Telegram drains the first one.
    expect(maxActiveUploads).toBe(2);
    // Lengthless tiny images retain their exact bodies rather than the
    // temporary 20 MiB completion reservation while Telegram overlaps.
    expect(reservedBytesAtUploadOverlap).toBeLessThan(1 * 1024 * 1024);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads WHERE task_id=$1 AND state='PERSISTED'`, [task!.id])).rows[0].count).toBe(2);
  });

  it('starts a fresh origin timeout after a page waits for the shared download gate', async () => {
    const key = 'pt-br|default|chapter|6-admission-before-transfer|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','a-6-admission-before-transfer','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'admission-before-transfer',
      payload: { displayNumber: '6.4', chapterTitle: 'Six Point Four', language: 'pt-BR', edition: 'default', orderKey: '6.4' } });
    const task = await repo.claim('worker-a', 120);
    const registry = { get: () => ({ id: 'source-a', fetchChapterPages: async () => ['https://source-a.test/page.png'] }) } as unknown as SourceRegistry;
    const downloads = new AsyncSemaphore(1, 'admission-before-transfer-downloads');
    expect(downloads.tryAcquire()).toBe(true);
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry,
        { upload: async () => 'telegram-file', healthCheck: async () => true, getProviderKey: () => 'telegram' },
        '00000000-0000-4000-8000-000000000009', downloads, 1,
        new AsyncSemaphore(1, 'admission-before-transfer-buffers'), new HostRateLimiter(100), 1,
        undefined, undefined, 30_000, undefined, 250, 30);
      const importing = executor.importChapter(task!, 'worker-a');
      await vi.waitFor(() => expect(downloads.queued).toBe(1));
      // This deliberately exceeds the short transfer budget.  It is local
      // queue time, so the fetch itself must still get its own full budget.
      await new Promise<void>((resolve) => setTimeout(resolve, 60));
      downloads.release();
      await importing;
    } finally {
      if (downloads.active > 0) downloads.release();
      vi.stubGlobal('fetch', originalFetch);
    }
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0].state).toBe('DONE');
    expect((await db.query(`SELECT health,consecutive_failures FROM importer_lite_source_health WHERE source='source-a'`)).rows[0])
      .toMatchObject({ health: 'HEALTHY', consecutive_failures: 0 });
  });

  it('retries a saturated local admission without degrading the origin source', async () => {
    const key = 'pt-br|default|chapter|6-admission-timeout|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','a-6-admission-timeout','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'admission-timeout',
      payload: { displayNumber: '6.5', chapterTitle: 'Six Point Five', language: 'pt-BR', edition: 'default', orderKey: '6.5' } });
    const task = await repo.claim('worker-a', 120);
    const registry = { get: () => ({ id: 'source-a', fetchChapterPages: async () => ['https://source-a.test/page.png'] }) } as unknown as SourceRegistry;
    const downloads = new AsyncSemaphore(1, 'admission-timeout-downloads');
    expect(downloads.tryAcquire()).toBe(true);
    const originalFetch = globalThis.fetch;
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry,
        { upload: async () => 'telegram-file', healthCheck: async () => true, getProviderKey: () => 'telegram' },
        '00000000-0000-4000-8000-000000000009', downloads, 1,
        new AsyncSemaphore(1, 'admission-timeout-buffers'), new HostRateLimiter(100), 1,
        undefined, undefined, 30_000, undefined, 40, 30);
      await executor.importChapter(task!, 'worker-a');
    } finally {
      downloads.release();
      vi.stubGlobal('fetch', originalFetch);
    }
    expect(fetchMock).not.toHaveBeenCalled();
    expect((await db.query(`SELECT state,reason FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0])
      .toMatchObject({ state: 'RETRY_WAIT', reason: 'PAGE_ADMISSION_TIMEOUT' });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_source_health WHERE source='source-a'`)).rows[0].count).toBe(0);
  });

  it('releases the page-count permit while a durable reservation batch is contended', async () => {
    const key = 'pt-br|default|chapter|6-reservation-backpressure|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','a-6-reservation-backpressure','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'reservation-backpressure',
      payload: { displayNumber: '6.3', chapterTitle: 'Six Point Three', language: 'pt-BR', edition: 'default', orderKey: '6.3' } });
    const task = await repo.claim('worker-a', 120);
    const registry = { get: () => ({ id: 'source-a', fetchChapterPages: async () => ['https://source-a.test/one.png', 'https://source-a.test/two.png'] }) } as unknown as SourceRegistry;
    let beginReservationQuery!: () => void;
    let releaseReservationQuery!: () => void;
    const reservationQueryStarted = new Promise<void>((resolve) => { beginReservationQuery = resolve; });
    const reservationQueryGate = new Promise<void>((resolve) => { releaseReservationQuery = resolve; });
    const reserveUploads = repo.reserveUploads.bind(repo);
    let delayed = false;
    const reservationSpy = vi.spyOn(repo, 'reserveUploads').mockImplementation(async (inputs) => {
      if (!delayed) {
        delayed = true;
        beginReservationQuery();
        await reservationQueryGate;
      }
      return reserveUploads(inputs);
    });
    const pngA = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    pngA[pngA.length - 13] ^= (Date.now() % 251) + 1;
    const pngB = Uint8Array.from(pngA);
    pngB[pngB.length - 14] ^= 1;
    let fetches = 0;
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async (url: string) => {
      fetches++;
      return new Response(url.includes('one') ? pngA : pngB, { status: 200 });
    }));
    const pageBuffers = new AsyncSemaphore(1, 'reservation-backpressure-page-buffers');
    const downloads = new AsyncSemaphore(2, 'reservation-backpressure-downloads');
    const byteBudget = new PageByteBudget(22 * 1024 * 1024);
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry,
        { upload: async (_bytes, _mime, mediaId) => `telegram-${mediaId}`, healthCheck: async () => true, getProviderKey: () => 'telegram' },
        '00000000-0000-4000-8000-000000000009', downloads, 2,
        pageBuffers, new HostRateLimiter(100), 1, undefined, byteBudget);
      const importing = executor.importChapter(task!, 'worker-a');
      await reservationQueryStarted;
      // The first page is waiting for its durable intent.  The second must
      // still finish its bounded HTTP body phase; byte admission, not the
      // stale count permit, is now the sole memory guard while YSQL is slow.
      await expect(Promise.race([
        (async () => { while (fetches < 2) await new Promise<void>((resolve) => setTimeout(resolve, 2)); })(),
        new Promise<void>((_, reject) => setTimeout(() => reject(new Error('SECOND_PAGE_BLOCKED_BY_RESERVATION')), 500)),
      ])).resolves.toBeUndefined();
      expect(pageBuffers.active).toBe(0);
      expect(downloads.active).toBe(0);
      expect(byteBudget.snapshot().reservedBytes).toBeGreaterThan(0);
      releaseReservationQuery();
      await importing;
    } finally {
      reservationSpy.mockRestore();
      vi.stubGlobal('fetch', originalFetch);
    }
    expect(byteBudget.snapshot()).toMatchObject({ reservedBytes: 0, waiting: 0 });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads WHERE task_id=$1 AND state='PERSISTED'`, [task!.id])).rows[0].count).toBe(2);
  });

  it('does not occupy a page-count slot while waiting for bounded byte admission', async () => {
    const key = 'pt-br|default|chapter|6-byte-admission|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','a-6-byte-admission','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'byte-admission-order',
      payload: { displayNumber: '6.2', chapterTitle: 'Six Point Two', language: 'pt-BR', edition: 'default', orderKey: '6.2' } });
    const task = await repo.claim('worker-a', 120);
    const registry = { get: () => ({ id: 'source-a', fetchChapterPages: async () => ['https://source-a.test/one.png', 'https://source-a.test/two.png'] }) } as unknown as SourceRegistry;
    let startFirstUpload!: () => void;
    let releaseFirstUpload!: () => void;
    const firstUploadStarted = new Promise<void>((resolve) => { startFirstUpload = resolve; });
    const firstUploadGate = new Promise<void>((resolve) => { releaseFirstUpload = resolve; });
    let uploadCount = 0;
    const storage: StorageProvider = {
      upload: async () => {
        uploadCount++;
        if (uploadCount === 1) {
          startFirstUpload();
          await firstUploadGate;
        }
        return `telegram-byte-admission-${uploadCount}`;
      }, healthCheck: async () => true, getProviderKey: () => 'telegram',
    };
    const pngA = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    pngA[pngA.length - 13] ^= (Date.now() % 251) + 1;
    const pngB = Uint8Array.from(pngA);
    pngB[pngB.length - 14] ^= 1;
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async (url: string) => new Response(url.includes('one') ? pngA : pngB, {
      status: 200, headers: { 'content-length': String(pngA.byteLength) },
    })));
    const pageBuffers = new AsyncSemaphore(1, 'byte-admission-page-buffers');
    const downloads = new AsyncSemaphore(2, 'byte-admission-downloads');
    // This capacity admits one 1 MiB provisional body plus its 1 MiB
    // terminal-upgrade headroom. After its tiny declared body refines down,
    // the second producer still must wait for a full terminal headroom.
    const byteBudget = new PageByteBudget(2 * 1024 * 1024);
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', downloads, 2,
        pageBuffers, new HostRateLimiter(100), 1, undefined, byteBudget);
      const importing = executor.importChapter(task!, 'worker-a');
      await firstUploadStarted;
      await expect(Promise.race([
        (async () => {
          while (byteBudget.snapshot().waiting < 1) await new Promise<void>((resolve) => setTimeout(resolve, 2));
        })(),
        new Promise<void>((_, reject) => setTimeout(() => reject(new Error('SECOND_PAGE_DID_NOT_WAIT_FOR_BYTES')), 500)),
      ])).resolves.toBeUndefined();
      // The first page released its count permit before upload; the second is
      // blocked only on bytes and must not consume a page slot or a global
      // HTTP request slot while the first upload drains.
      expect(pageBuffers.active).toBe(0);
      expect(downloads.active).toBe(0);
      releaseFirstUpload();
      await importing;
    } finally {
      vi.stubGlobal('fetch', originalFetch);
    }
    expect(byteBudget.snapshot()).toMatchObject({ reservedBytes: 0, waiting: 0 });
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads WHERE task_id=$1 AND state='PERSISTED'`, [task!.id])).rows[0].count).toBe(2);
  });

  it('overlaps the next bounded page wave while scalar receipt persistence waits, then drains before completion', async () => {
    const key = 'pt-br|default|chapter|6-receipt-overlap|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','a-6-receipt-overlap','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'receipt-overlap',
      payload: { displayNumber: '6.1', chapterTitle: 'Six Point One', language: 'pt-BR', edition: 'default', orderKey: '6.1' } });
    const task = await repo.claim('worker-a', 120);
    const registry = { get: () => ({ id: 'source-a', fetchChapterPages: async () => [
      'https://source-a.test/one.png', 'https://source-a.test/two.png', 'https://source-a.test/three.png',
    ] }) } as unknown as SourceRegistry;
    const storage: StorageProvider = { upload: vi.fn(async () => 'telegram-file'), healthCheck: async () => true, getProviderKey: () => 'telegram' };
    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    const originalReceiptBatch = repo.recordUploadReceipts.bind(repo);
    const chapterMetric = vi.spyOn(telemetryCollector, 'recordChapterMetric');
    let releaseFirst!: () => void;
    let markFirstStarted!: () => void;
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    let receiptWrites = 0;
    const fetchMock = vi.fn(async (url: string) => {
      const body = Uint8Array.from(png);
      if (url.includes('two')) body[body.length - 13] ^= 1;
      if (url.includes('three')) body[body.length - 14] ^= 1;
      return new Response(body, { status: 200, headers: { 'content-length': String(body.byteLength) } });
    });
    vi.stubGlobal('fetch', fetchMock);
    (repo as any).recordUploadReceipts = async (inputs: any[]) => {
      receiptWrites++;
      if (receiptWrites === 1) {
        markFirstStarted();
        await firstGate;
      }
      return originalReceiptBatch(inputs);
    };
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'receipt-overlap-downloads'), 2,
        new AsyncSemaphore(2, 'receipt-overlap-buffers'));
      const importing = executor.importChapter(task!, 'worker-a');
      await firstStarted;
      await expect(Promise.race([
        (async () => { while (fetchMock.mock.calls.length < 3) await new Promise<void>((resolve) => setTimeout(resolve, 2)); })(),
        new Promise<void>((_, reject) => setTimeout(() => reject(new Error('NEXT_PAGE_WAITED_FOR_RECEIPT')), 250)),
      ])).resolves.toBeUndefined();
      releaseFirst();
      await importing;
    } finally {
      (repo as any).recordUploadReceipts = originalReceiptBatch;
      vi.stubGlobal('fetch', originalFetch);
    }
    expect(storage.upload).toHaveBeenCalledTimes(3);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads WHERE task_id=$1 AND state='PERSISTED'`, [task!.id])).rows[0].count).toBe(3);
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0].state).toBe('DONE');
    expect(chapterMetric).toHaveBeenLastCalledWith(expect.objectContaining({
      jobId: task!.id, source: 'source-a', pageCount: 3,
      totalDurationMs: expect.any(Number), media_pipeline_wall_ms: expect.any(Number),
      db_publish_ms: expect.any(Number), totalBytes: expect.any(Number),
    }));
  });

  it('flushes receipt arrivals that occur while a prior durable batch is in flight', async () => {
    let releaseFirst!: () => void;
    let markFirstStarted!: () => void;
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const calls: number[] = [];
    const batcher = new UploadReceiptBatcher({
      recordUploadReceipts: async (inputs) => {
        calls.push(inputs.length);
        if (calls.length === 1) {
          markFirstStarted();
          await firstGate;
        }
      },
    } as unknown as LiteRepository);
    const input = {
      taskId: ids.p1a, pagePosition: 1, mediaId: ids.page, sha256: 'receipt-race', bytes: 1024,
      mime: 'image/png', width: 16, height: 16, providerKey: 'file', botReference: null,
      shardId: null, channelId: null, messageId: null, createdBy: ids.staff,
    };
    const first = batcher.persist(input);
    await firstStarted;
    const second = batcher.persist({ ...input, pagePosition: 2 });
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
    releaseFirst();
    // This arrives after the first write completes but before the bounded
    // follow-up join window. It must join the queued second receipt instead
    // of becoming a third one-row durable write.
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
    const third = batcher.persist({ ...input, pagePosition: 3 });
    const result = await Promise.race([
      Promise.all([first, second, third]).then(() => 'completed'),
      new Promise<string>((resolve) => setTimeout(() => resolve('timed-out'), DURABLE_RECEIPT_BATCH_JOIN_MS + 500)),
    ]);
    expect(result).toBe('completed');
    expect(calls).toEqual([1, 2]);
    expect(batcher.snapshot()).toMatchObject({ sampledFlushes: 2, totalItems: 3, totalWrites: 2, failedFlushes: 0 });
  });

  it('uses a longer join only for deferred receipts, not pre-upload intents', () => {
    expect(DURABLE_RESERVATION_BATCH_JOIN_MS).toBe(350);
    expect(DURABLE_RECEIPT_BATCH_JOIN_MS).toBe(900);
    expect(DURABLE_RECEIPT_BATCH_JOIN_MS).toBeGreaterThan(DURABLE_RESERVATION_BATCH_JOIN_MS);
    expect(DURABLE_RECEIPT_BATCH_JOIN_MS).toBeLessThan(1_000);
  });

  it('flushes reservation arrivals that occur while a prior durable batch is in flight', async () => {
    let releaseFirst!: () => void;
    let markFirstStarted!: () => void;
    const firstStarted = new Promise<void>((resolve) => { markFirstStarted = resolve; });
    const firstGate = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const calls: number[] = [];
    const batcher = new UploadReservationBatcher({
      reserveUploads: async (inputs) => {
        calls.push(inputs.length);
        if (calls.length === 1) {
          markFirstStarted();
          await firstGate;
        }
        return inputs.map(() => ({ mediaId: ids.page, state: 'INTENT' }));
      },
    } as unknown as LiteRepository);
    const first = batcher.reserve({ taskId: ids.p1a, pagePosition: 1, sha256: 'reservation-race-one' });
    await firstStarted;
    const second = batcher.reserve({ taskId: ids.p1a, pagePosition: 2, sha256: 'reservation-race-two' });
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
    releaseFirst();
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
    const third = batcher.reserve({ taskId: ids.p1a, pagePosition: 3, sha256: 'reservation-race-three' });
    const result = await Promise.race([
      Promise.all([first, second, third]).then(() => 'completed'),
      new Promise<string>((resolve) => setTimeout(() => resolve('timed-out'), DURABLE_RESERVATION_BATCH_JOIN_MS + 500)),
    ]);
    expect(result).toBe('completed');
    expect(calls).toEqual([1, 2]);
    expect(batcher.snapshot()).toMatchObject({ sampledFlushes: 2, totalItems: 3, totalWrites: 2, failedFlushes: 0 });
  });

  it('does not skip uncovered P0 mappings when missing count exceeds limit and revives BLOCKED monitors', async () => {
    await db.query(`UPDATE importer_sources SET enabled=false WHERE id='source-b'`);
    const workIds = Array.from({ length: 5 }, (_, index) =>
      `00000000-0000-4000-8000-${String(2200 + index).padStart(12, '0')}`,
    );
    for (const [index, workId] of workIds.entries()) {
      const sourceWorkId = `p0-no-skip-${String(index + 1).padStart(2, '0')}`;
      await db.query(`INSERT INTO works(id,title,slug,cover_id,published)
        VALUES($1::uuid,$2,$3,$4::uuid,true)`, [workId, `NoSkip ${index}`, `noskip-${index}`, ids.cover]);
      await db.query(`INSERT INTO importer_work_mappings(id,source,source_work_id,work_id,sync_status)
        VALUES($1::uuid,'source-a',$2,$3::uuid,'ACTIVE')`, [
        `00000000-0000-4000-8000-${String(2300 + index).padStart(12, '0')}`, sourceWorkId, workId,
      ]);
      if (index === 0) {
        await repo.enqueue({
          taskType: 'CATALOG', workId, workKey: `work:${workId}`, priority: 'P2', monitoring: true,
          idempotencyKey: `lite:p0-monitor:source-a:${sourceWorkId}`,
          payload: { source: 'source-a', sourceWorkId, p0MonitorCoverage: true },
        });
        await db.query(`UPDATE importer_lite_tasks SET state='BLOCKED', reason='TRANSIENT_EXHAUSTED'
          WHERE idempotency_key=$1`, [`lite:p0-monitor:source-a:${sourceWorkId}`]);
      }
    }

    // With limit=2, scanLimit=16 sees all 5 uncovered/blocked mappings, processes 2,
    // and must keep cursor at p0-no-skip-02 so 03..05 are enrolled on subsequent turns.
    const first = await repo.refillP0MonitorCoverage(2);
    expect(first).toEqual({ source: 'source-a', admitted: 1, revived: 1 });
    expect((await db.query(`SELECT value FROM settings WHERE key='p0_monitor_coverage_mapping:source-a'`)).rows[0].value)
      .toBe('p0-no-skip-02');

    const second = await repo.refillP0MonitorCoverage(2);
    expect(second).toEqual({ source: 'source-a', admitted: 2, revived: 0 });
    const third = await repo.refillP0MonitorCoverage(2);
    expect(third).toEqual({ source: 'source-a', admitted: 1, revived: 0 });

    const enrolledCount = await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks
      WHERE monitoring=true AND state IN ('READY','RETRY_WAIT') AND payload->>'source'='source-a'`);
    expect(enrolledCount.rows[0].count).toBe(5);
  });

  it('enforces Nano Machine P0->P1 canonical sequence rule and transient-cooldown frontier barrier', async () => {
    await db.query(`UPDATE works SET published=true WHERE id=$1`, [ids.p1a]);
    // Chapter 1 is published; chapter 2 is pending P1 historical backfill.
    await db.query(`INSERT INTO chapters(id,work_id,number,display_number,canonical_chapter_key,published_at)
      VALUES('00000000-0000-4000-8000-000000000701',$1,1,'1','pt-br|default|chapter|1|-',now())`, [ids.p1a]);
    await db.query(`INSERT INTO importer_chapter_mappings(source,source_chapter_id,chapter_id)
      VALUES('source-a','ch-1','00000000-0000-4000-8000-000000000701')`);
    await db.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key)
      VALUES($1,'pt-br|default|chapter|1|-','00000000-0000-4000-8000-000000000701','1','n|000000000001|000000000000|||')`, [ids.p1a]);
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'pt-br|default|chapter|2|-', priority: 'P1',
      idempotencyKey: `lite:chapter:${ids.p1a}:pt-br|default|chapter|2|-`,
      payload: { displayNumber: '2', orderKey: 'n|000000000002|000000000000|||', language: 'pt-BR', edition: 'default', manifestP1Repair: true },
    });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter)
      VALUES($1::uuid,'pt-br|default|chapter|2|-','source-a','nano','ch-2','pt-BR','default','n|000000000002|000000000000|||',10,true,true)`, [ids.p1a]);

    // P0 monitor observes brand-new chapter 3 while chapter 2 is still pending in P1.
    await repo.enqueue({
      taskType: 'CATALOG', workId: ids.p1a, workKey: `work:${ids.p1a}`, priority: 'P2', monitoring: true,
      idempotencyKey: 'lite:p0-monitor:source-a:nano',
      payload: { source: 'source-a', sourceWorkId: 'nano', p0MonitorCoverage: true },
    });
    const watch = await repo.claimMonitor('nano-monitor', 120);
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, {
        get: () => ({
          id: 'source-a',
          fetchWorkSnapshot: async () => ({
            details: { sourceWorkId: 'nano', title: 'P1 A', slug: 'p1-a', status: 'ONGOING' as const, language: 'pt-BR' },
            chapters: [
              { sourceChapterId: 'ch-1', number: 1, displayNumber: '1', title: 'Ch 1' },
              { sourceChapterId: 'ch-2', number: 2, displayNumber: '2', title: 'Ch 2' },
              { sourceChapterId: 'ch-3', number: 3, displayNumber: '3', title: 'Ch 3' },
            ],
          }),
        }),
      } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (runtime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'CONFIRMED_EXISTING' }) };
    await (runtime as any).catalog(watch, 'nano-monitor');

    // Chapter 3 must be incorporated into P1 (priority=2), NOT P0 (priority=1).
    const ch3 = await db.query(`SELECT priority FROM importer_lite_tasks
      WHERE idempotency_key=$1`, [`lite:chapter:${ids.p1a}:pt-br|default|chapter|3|-`]);
    expect(ch3.rows[0].priority).toBe(2);

    // Even if source-a enters a transient 60s health cooldown, chapter 2 must NOT be skipped to claim chapter 3!
    await db.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures)
      VALUES('source-a','RATE_LIMITED',now()+interval '60 seconds',1)
      ON CONFLICT(source) DO UPDATE SET health='RATE_LIMITED', cooldown_until=excluded.cooldown_until`);
    expect(await repo.claim('cooldown-barrier-worker', 120)).toBeNull();

    // Clear cooldown and complete chapters 2 and 3; next P0 release (chapter 4) must resume P0 fast-lane (priority=1)!
    await db.query(`DELETE FROM importer_lite_source_health WHERE source='source-a'`);
    await db.query(`UPDATE importer_lite_ready_works SET next_run_at=now() WHERE work_key=$1`, [`work:${ids.p1a}`]);
    const claimedCh2 = await repo.claim('drain-worker', 120);
    expect(claimedCh2?.chapterKey).toBe('pt-br|default|chapter|2|-');
    await db.query(`UPDATE importer_lite_tasks SET state='DONE', lease_owner=NULL, lease_until=NULL
      WHERE work_id=$1 AND task_type='IMPORT_CHAPTER'`, [ids.p1a]);
    await db.query(`INSERT INTO chapters(id,work_id,number,display_number,canonical_chapter_key,published_at) VALUES
      ('00000000-0000-4000-8000-000000000702',$1,2,'2','pt-br|default|chapter|2|-',now()),
      ('00000000-0000-4000-8000-000000000703',$1,3,'3','pt-br|default|chapter|3|-',now())`, [ids.p1a]);
    await db.query(`INSERT INTO importer_chapter_mappings(source,source_chapter_id,chapter_id) VALUES
      ('source-a','ch-2','00000000-0000-4000-8000-000000000702'),
      ('source-a','ch-3','00000000-0000-4000-8000-000000000703')`);
    await db.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key) VALUES
      ($1,'pt-br|default|chapter|2|-','00000000-0000-4000-8000-000000000702','2','n|000000000002|000000000000|||'),
      ($1,'pt-br|default|chapter|3|-','00000000-0000-4000-8000-000000000703','3','n|000000000003|000000000000|||')`, [ids.p1a]);

    await db.query(`UPDATE importer_lite_tasks SET state='READY', next_run_at=now() WHERE id=$1`, [watch!.id]);
    const secondWatch = await repo.claimMonitor('nano-monitor', 120);
    const caughtUpRuntime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, {
        get: () => ({
          id: 'source-a',
          fetchWorkSnapshot: async () => ({
            details: { sourceWorkId: 'nano', title: 'P1 A', slug: 'p1-a', status: 'ONGOING' as const, language: 'pt-BR' },
            chapters: [
              { sourceChapterId: 'ch-1', number: 1, displayNumber: '1', title: 'Ch 1' },
              { sourceChapterId: 'ch-2', number: 2, displayNumber: '2', title: 'Ch 2' },
              { sourceChapterId: 'ch-3', number: 3, displayNumber: '3', title: 'Ch 3' },
              { sourceChapterId: 'ch-4', number: 4, displayNumber: '4', title: 'Ch 4' },
            ],
          }),
        }),
      } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    (caughtUpRuntime as any).dedupe = { resolveWork: async () => ({ workId: ids.p1a, status: 'CONFIRMED_EXISTING' }) };
    await (caughtUpRuntime as any).catalog(secondWatch, 'nano-monitor');

    const ch4 = await db.query(`SELECT priority FROM importer_lite_tasks
      WHERE idempotency_key=$1`, [`lite:chapter:${ids.p1a}:pt-br|default|chapter|4|-`]);
    expect(ch4.rows[0].priority).toBe(1);
  });

  it('prioritizes P1 focus group works with secondary spillover, excluded-source skipping, and completion tracking', async () => {
    const focusWork = ids.p1a;
    const secondaryWork = ids.p1b;
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId: focusWork, workKey: `work:${focusWork}`,
      chapterKey: 'focus-ch-1', priority: 'P1', idempotencyKey: 'focus-ch-1',
      payload: { orderKey: 'n|000001', language: 'pt-BR', edition: 'default' },
    });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter)
      VALUES($1::uuid,'focus-ch-1','source-a','focus','focus-1','pt-BR','default','n|000001',10,true,true)`, [focusWork]);

    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId: secondaryWork, workKey: `work:${secondaryWork}`,
      chapterKey: 'secondary-ch-1', priority: 'P1', idempotencyKey: 'secondary-ch-1',
      payload: { orderKey: 'n|000001', language: 'pt-BR', edition: 'default' },
    });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter)
      VALUES($1::uuid,'secondary-ch-1','source-b','sec','sec-1','pt-BR','default','n|000001',10,true,true)`, [secondaryWork]);

    // Make secondaryWork appear older in ready_works so default FIFO would pick secondaryWork,
    // while preferFocus=true picks focusWork first!
    await db.query(`UPDATE importer_lite_ready_works SET last_claimed_at = now() - interval '10 minutes' WHERE work_key=$1`, [`work:${secondaryWork}`]);
    await db.query(`UPDATE importer_lite_ready_works SET last_claimed_at = now() - interval '1 minute' WHERE work_key=$1`, [`work:${focusWork}`]);

    // When source-a is excluded (fair-share saturated), claim jumps directly to source-b (secondaryWork)!
    const excludedClaim = await repo.claim('fair-skip-worker', 120, {
      focusWorkKeys: [`work:${focusWork}`],
      preferFocus: true,
      excludedSources: ['source-a'],
    });
    expect(excludedClaim?.idempotencyKey).toBe('secondary-ch-1');
    expect(await repo.yieldForSourceFairness(excludedClaim!, 'fair-skip-worker', 50)).toBe(true);
    await db.query(`UPDATE importer_lite_tasks SET next_run_at=now() WHERE id=$1`, [excludedClaim!.id]);
    await db.query(`UPDATE importer_lite_ready_works SET next_run_at=now() WHERE work_key=$1`, [`work:${secondaryWork}`]);

    // With no excluded source and preferFocus=true, focusWork is claimed first;
    // while focusWork is RUNNING, the very next preferFocus=true claim automatically spills over to secondaryWork!
    const firstClaim = await repo.claim('focus-worker-1', 120, {
      focusWorkKeys: [`work:${focusWork}`],
      preferFocus: true,
    });
    expect(firstClaim?.idempotencyKey).toBe('focus-ch-1');

    const spilloverClaim = await repo.claim('focus-worker-2', 120, {
      focusWorkKeys: [`work:${focusWork}`],
      preferFocus: true,
    });
    expect(spilloverClaim?.idempotencyKey).toBe('secondary-ch-1');

    // Track focus group refresh & completion accounting once focusWork finishes its backlog.
    await db.query(`UPDATE importer_lite_tasks SET state='READY', lease_owner=NULL, lease_until=NULL WHERE id IN ($1,$2)`, [firstClaim!.id, spilloverClaim!.id]);
    await db.query(`SELECT public.importer_lite_refresh_ready_work($1), public.importer_lite_refresh_ready_work($2)`, [`work:${focusWork}`, `work:${secondaryWork}`]);
    const initialSnap = await repo.refreshP1FocusGroup({ targetSize: 15, maxPerSource: 3 });
    expect(initialSnap.focusWorkCount).toBe(2);

    await db.query(`UPDATE importer_lite_tasks SET state='DONE', lease_owner=NULL, lease_until=NULL WHERE id=$1`, [firstClaim!.id]);
    await db.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key)
      VALUES($1,'focus-ch-1',$2,'1','n|000001')`, [focusWork, ids.chapter]);
    await db.query(`SELECT public.importer_lite_refresh_ready_work($1)`, [`work:${focusWork}`]);

    const afterCompletion = await repo.refreshP1FocusGroup({ targetSize: 15, maxPerSource: 3 });
    expect(afterCompletion.focusWorkCount).toBe(1);
    expect(afterCompletion.completedWorksSinceBoot).toBe(1);
  });

  it('publishes inline atomically on the worker hot path without creating an intermediate PUBLISH task', async () => {
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId: ids.publish, workKey: `work:${ids.publish}`,
      chapterKey: 'pt-br|default|chapter|9|-', priority: 'P1', idempotencyKey: 'inline-publish-ch-9',
      payload: { displayNumber: '9', chapterTitle: 'Inline Nine', orderKey: 'n|000009', language: 'pt-BR', edition: 'default' },
    });
    await db.query(`INSERT INTO importer_lite_chapter_sources
      (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed)
      VALUES($1::uuid,'pt-br|default|chapter|9|-','source-a','pub','src-ch-9','pt-BR','default','n|000009',10,true)`, [ids.publish]);

    const png = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    try {
      const storage: StorageProvider = {
        upload: vi.fn(async () => 'telegram-inline-9'),
        healthCheck: async () => true,
        getProviderKey: () => 'telegram',
        getLastBotReference: () => 'bot-1',
        getLastShardId: () => '00000000-0000-4000-8000-000000000801',
        getLastChannelId: () => 'chan-1',
        getLastMessageId: () => 901,
      };
      const adapter = {
        id: 'source-a',
        fetchChapterPages: async () => ['https://source-a.test/inline-9.png'],
      };
      const runtime = new ImporterLiteRuntime(
        poolFor(db) as Pool, {}, { get: () => adapter } as unknown as SourceRegistry,
        storage,
        {
          IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009',
          DOWNLOAD_INFLIGHT_CONCURRENCY: 2,
          BUFFERED_PAGE_CONCURRENCY: 2,
          BATCH_PAGE_DOWNLOAD_CONCURRENCY: 2,
          SOURCE_OPERATION_TIMEOUT_MS: 5_000,
          TELEGRAM_MEDIA_CONCURRENCY: 2,
        } as any,
      );

      const claimed = await repo.claim('inline-worker', 120);
      expect(claimed?.idempotencyKey).toBe('inline-publish-ch-9');
      await (runtime as any).handle(claimed, 'inline-worker', { inlinePublish: true });

      const publishTasks = await db.query(`SELECT count(*)::int AS count FROM importer_lite_tasks WHERE task_type='PUBLISH'`);
      expect(publishTasks.rows[0].count).toBe(0);

      const chapterRow = await db.query(`SELECT published_at IS NOT NULL AS is_published FROM chapters
        WHERE work_id=$1 AND canonical_chapter_key='pt-br|default|chapter|9|-'`, [ids.publish]);
      expect(chapterRow.rows[0].is_published).toBe(true);

      const events = await db.query(`SELECT source, work_id::text AS work_id, priority FROM importer_publication_events`);
      expect(events.rows).toEqual([{ source: 'source-a', work_id: ids.publish, priority: 2 }]);
    } finally {
      vi.stubGlobal('fetch', originalFetch);
    }
  });

  it('adaptively reduces chapter admission under Reader/YSQL pressure with hysteresis and restores gradually', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      {
        IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009',
        NOX_MANGA_URL: 'https://reader.test',
        LITE_CHAPTER_WORKERS: 12,
        MAX_CONCURRENT_CHAPTERS: 12,
        TESTED_CONCURRENCY_CEILING: 12,
        DOWNLOAD_INFLIGHT_CONCURRENCY: 8,
        BUFFERED_PAGE_CONCURRENCY: 16,
        BATCH_PAGE_DOWNLOAD_CONCURRENCY: 4,
        QUEUE_LEASE_DURATION_SECONDS: 30,
      } as any,
    );
    await db.query(`INSERT INTO settings(key,value) VALUES('importer_chapter_admission_cap','12')`);
    await (runtime as any).refreshManualProtectiveStop();
    expect((runtime as any).chapterAdmissionGate.capacity).toBe(12);

    // Simulate elevated Reader DB latency (>5000ms): steps down immediately (12 -> 9).
    (runtime as any).lastSiteProbe = { state: 'HEALTHY', checkedAt: new Date().toISOString(), latencyMs: 5_200 };
    (runtime as any).updateAdaptiveAdmissionControl();
    expect((runtime as any).chapterAdmissionGate.capacity).toBe(9);

    // Healthy probe (<4000ms) steps up +3 (9 -> 12).
    (runtime as any).lastSiteProbe = { state: 'HEALTHY', checkedAt: new Date().toISOString(), latencyMs: 220 };
    (runtime as any).updateAdaptiveAdmissionControl();
    expect((runtime as any).chapterAdmissionGate.capacity).toBe(12);

    // Subsequent healthy probe restores operator ceiling (12 -> 12) and never exceeds it.
    (runtime as any).updateAdaptiveAdmissionControl();
    expect((runtime as any).chapterAdmissionGate.capacity).toBe(12);
    (runtime as any).updateAdaptiveAdmissionControl();
    expect((runtime as any).chapterAdmissionGate.capacity).toBe(12);
  });

  it('projects operation_rank=0 and claims woken legacy PUBLISH tasks (orderKey IS NULL) ahead of orderKey-bearing IMPORT_CHAPTER tasks', async () => {
    const workId = '00000000-0000-4000-8000-000000000846';
    const coverId = '00000000-0000-4000-8000-000000001846';
    const workKey = `work:${workId}`;

    await db.query(`INSERT INTO importer_sources(id, enabled, status) VALUES('nexus', true, 'ACTIVE') ON CONFLICT (id) DO NOTHING`);
    await db.query(
      `INSERT INTO media(id, storage_ready, bytes, mime, purpose, provider, provider_key)
       VALUES($1, true, 4096, 'image/webp', 'COVER', 'telegram', 'cover-846')`,
      [coverId],
    );
    await db.query(
      `INSERT INTO works(id, slug, title, cover_id, status)
       VALUES($1, 'legacy-publish-woken-work', 'Legacy Publish Woken Work', $2, 'ONGOING')`,
      [workId, coverId],
    );

    // Enqueue a pre-uploaded PUBLISH task WITHOUT payload.orderKey that starts BLOCKED on WORK_PRESENTATION_INCOMPLETE,
    // plus an orderKey-bearing READY IMPORT_CHAPTER task on the same work.
    const publishEnqueued = await repo.enqueue({
      taskType: 'PUBLISH',
      workId,
      workKey,
      chapterKey: 'c|1|pt-BR|default',
      priority: 'P1',
      idempotencyKey: `lite:publish:${workId}:c|1|pt-BR|default`,
      payload: {
        displayNumber: '1',
        chapterTitle: 'Capítulo 1',
        chapterKind: 'CHAPTER',
        language: 'pt-BR',
        edition: 'default',
      },
    });
    expect(publishEnqueued.task?.id).toBeTruthy();
    await db.query(
      `UPDATE importer_lite_tasks SET state = 'BLOCKED', reason = 'WORK_PRESENTATION_INCOMPLETE' WHERE id = $1::uuid`,
      [publishEnqueued.task!.id],
    );

    await db.query(
      `INSERT INTO importer_lite_chapter_sources(work_id, canonical_chapter_key, source, source_work_id, source_chapter_id, language, edition, order_key, source_rank, confirmed, has_chapter)
       VALUES($1::uuid, 'c|2|pt-BR|default', 'nexus', 'nexus-w-846', 'ch-2', 'pt-BR', 'default', 'n|000002|000000|', 10, true, true)`,
      [workId],
    );
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER',
      workId,
      workKey,
      chapterKey: 'c|2|pt-BR|default',
      priority: 'P1',
      idempotencyKey: `lite:chapter:${workId}:c|2|pt-BR|default`,
      payload: {
        displayNumber: '2',
        chapterTitle: 'Capítulo 2',
        chapterKind: 'CHAPTER',
        orderKey: 'n|000002|000000|',
        language: 'pt-BR',
        edition: 'default',
      },
    });

    // Wake catalog-blocked tasks on the work now that presentation is complete.
    const woken = await repo.wakeCatalogBlocked(workId, workKey);
    expect(woken).toBe(1);

    const readyRow = await db.query<{ operation_rank: number; priority: number }>(
      `SELECT operation_rank, priority FROM importer_lite_ready_works WHERE work_key = $1`,
      [workKey],
    );
    expect(readyRow.rows).toHaveLength(1);
    expect(Number(readyRow.rows[0].operation_rank)).toBe(0);

    const claimed = await repo.claim('worker-r846', 60);
    expect(claimed?.id).toBe(publishEnqueued.task!.id);
    expect(claimed?.taskType).toBe('PUBLISH');
  });
});
