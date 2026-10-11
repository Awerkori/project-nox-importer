import { PGlite } from '@electric-sql/pglite';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import type { Pool } from 'pg';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { AsyncSemaphore } from '../../core/concurrency.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { ImporterEngine } from '../../core/engine.js';
import { LiteExecutor } from '../executor.js';
import { ImporterLiteRuntime } from '../runtime.js';
import { LiteRepository } from '../repository.js';
import type { SourceRegistry } from '../../sources/registry.js';
import type { StorageProvider } from '../../storage/provider.js';

const migration = readFileSync(resolve(process.cwd(), 'migrations/20261010090000_importer_lite_v2.sql'), 'utf8');
const readerMigration = readFileSync(resolve(process.cwd(), 'migrations/20260914171000_atomic_reader_repair.sql'), 'utf8');

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
      storage_shard_id text, mime text, width integer, height integer, bytes integer,
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
      cooldown_until timestamptz NULL
    );
    CREATE TABLE importer_staff_requests (work_id uuid NOT NULL REFERENCES works(id), status text NOT NULL);
    CREATE TABLE importer_chapter_mappings (
      source text NOT NULL, source_chapter_id text NOT NULL, chapter_id uuid NULL REFERENCES chapters(id),
      PRIMARY KEY(source,source_chapter_id)
    );
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
    UPDATE works SET cover_id='${ids.cover}' WHERE id='${ids.publish}';
  `);
  if (optionalRoles) await db.exec(readerMigration);
  await db.exec(migration);
}

describe('LiteRepository against the additive Lite migration', () => {
  let db: PGlite;
  let repo: LiteRepository;

  beforeEach(async () => {
    db = new PGlite();
    await createSchema(db);
    repo = new LiteRepository(poolFor(db));
  }, 20_000);

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

  it('admits ready publication before another same-priority chapter download', async () => {
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: 'work:download', chapterKey: 'download', priority: 'P1', idempotencyKey: 'p1-download' });
    await repo.enqueue({ taskType: 'PUBLISH', workId: ids.p1b, workKey: 'work:publish', chapterKey: 'publish', priority: 'P1', idempotencyKey: 'p1-publish' });
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

  it('uses quick rechecks for releases and bounded intervals for quiet or hiatus P0 watches', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1 } as any,
    );
    const interval = (checkpoint: Record<string, unknown>, status: string, releases: number) =>
      (runtime as any).monitorIntervalMs({ checkpoint }, status, releases);
    expect(interval({}, 'ONGOING', 1)).toBe(5 * 60_000);
    expect(interval({ emptyChecks: 0 }, 'ONGOING', 0)).toBe(15 * 60_000);
    expect(interval({ emptyChecks: 5 }, 'ONGOING', 0)).toBe(6 * 60 * 60_000);
    expect(interval({}, 'HIATUS', 0)).toBe(6 * 60 * 60_000);
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

    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: 'chapter:2', priority: 'P1', idempotencyKey: 'needs-publish' });
    const imported = await repo.claim('worker-a', 30);
    await repo.finish(imported!, 'worker-a', { state: 'DONE', checkpoint: { mediaIds: [ids.page] } });
    expect((await repo.reconcile(10)).publishable).toBe(1);
    const publish = await db.query(`SELECT task_type,state FROM importer_lite_tasks WHERE idempotency_key='lite:publish:' || $1 || ':chapter:2'`, [ids.p1a]);
    expect(publish.rows).toEqual([{ task_type: 'PUBLISH', state: 'READY' }]);
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
    const executor = new LiteExecutor(poolFor(db) as Pool, repo, { get: () => adapter } as unknown as SourceRegistry, storage,
      '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(1, 'test-downloads'));
    await executor.importChapter(task!, 'worker-a');
    expect(adapter.fetchChapterPages).not.toHaveBeenCalled();
    expect(storage.upload).not.toHaveBeenCalled();
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
    const originalCheckpoint = repo.checkpoint.bind(repo);
    let checkpoints = 0;
    (repo as any).checkpoint = async (...args: any[]) => {
      checkpoints++;
      if (checkpoints === 2) {
        await db.query(`UPDATE importer_lite_tasks SET lease_until=now()-interval '1 second' WHERE id=$1`, [task!.id]);
        return false;
      }
      return originalCheckpoint(...args);
    };
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(1, 'test-downloads'), 1);
      await executor.importChapter(task!, 'worker-a');
    } finally {
      (repo as any).checkpoint = originalCheckpoint;
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
            if (text.includes("UPDATE importer_lite_tasks SET state='DONE'")) return { rows: [], rowCount: 0 } as any;
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
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN catalog_discovery_enabled boolean NOT NULL DEFAULT true`);
    await db.query(`UPDATE importer_sources SET catalog_discovery_enabled=false WHERE id='source-b'`);
    expect((await repo.sourcePolicy('source-b'))?.catalogDiscoveryEnabled).toBe(false);
    await (runtime as any).seedDiscovery();
    expect((await db.query(`SELECT idempotency_key FROM importer_lite_tasks WHERE idempotency_key LIKE 'lite:discover:%' ORDER BY idempotency_key`)).rows)
      .toEqual([]);
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

  it('skips a preferred source with chapter ingestion disabled and immediately uses its confirmed alternative', async () => {
    const key = 'pt-br|default|chapter|admin-fallback|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,source_rank,confirmed)
      VALUES ($1,$2,'source-a','disabled-a','pt-BR','default',10,true),
             ($1,$2,'source-b','enabled-b','pt-BR','default',20,true)`, [ids.p1a, key]);
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN chapter_ingestion_enabled boolean NOT NULL DEFAULT true`);
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
    const originalReceipt = repo.recordUploadReceipt.bind(repo);
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async () => new Response(png, { status: 200 })));
    (repo as any).recordUploadReceipt = async () => { throw new Error('DB_RECEIPT_WRITE_FAILED'); };
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 1);
      await executor.importChapter(task!, 'worker-a');
    } finally {
      (repo as any).recordUploadReceipt = originalReceipt;
      vi.stubGlobal('fetch', originalFetch);
    }
    expect((await db.query(`SELECT state FROM importer_lite_tasks WHERE id=$1`, [task!.id])).rows[0].state).toBe('BLOCKED');
    expect((await db.query(`SELECT state FROM importer_lite_uploads WHERE task_id=$1`, [task!.id])).rows[0].state).toBe('UNKNOWN');
    expect(storage.upload).toHaveBeenCalledOnce();
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

  it('turns only a genuinely new monitor observation into P0 and returns the watch to its bounded lane', async () => {
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

  it('keeps catalog metadata and source mappings while honoring chapter ingestion disablement', async () => {
    await db.exec(`ALTER TABLE importer_sources ADD COLUMN chapter_ingestion_enabled boolean NOT NULL DEFAULT true`);
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
    expect(acquire).toHaveBeenCalledWith('source-a.test');
    expect(success).toHaveBeenCalledWith('source-a.test');
    expect((await db.query(`SELECT provider_key,bot_reference,storage_shard_id,channel_id,telegram_message_id
      FROM importer_lite_uploads ORDER BY created_at LIMIT 1`)).rows[0])
      .toEqual({ provider_key: 'reader-file', bot_reference: 'bot-a', storage_shard_id: 'shard-a', channel_id: 'channel-a', telegram_message_id: 12345 });
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

  it('uses the measured three-chapter Hanami fence instead of serializing a healthy source', () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    expect((runtime as any).executor.sourceSlot('hanamiheaven').capacity).toBe(3);
  });

  it('shares an empty P0 monitor probe and leaves the normal pool untouched', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    (runtime as any).isRuntimeLeader = true;
    const monitorClaim = vi.spyOn((runtime as any).queue, 'claimMonitor');
    const normalClaim = vi.spyOn((runtime as any).queue, 'claim');
    await Promise.all(Array.from({ length: 20 }, () => (runtime as any).runMonitorTurn()));
    await (runtime as any).runMonitorTurn();
    expect(monitorClaim).toHaveBeenCalledTimes(1);
    expect(normalClaim).not.toHaveBeenCalled();
  });

  it('keeps retained page buffers globally bounded while uploads are slow', async () => {
    const key = 'pt-br|default|chapter|6|-';
    await db.query(`INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,confirmed)
      VALUES ($1,$2,'source-a','a-6','pt-BR','default',true)`, [ids.p1a, key]);
    await repo.enqueue({ taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`, chapterKey: key, priority: 'P1', idempotencyKey: 'buffered-pages',
      payload: { displayNumber: '6', chapterTitle: 'Six', language: 'pt-BR', edition: 'default', orderKey: '6' } });
    const task = await repo.claim('worker-a', 120);
    const registry = { get: () => ({ id: 'source-a', fetchChapterPages: async () => ['https://source-a.test/one.png', 'https://source-a.test/two.png'] }) } as unknown as SourceRegistry;
    let activeUploads = 0;
    let maxActiveUploads = 0;
    const storage: StorageProvider = {
      upload: async () => {
        activeUploads++;
        maxActiveUploads = Math.max(maxActiveUploads, activeUploads);
        await new Promise<void>((resolve) => setTimeout(resolve, 15));
        activeUploads--;
        return `telegram-${maxActiveUploads}`;
      }, healthCheck: async () => true, getProviderKey: () => 'telegram',
    };
    const pngA = Uint8Array.from(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL+XQAAAABJRU5ErkJggg==', 'base64'));
    const pngB = Uint8Array.from(pngA);
    pngB[pngB.length - 13] ^= 1; // different valid IDAT payload/hash; parser intentionally does not decode pixels
    const originalFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async (url: string) => new Response(url.includes('one') ? pngA : pngB, { status: 200 })));
    try {
      const executor = new LiteExecutor(poolFor(db) as Pool, repo, registry, storage,
        '00000000-0000-4000-8000-000000000009', new AsyncSemaphore(2, 'test-downloads'), 2,
        new AsyncSemaphore(1, 'test-buffered-pages'), new HostRateLimiter(100));
      await executor.importChapter(task!, 'worker-a');
    } finally { vi.stubGlobal('fetch', originalFetch); }
    expect(maxActiveUploads).toBe(1);
    expect((await db.query(`SELECT count(*)::int AS count FROM importer_lite_uploads WHERE task_id=$1 AND state='PERSISTED'`, [task!.id])).rows[0].count).toBe(2);
  });
});
