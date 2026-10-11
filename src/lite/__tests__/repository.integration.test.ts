import { PGlite } from '@electric-sql/pglite';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import type { Pool } from 'pg';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { AsyncSemaphore } from '../../core/concurrency.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
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

async function createSchema(db: PGlite): Promise<void> {
  await db.exec(`
    CREATE ROLE anon; CREATE ROLE authenticated; CREATE ROLE service_role;
    CREATE TABLE works (
      id uuid PRIMARY KEY, title text, slug text, cover_id uuid,
      published boolean NOT NULL DEFAULT false, latest_chapter_published_at timestamptz,
      updated_at timestamptz
    );
    CREATE TABLE chapters (
      id uuid PRIMARY KEY, work_id uuid NOT NULL REFERENCES works(id), number numeric,
      title text, origin text NOT NULL DEFAULT 'IMPORTER', published_at timestamptz,
      is_fresh_release boolean NOT NULL DEFAULT false
    );
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
    CREATE TABLE importer_sources (id text PRIMARY KEY);
    CREATE TABLE importer_staff_requests (work_id uuid NOT NULL REFERENCES works(id), status text NOT NULL);
    CREATE TABLE importer_publication_events (
      chapter_id uuid PRIMARY KEY, transition_at timestamptz NOT NULL, recorded_at timestamptz NOT NULL,
      bucket_minute timestamptz NOT NULL, is_fresh_release boolean NOT NULL
    );
    INSERT INTO importer_sources VALUES ('source-a'), ('source-b');
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
  await db.exec(readerMigration);
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

  it('falls back within the same claim when the preferred source fails before media storage', async () => {
    await db.query(`
      INSERT INTO importer_lite_chapter_sources(work_id,canonical_chapter_key,source,source_chapter_id,language,edition,source_rank,confirmed)
      VALUES ($1,'number:1|language:pt-BR|edition:default|kind:normal','source-a','a-1','pt-BR','default',10,true),
             ($1,'number:1|language:pt-BR|edition:default|kind:normal','source-b','b-1','pt-BR','default',20,true)
    `, [ids.p1a]);
    await repo.enqueue({
      taskType: 'IMPORT_CHAPTER', workId: ids.p1a, workKey: `work:${ids.p1a}`,
      chapterKey: 'number:1|language:pt-BR|edition:default|kind:normal', priority: 'P1', idempotencyKey: 'fallback',
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
  });

  it('shares idle claim backoff across workers instead of generating an empty-claim storm', async () => {
    const runtime = new ImporterLiteRuntime(
      poolFor(db) as Pool, {}, { get: () => undefined } as unknown as SourceRegistry,
      { upload: async () => 'unused', healthCheck: async () => true, getProviderKey: () => 'telegram' },
      { IMPORTER_USER_ID: '00000000-0000-4000-8000-000000000009', DOWNLOAD_INFLIGHT_CONCURRENCY: 1,
        BUFFERED_PAGE_CONCURRENCY: 1, BATCH_PAGE_DOWNLOAD_CONCURRENCY: 1, QUEUE_LEASE_DURATION_SECONDS: 30 } as any,
    );
    const claim = vi.spyOn((runtime as any).queue, 'claim');
    await Promise.all(Array.from({ length: 12 }, (_, index) => (runtime as any).claimNext(`idle-${index}`)));
    expect(claim).toHaveBeenCalledTimes(1);
  });
});
