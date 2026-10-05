import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { readFileSync } from 'node:fs';
import { PublicationBarrier } from '../src/core/publication.js';

describe('atomic canonical publication events', () => {
  const work = '00000000-0000-0000-0000-000000000001';
  const chapter = '00000000-0000-0000-0000-000000000002';
  let db: PGlite;
  let barrier: PublicationBarrier;
  beforeAll(async () => {
    db = new PGlite();
    await db.exec(`
      CREATE TABLE works (id uuid PRIMARY KEY, title text, slug text, cover_id uuid, published boolean, latest_chapter_published_at timestamptz, updated_at timestamptz);
      CREATE TABLE chapters (id uuid PRIMARY KEY, published_at timestamptz, is_fresh_release boolean);
      CREATE TABLE importer_chapter_mappings (work_id uuid, chapter_id uuid, status text, updated_at timestamptz, chapter_sort_key numeric, is_page_provider boolean);
      CREATE TABLE importer_rate_buckets (bucket_minute timestamptz PRIMARY KEY, completed_jobs int DEFAULT 0, fresh_visible int DEFAULT 0, visible_published int DEFAULT 0, updated_at timestamptz);
      INSERT INTO works (id,title,slug,published) VALUES ('${work}','Fixture','fixture',true);
      INSERT INTO chapters (id) VALUES ('${chapter}');
    `);
    await db.exec(readFileSync('migrations/20260927130000_publication_events.sql','utf8'));
    barrier = new PublicationBarrier({getPool: () => ({connect: async () => ({query: (sql:string, args?:any[]) => db.query(sql,args),release:()=>{}})})} as any);
    vi.stubGlobal('fetch', vi.fn(async () => new Response('{}')));
  });
  afterAll(async () => { vi.unstubAllGlobals(); await db.close(); });

  it('rolls back chapter, durable event and bucket together on failure, then retries exactly once', async () => {
    // Force the later works update to fail, after the event CTE ran.
    await db.exec("ALTER TABLE works ADD CONSTRAINT reject_publication CHECK (latest_chapter_published_at IS NULL)");
    await expect((barrier as any).executePublish(work,chapter,new Date().toISOString(),undefined,true)).rejects.toThrow();
    expect((await db.query('SELECT published_at FROM chapters')).rows[0]).toEqual({published_at:null});
    expect((await db.query('SELECT * FROM importer_publication_events')).rows).toHaveLength(0);
    // The durable event is part of the publication transaction. The derived
    // rate bucket is deliberately outside that transaction to avoid a hot-row
    // write bottleneck when several chapters publish in one minute.
    expect((await db.query('SELECT * FROM importer_rate_buckets')).rows).toHaveLength(0);
    await db.exec('ALTER TABLE works DROP CONSTRAINT reject_publication');
    const onPublished=vi.fn(); barrier.onPublished=onPublished;
    expect(await (barrier as any).executePublish(work,chapter,new Date().toISOString(),undefined,true)).toEqual({newlyVisible:true});
    // Reprocessing must not change fresh semantics or increment either counter.
    expect(await (barrier as any).executePublish(work,chapter,new Date().toISOString(),undefined,false)).toEqual({newlyVisible:false});
    expect(onPublished).toHaveBeenCalledTimes(1);
    expect(onPublished).toHaveBeenCalledWith(true, false);
    expect((await db.query('SELECT visible_published,fresh_visible FROM importer_rate_buckets')).rows).toHaveLength(0);
    expect((await db.query('SELECT is_fresh_release FROM chapters')).rows[0]).toEqual({is_fresh_release:true});
    const correlation=await db.query(`SELECT c.published_at=e.transition_at AS same_clock,
      e.bucket_minute=date_trunc('minute',c.published_at) AS same_bucket
      FROM chapters c JOIN importer_publication_events e ON e.chapter_id=c.id`);
    expect(correlation.rows).toEqual([{same_clock:true,same_bucket:true}]);
  });

  it('retains a per-work lock until all holders and waiters leave, then evicts it', async () => {
    let release!:()=>void;
    const first=(barrier as any).getWorkLock(work).runExclusive(()=>new Promise<void>(r=>release=r));
    await Promise.resolve();
    const second=(barrier as any).getWorkLock(work).runExclusive(async()=>expect((barrier as any).workLocks.size).toBe(1));
    expect((barrier as any).workLocks.size).toBe(1);
    release();
    await Promise.all([first,second]);
    expect((barrier as any).workLocks.size).toBe(0);
  });
});
