import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { PublicationBarrier } from '../src/core/publication.js';

describe('Publication Barrier & Canonical Ordering', () => {
  let db: PGlite;
  let supabaseMock: any;
  let barrier: PublicationBarrier;
  let testUserId = '00000000-0000-0000-0000-000000000001';
  let testWorkId: string;
  let testWorkMappingId: string;
  let testCoverMediaId: string;

  beforeAll(async () => {
    db = new PGlite({ extensions: { pg_trgm } });
    await db.exec(`
      create role anon;
      create role authenticated;
      create role service_role bypassrls;
      create schema if not exists auth;
      create schema if not exists storage;
      create table if not exists auth.users (id uuid primary key, email text, email_confirmed_at timestamptz, raw_user_meta_data jsonb);
      create or replace function auth.uid() returns uuid language sql stable as $$ select nullif(current_setting('request.jwt.claim.sub', true), '')::uuid $$;
      create table if not exists storage.buckets (id text primary key, name text, public boolean, file_size_limit bigint, allowed_mime_types text[]);
      create table if not exists storage.objects (id uuid primary key default gen_random_uuid(), bucket_id text references storage.buckets(id), name text, owner uuid, created_at timestamptz default now(), updated_at timestamptz default now(), last_accessed_at timestamptz default now(), metadata jsonb);
      grant usage on schema public, auth, storage to anon, authenticated, service_role;
    `);

    const mangaMigrationsDir = resolve('/home/awerkori/.Projects/project-nox-manga/supabase/migrations');
    const files = readdirSync(mangaMigrationsDir).filter((f) => f.endsWith('.sql')).sort();
    for (const f of files) {
      const sql = readFileSync(resolve(mangaMigrationsDir, f), 'utf8')
        .replace('create extension if not exists pgcrypto;', '');
      await db.exec(sql);
    }
    await db.exec(readFileSync(resolve('migrations/001_importer_schema.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/002_importer_sources_status.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/003_importer_sort_key_and_concurrency.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/004_importer_telemetry_and_provenance.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/005_importer_page_provider_column.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/006_importer_publication_barrier.sql'), 'utf8'));

    // Insert bot user into auth.users
    await db.query(`insert into auth.users (id, email) values ($1, 'bot@projectnox.app')`, [testUserId]);

    // Create valid cover media with storage_ready = true and valid created_by
    const mediaRes = await db.query(
      `insert into public.media (created_by, provider, provider_key, mime, width, height, bytes, sha256, storage_ready, purpose)
       values ($1, 'telegram', 'tg-cover', 'image/jpeg', 800, 1200, 50000, 'sha-cover', true, 'editorial') returning id`,
      [testUserId]
    );
    testCoverMediaId = (mediaRes.rows[0] as any).id;

    // Create a test work with valid cover_id
    const workRes = await db.query(
      `insert into public.works (title, slug, published, cover_id) values ('Test Work', 'test-work', false, $1) returning id`,
      [testCoverMediaId]
    );
    testWorkId = (workRes.rows[0] as any).id;

    const mapRes = await db.query(
      `insert into public.importer_work_mappings (source, source_work_id, work_id, source_slug, source_title)
       values ('kuro', 'src-123', $1, 'test-work', 'Test Work') returning id`,
      [testWorkId]
    );
    testWorkMappingId = (mapRes.rows[0] as any).id;

    // Build real PGlite adapter mock for Supabase
    supabaseMock = {
      from: (table: string) => {
        let selectedColumns = '*';
        let filterStatements: Array<{ sql: string; vals: any[] }> = [];
        let orderStatement = '';
        let limitCount: number | null = null;

        const builder: any = {
          select: (cols: string = '*') => {
            selectedColumns = cols;
            return builder;
          },
          eq: (col: string, val: any) => {
            filterStatements.push({ sql: `${col} = $${filterStatements.length + 1}`, vals: [val] });
            return builder;
          },
          neq: (col: string, val: any) => {
            filterStatements.push({ sql: `${col} != $${filterStatements.length + 1}`, vals: [val] });
            return builder;
          },
          not: (col: string, op: string, val: any) => {
            if (op === 'is' && val === null) {
              filterStatements.push({ sql: `${col} is not null`, vals: [] });
            }
            return builder;
          },
          order: (col: string, opts?: { ascending?: boolean }) => {
            const dir = opts?.ascending === false ? 'desc' : 'asc';
            orderStatement = `order by ${col} ${dir}`;
            return builder;
          },
          limit: (n: number) => {
            limitCount = n;
            return builder;
          },
          insert: async (rows: any | any[]) => {
            const items = Array.isArray(rows) ? rows : [rows];
            for (const item of items) {
              const keys = Object.keys(item);
              const vals = Object.values(item);
              const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
              await db.query(`insert into public.${table} (${keys.join(', ')}) values (${placeholders})`, vals);
            }
            return { data: items, error: null };
          },
          upsert: async (rows: any | any[], opts?: { onConflict?: string }) => {
            const items = Array.isArray(rows) ? rows : [rows];
            for (const item of items) {
              const keys = Object.keys(item);
              const vals = Object.values(item);
              const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
              let query = `insert into public.${table} (${keys.join(', ')}) values (${placeholders})`;
              if (opts?.onConflict) {
                const updateCols = keys.filter((k) => !opts.onConflict!.split(',').includes(k));
                const updates = updateCols.map((c) => `${c} = excluded.${c}`).join(', ');
                query += ` on conflict (${opts.onConflict}) do update set ${updates}`;
              }
              await db.query(query, vals);
            }
            return { data: items, error: null };
          },
          update: (fields: any) => ({
            eq: (filterCol: string, filterVal: any) => ({
              eq: async (secondCol: string, secondVal: any) => {
                const keys = Object.keys(fields);
                const vals = Object.values(fields);
                const setClauses = keys.map((k, i) => `${k} = $${i + 1}`).join(', ');
                vals.push(filterVal);
                vals.push(secondVal);
                await db.query(
                  `update public.${table} set ${setClauses} where ${filterCol} = $${keys.length + 1} and ${secondCol} = $${keys.length + 2}`,
                  vals
                );
                return { error: null };
              },
              then: async (resolveFn: any, rejectFn?: any) => {
                try {
                  const keys = Object.keys(fields);
                  const vals = Object.values(fields);
                  const setClauses = keys.map((k, i) => `${k} = $${i + 1}`).join(', ');
                  vals.push(filterVal);
                  await db.query(
                    `update public.${table} set ${setClauses} where ${filterCol} = $${keys.length + 1}`,
                    vals
                  );
                  resolveFn({ error: null });
                } catch (err: any) {
                  resolveFn({ error: err });
                }
              },
            }),
          }),
          then: async (resolveFn: any, rejectFn?: any) => {
            const allVals: any[] = [];
            let whereClause = '';
            if (filterStatements.length > 0) {
              const parts = filterStatements.map((f) => {
                let s = f.sql;
                for (const v of f.vals) {
                  allVals.push(v);
                  s = s.replace(/\$\d+/, `$${allVals.length}`);
                }
                return s;
              });
              whereClause = `where ${parts.join(' and ')}`;
            }
            let query = `select ${selectedColumns} from public.${table} ${whereClause} ${orderStatement}`;
            if (limitCount) query += ` limit ${limitCount}`;
            try {
              const res = await db.query(query, allVals);
              resolveFn({ data: res.rows, error: null });
            } catch (err: any) {
              resolveFn({ data: null, error: err });
            }
          },
        };

        return builder;
      },
      rpc: async (funcName: string, args: any) => {
        if (funcName === 'importer_check_publication_barrier') {
          try {
            const res = await db.query(
              `select * from public.importer_check_publication_barrier($1, $2)`,
              [args.p_work_id, args.p_target_sort_key]
            );
            return { data: res.rows, error: null };
          } catch (err: any) {
            return { data: null, error: err };
          }
        }
        return { data: null, error: null };
      },
    };

    barrier = new PublicationBarrier(supabaseMock);
  });

  afterAll(async () => {
    await db.close();
  });

  async function createValidChapter(num: number, title: string) {
    const chRes = await db.query(
      `insert into public.chapters (work_id, number, title) values ($1, $2, $3) returning id`,
      [testWorkId, num, title]
    );
    const chapterId = (chRes.rows[0] as any).id;

    // Create page media with storage_ready = true and created_by = testUserId
    const mediaRes = await db.query(
      `insert into public.media (created_by, provider, provider_key, mime, width, height, bytes, sha256, storage_ready, purpose)
       values ($1, 'telegram', $2, 'image/jpeg', 800, 1200, 40000, $3, true, 'editorial') returning id`,
      [testUserId, `tg-page-${num}`, `sha-page-${num}`]
    );
    const pageMediaId = (mediaRes.rows[0] as any).id;

    // Insert page
    await db.query(
      `insert into public.pages (chapter_id, position, media_id, width, height) values ($1, 1, $2, 800, 1200)`,
      [chapterId, pageMediaId]
    );

    return chapterId;
  }

  it('allows normal sequential publication (1 -> 2 -> 3)', async () => {
    // Stage and publish chapter 1
    const ch1Id = await createValidChapter(1, 'Cap 1');

    await barrier.stageChapter({
      workId: testWorkId,
      chapterId: ch1Id,
      chapterNumber: 1,
      sortKey: 1,
      source: 'kuro',
      sourceChapterId: 'k-1',
      workMappingId: testWorkMappingId,
      pageCount: 1,
    });

    const pub1 = await barrier.tryPublish(testWorkId, 1, ch1Id);
    expect(pub1.published).toBe(true);

    const check1 = await db.query(`select published_at from public.chapters where id = $1`, [ch1Id]);
    expect((check1.rows[0] as any).published_at).not.toBeNull();

    // Stage and publish chapter 2
    const ch2Id = await createValidChapter(2, 'Cap 2');

    await barrier.stageChapter({
      workId: testWorkId,
      chapterId: ch2Id,
      chapterNumber: 2,
      sortKey: 2,
      source: 'kuro',
      sourceChapterId: 'k-2',
      workMappingId: testWorkMappingId,
      pageCount: 1,
    });

    const pub2 = await barrier.tryPublish(testWorkId, 2, ch2Id);
    expect(pub2.published).toBe(true);

    const check2 = await db.query(`select published_at from public.chapters where id = $1`, [ch2Id]);
    expect((check2.rows[0] as any).published_at).not.toBeNull();
  });

  it('blocks out-of-order publication (Cap 4 blocked until Cap 3 published, then cascades)', async () => {
    // Insert Cap 3 as PENDING (discovered)
    const ch3Id = await createValidChapter(3, 'Cap 3');

    await db.query(
      `insert into public.importer_chapter_mappings (source, source_chapter_id, chapter_id, work_id, work_mapping_id, chapter_number, chapter_sort_key, status)
       values ('kuro', 'k-3', $1, $2, $3, 3, 3, 'PENDING')`,
      [ch3Id, testWorkId, testWorkMappingId]
    );

    // Now Cap 4 finishes before Cap 3!
    const ch4Id = await createValidChapter(4, 'Cap 4');

    await barrier.stageChapter({
      workId: testWorkId,
      chapterId: ch4Id,
      chapterNumber: 4,
      sortKey: 4,
      source: 'kuro',
      sourceChapterId: 'k-4',
      workMappingId: testWorkMappingId,
      pageCount: 1,
    });

    // Cap 4 attempts to publish: MUST BE BLOCKED!
    const pub4Attempt1 = await barrier.tryPublish(testWorkId, 4, ch4Id);
    expect(pub4Attempt1.published).toBe(false);
    expect(pub4Attempt1.reason).toBe('PRECEDING_CHAPTERS_UNPUBLISHED');

    // Verify Cap 4 is still unpublished in public.chapters
    const check4Before = await db.query(`select published_at from public.chapters where id = $1`, [ch4Id]);
    expect((check4Before.rows[0] as any).published_at).toBeNull();

    // Now Cap 3 finishes, stages, and publishes!
    await barrier.stageChapter({
      workId: testWorkId,
      chapterId: ch3Id,
      chapterNumber: 3,
      sortKey: 3,
      source: 'kuro',
      sourceChapterId: 'k-3',
      workMappingId: testWorkMappingId,
      pageCount: 1,
    });

    const pub3 = await barrier.tryPublish(testWorkId, 3, ch3Id);
    expect(pub3.published).toBe(true);

    // Assert that Cap 4 was automatically published via CASCADE!
    const check4After = await db.query(`select published_at from public.chapters where id = $1`, [ch4Id]);
    expect((check4After.rows[0] as any).published_at).not.toBeNull();

    const check3 = await db.query(`select published_at from public.chapters where id = $1`, [ch3Id]);
    const t3 = new Date((check3.rows[0] as any).published_at).getTime();
    const t4 = new Date((check4After.rows[0] as any).published_at).getTime();

    // Publication timestamps must be strictly monotonic: Cap 3 <= Cap 4
    expect(t4).toBeGreaterThanOrEqual(t3);
  });

  it('releases subsequent chapters when an earlier chapter is registered as a GAP', async () => {
    // Insert Cap 5 (which will fail definitively)
    const ch5Id = await createValidChapter(5, 'Cap 5');

    await db.query(
      `insert into public.importer_chapter_mappings (source, source_chapter_id, chapter_id, work_id, work_mapping_id, chapter_number, chapter_sort_key, status)
       values ('kuro', 'k-5', $1, $2, $3, 5, 5, 'IMPORTING')`,
      [ch5Id, testWorkId, testWorkMappingId]
    );

    // Insert Cap 6 which is ready and STAGED
    const ch6Id = await createValidChapter(6, 'Cap 6');

    await barrier.stageChapter({
      workId: testWorkId,
      chapterId: ch6Id,
      chapterNumber: 6,
      sortKey: 6,
      source: 'kuro',
      sourceChapterId: 'k-6',
      workMappingId: testWorkMappingId,
      pageCount: 1,
    });

    // Cap 6 is blocked by Cap 5
    const pub6Before = await barrier.tryPublish(testWorkId, 6, ch6Id);
    expect(pub6Before.published).toBe(false);

    // Cap 5 suffers definite failure: handleDefiniteFailure marks gap
    await barrier.handleDefiniteFailure(testWorkId, 5, 5, 'kuro');

    // Cap 6 should now be unblocked and published via cascade!
    const check6After = await db.query(`select published_at from public.chapters where id = $1`, [ch6Id]);
    expect((check6After.rows[0] as any).published_at).not.toBeNull();
  });
});
