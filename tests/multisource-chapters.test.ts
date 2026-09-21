import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { ImporterEngine, computeCanonicalChapterKey } from '../src/core/engine.js';
import { SourceRegistry } from '../src/sources/registry.js';
import { SourceAdapter } from '../src/sources/types.js';
import { MockStorageProvider } from '../src/storage/mock.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { Config } from '../src/config.js';

describe('Multi-Source Chapter Ingestion & Canonical Deduplication', () => {
  let db: PGlite;
  let supabaseMock: any;
  let storage: MockStorageProvider;
  let engine: ImporterEngine;
  let mfAdapter: SourceAdapter;
  let kuroAdapter: SourceAdapter;
  let mfAvailableChapters: any[] = [];
  const botUserId = '00000000-0000-4000-8000-000000000001';

  const samplePngBytes = new Uint8Array([
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
    0x00, 0x00, 0x00, 0x0d,
    0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01,
    0x00, 0x00, 0x00, 0x01,
    0x08, 0x06, 0x00, 0x00, 0x00,
    0x1f, 0x15, 0xc4, 0x89,
    0x00, 0x00, 0x00, 0x0a,
    0x49, 0x44, 0x41, 0x54,
    0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00, 0x05, 0x00, 0x01,
    0x0d, 0x0a, 0x2d, 0xb4,
    0x00, 0x00, 0x00, 0x00,
    0x49, 0x45, 0x4e, 0x44,
    0xae, 0x42, 0x60, 0x82,
  ]);

  const sampleJpeg = new Uint8Array([
    0xff, 0xd8, 0xff, 0xc0, 0x00, 0x0b, 0x08, 0x00, 0x02, 0x00, 0x02, 0x01, 0x01, 0x11, 0x00,
    0xff, 0xda, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00, 0x3f, 0x00, 0xff, 0xd9,
  ]);

  const sampleCoverJpeg = new Uint8Array(2000);
  sampleCoverJpeg[0] = 0xff; sampleCoverJpeg[1] = 0xd8;
  sampleCoverJpeg[2] = 0xff; sampleCoverJpeg[3] = 0xc0;
  sampleCoverJpeg[4] = 0x00; sampleCoverJpeg[5] = 0x0b;
  sampleCoverJpeg[6] = 0x08;
  sampleCoverJpeg[7] = 0x01; sampleCoverJpeg[8] = 0x2c; // height = 300
  sampleCoverJpeg[9] = 0x00; sampleCoverJpeg[10] = 0xc8; // width = 200
  sampleCoverJpeg[11] = 0x01; sampleCoverJpeg[12] = 0x01; sampleCoverJpeg[13] = 0x11; sampleCoverJpeg[14] = 0x00;
  sampleCoverJpeg[15] = 0xff; sampleCoverJpeg[16] = 0xda;
  sampleCoverJpeg[17] = 0x00; sampleCoverJpeg[18] = 0x08; sampleCoverJpeg[19] = 0x01; sampleCoverJpeg[20] = 0x01; sampleCoverJpeg[21] = 0x00; sampleCoverJpeg[22] = 0x00; sampleCoverJpeg[23] = 0x3f; sampleCoverJpeg[24] = 0x00;
  sampleCoverJpeg[1998] = 0xff; sampleCoverJpeg[1999] = 0xd9;

  let originalFetch: typeof globalThis.fetch;

  beforeAll(async () => {
    db = new PGlite({ extensions: { pg_trgm } });

    await db.exec(`
      create role anon;
      create role authenticated;
      create role service_role bypassrls;
      create schema if not exists auth;
      create schema if not exists storage;
      create type public.scan_member_role as enum ('LEADER', 'VICE_LEADER', 'STAFF', 'MEMBER');
      create table if not exists auth.users (id uuid primary key, email text, email_confirmed_at timestamptz, raw_user_meta_data jsonb);
      create or replace function auth.uid() returns uuid language sql stable as $$
        select nullif(current_setting('request.jwt.claim.sub', true), '')::uuid
      $$;
      create table if not exists storage.buckets (id text primary key, name text, public boolean, file_size_limit bigint, allowed_mime_types text[]);
      create table if not exists storage.objects (id uuid primary key default gen_random_uuid(), bucket_id text, name text, owner uuid, created_at timestamptz default now(), updated_at timestamptz default now(), last_accessed_at timestamptz default now(), metadata jsonb);
      grant usage on schema public, auth, storage to anon, authenticated, service_role;
    `);

    const mangaMigrationsDir = resolve('/home/awerkori/.Projects/project-nox-manga/supabase/migrations');
    const files = readdirSync(mangaMigrationsDir).filter(f => f.endsWith('.sql')).sort();
    for (const f of files) {
      const sql = readFileSync(resolve(mangaMigrationsDir, f), 'utf8')
        .replace('create extension if not exists pgcrypto;', '')
        .replace(/create\s+index\s+concurrently/gi, 'create index');
      await db.exec(sql);
    }
    await db.exec(readFileSync(resolve('migrations/001_importer_schema.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/002_importer_sources_status.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/003_importer_sort_key_and_concurrency.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/004_importer_telemetry_and_provenance.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/005_importer_page_provider_column.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/20260914171000_atomic_reader_repair.sql'), 'utf8'));
    await db.exec(`ALTER TABLE public.chapters ADD COLUMN IF NOT EXISTS is_fresh_release boolean DEFAULT false;`);
    await db.exec(`ALTER TABLE public.works ADD COLUMN IF NOT EXISTS latest_chapter_published_at timestamptz;`);

    await db.query(`insert into auth.users (id, email, email_confirmed_at) values ($1, 'bot@projectnox.com', now())`, [botUserId]);
    await db.query(`insert into public.access_roles (user_id, role) values ($1, 'ADMIN') on conflict (user_id) do update set role = 'ADMIN'`, [botUserId]);

    // Setup sources in importer_sources
    await db.query(`update public.importer_sources set enabled = false where id not in ('mangaflix', 'kuro')`);
    await db.query(`insert into public.importer_sources (id, name, base_url, status, enabled) values ('mangaflix', 'MangaFlix', 'https://mangaflix.org', 'ACTIVE', true) on conflict (id) do update set status = 'ACTIVE', enabled = true`);
    await db.query(`insert into public.importer_sources (id, name, base_url, status, enabled) values ('kuro', 'Kuro', 'https://kuro.moe', 'ACTIVE', true) on conflict (id) do update set status = 'ACTIVE', enabled = true`);
    await db.query(`insert into public.settings (key, value) values ('catalog_discovery_enabled', 'ENABLED') on conflict (key) do update set value = 'ENABLED'`);

    // Mock fetch for image downloads
    originalFetch = globalThis.fetch;
    globalThis.fetch = async (url: any) => {
      const urlStr = url.toString();
      if (urlStr.includes('cover')) {
        return new Response(sampleCoverJpeg as Uint8Array<ArrayBuffer>, { status: 200, headers: { 'Content-Type': 'image/jpeg' } });
      }
      if (urlStr.includes('page-1')) {
        return new Response(samplePngBytes as Uint8Array<ArrayBuffer>, { status: 200, headers: { 'Content-Type': 'image/png' } });
      }
      return new Response(sampleJpeg as Uint8Array<ArrayBuffer>, { status: 200, headers: { 'Content-Type': 'image/jpeg' } });
    };

    mfAvailableChapters = [
      {
        sourceChapterId: 'mf-chap-1',
        number: 1,
        title: 'Chapter 1: The Rebirth',
      },
    ];

    // Build adapter mocks
    mfAdapter = {
      id: 'mangaflix',
      name: 'MangaFlix',
      baseUrl: 'https://mangaflix.org',
      fetchUpdatedWorks: async () => ({
        works: [{
          sourceWorkId: 'mf-work-multi-1',
          title: 'Apocalyptic Knight Multi',
          slug: 'apocalyptic-knight-multi',
        }],
        nextCursor: '2026-09-08T10:00:00Z',
      }),
      fetchWorkDetails: async (id: string) => ({
        sourceWorkId: id,
        title: 'Apocalyptic Knight Multi',
        slug: 'apocalyptic-knight-multi',
        synopsis: 'Mangaflix synopsis',
        kind: 'MANHWA',
        status: 'ONGOING',
        coverUrl: 'https://cdn.mangaflix.org/cover.png',
      }),
      fetchChapters: async () => mfAvailableChapters,
      fetchChapterPages: async () => [
        'https://cdn.mangaflix.org/page-1.png',
        'https://cdn.mangaflix.org/page-2.jpg',
      ],
    };

    kuroAdapter = {
      id: 'kuro',
      name: 'Kuro',
      baseUrl: 'https://kuro.moe',
      fetchUpdatedWorks: async () => ({
        works: [{
          sourceWorkId: 'kuro-work-multi-1',
          title: 'Apocalyptic Knight Multi',
          slug: 'apocalyptic-knight-multi',
        }],
        nextCursor: '2026-09-08T10:00:00Z',
      }),
      fetchWorkDetails: async (id: string) => ({
        sourceWorkId: id,
        title: 'Apocalyptic Knight Multi',
        slug: 'apocalyptic-knight-multi',
        synopsis: 'Superior Kuro synopsis',
        author: 'Kuro Author',
        kind: 'MANHWA',
        status: 'ONGOING',
        coverUrl: 'https://cdn.kuro.moe/cover.png',
      }),
      fetchChapters: async () => [
        {
          sourceChapterId: 'kuro-chap-0',
          number: 0,
          title: 'Prologue',
        },
        {
          sourceChapterId: 'kuro-chap-1',
          number: 1,
          title: 'Chapter 1: The Rebirth (Kuro)',
        },
        {
          sourceChapterId: 'kuro-chap-2',
          number: 2,
          title: 'Chapter 2: The Second Awakening (Kuro)',
        },
      ],
      fetchChapterPages: async (sourceChapterId: string) => {
        if (sourceChapterId === 'kuro-chap-2') {
          throw new Error('Kuro chapter 2 404 upstream error');
        }
        return [
          'https://cdn.kuro.moe/page-1.png',
          'https://cdn.kuro.moe/page-2.jpg',
        ];
      },
    };

    // Construct mock Supabase
    supabaseMock = {
      db,
      pool: db,
      from: (table: string) => {
        const filters: Array<{ sql: string; vals: any[] }> = [];
        let orderStatement = '';
        let limitCount: number | null = null;

        const builder: any = {
          select: () => builder,
          eq: (col: string, val: any) => {
            filters.push({ sql: `${col} = $PARAM`, vals: [val] });
            return builder;
          },
          neq: (col: string, val: any) => {
            filters.push({ sql: `${col} != $PARAM`, vals: [val] });
            return builder;
          },
          lt: (col: string, val: any) => {
            filters.push({ sql: `${col} < $PARAM`, vals: [val] });
            return builder;
          },
          lte: (col: string, val: any) => {
            filters.push({ sql: `${col} <= $PARAM`, vals: [val] });
            return builder;
          },
          gt: (col: string, val: any) => {
            filters.push({ sql: `${col} > $PARAM`, vals: [val] });
            return builder;
          },
          gte: (col: string, val: any) => {
            filters.push({ sql: `${col} >= $PARAM`, vals: [val] });
            return builder;
          },
          ilike: (col: string, val: any) => {
            filters.push({ sql: `${col} ilike $PARAM`, vals: [val] });
            return builder;
          },
          in: (col: string, vals: any[]) => {
            if (!vals || vals.length === 0) {
              filters.push({ sql: `1 = 0`, vals: [] });
            } else {
              filters.push({ sql: `${col} = ANY($PARAM)`, vals: [vals] });
            }
            return builder;
          },
          overlaps: (col: string, vals: any[]) => {
            if (!vals || vals.length === 0) {
              filters.push({ sql: `1 = 0`, vals: [] });
            } else {
              filters.push({ sql: `${col} && $PARAM`, vals: [vals] });
            }
            return builder;
          },
          not: (col: string, op: string, val: any) => {
            if (op === 'is' && val === null) {
              filters.push({ sql: `${col} is not null`, vals: [] });
            } else {
              filters.push({ sql: `${col} != $PARAM`, vals: [val] });
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
          _execute: async (limitOne = false) => {
            const allVals: any[] = [];
            let whereClause = '';
            if (filters.length > 0) {
              const parts = filters.map(f => {
                let s = f.sql;
                for (const v of f.vals) {
                  allVals.push(v);
                  s = s.replace('$PARAM', `$${allVals.length}`);
                }
                return s;
              });
              whereClause = `where ${parts.join(' and ')}`;
            }
            let query = `select * from public.${table} ${whereClause} ${orderStatement}`;
            if (limitOne) query += ` limit 1`;
            else if (limitCount) query += ` limit ${limitCount}`;
            try {
              const res = await db.query(query, allVals);
              return { data: res.rows || [], error: null };
            } catch (err: any) {
              return { data: null, error: err };
            }
          },
          maybeSingle: async () => {
            const res = await builder._execute(true);
            return { data: res.data?.[0] || null, error: res.error };
          },
          single: async () => {
            const res = await builder._execute(true);
            return { data: res.data?.[0] || null, error: res.error };
          },
          then: (resolve: any, reject?: any) => {
            builder._execute().then((r: any) => resolve(r), reject);
          }
        };

        return {
          select: () => builder,
        insert: (row: any) => ({
          then: (resolve?: any, reject?: any) => {
            const keys = Object.keys(row);
            const vals = Object.values(row);
            const cols = keys.join(', ');
            const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
            db.query(`insert into public.${table} (${cols}) values (${placeholders}) returning *`, vals)
              .then(r => resolve?.({ data: r.rows[0], error: null }))
              .catch(err => reject ? reject(err) : resolve?.({ data: null, error: err }));
          }
        }),
        update: (row: any) => ({
          eq: (col: string, val: any) => ({
            eq: (col2: string, val2: any) => ({
              then: (resolve?: any, reject?: any) => {
                const keys = Object.keys(row);
                const vals = Object.values(row);
                const setClause = keys.map((k, i) => `${k} = $${i + 1}`).join(', ');
                db.query(`update public.${table} set ${setClause} where ${col} = $${keys.length + 1} and ${col2} = $${keys.length + 2} returning *`, [...vals, val, val2])
                  .then(r => resolve?.({ data: r.rows, error: null }))
                  .catch(err => reject ? reject(err) : resolve?.({ data: null, error: err }));
              }
            }),
            then: (resolve?: any, reject?: any) => {
              const keys = Object.keys(row);
              const vals = Object.values(row);
              const setClause = keys.map((k, i) => `${k} = $${i + 1}`).join(', ');
              db.query(`update public.${table} set ${setClause} where ${col} = $${keys.length + 1} returning *`, [...vals, val])
                .then(r => resolve?.({ data: r.rows, error: null }))
                .catch(err => reject ? reject(err) : resolve?.({ data: null, error: err }));
            }
          })
        }),
        upsert: (rowsOrRow: any, opts?: any) => {
          const rows = Array.isArray(rowsOrRow) ? rowsOrRow : [rowsOrRow];
          return {
            select: () => ({
              single: async () => {
                const row = rows[0];
                const keys = Object.keys(row);
                const vals = Object.values(row);
                const conflictCols = (opts?.onConflict || 'id').split(',');
                const updateCols = keys.filter(k => !conflictCols.includes(k));
                const setClause = updateCols.length > 0 ? updateCols.map(k => `${k} = excluded.${k}`).join(', ') : 'nothing';
                const conflictAction = updateCols.length > 0 ? `do update set ${setClause}` : 'do nothing';
                const res = await db.query(`
                  insert into public.${table} (${keys.join(', ')})
                  values (${keys.map((_, i) => `$${i + 1}`).join(', ')})
                  on conflict (${conflictCols.join(', ')}) ${conflictAction}
                  returning *
                `, vals);
                return { data: res.rows[0], error: null };
              }
            }),
            then: async (resolve?: any, reject?: any) => {
              if (rows.length === 0) return resolve?.({ data: [], error: null });
              const results: any[] = [];
              for (const row of rows) {
                const keys = Object.keys(row);
                const vals = Object.values(row);
                const conflictCols = (opts?.onConflict || 'id').split(',');
                const updateCols = keys.filter(k => !conflictCols.includes(k));
                const setClause = updateCols.length > 0 ? updateCols.map(k => `${k} = excluded.${k}`).join(', ') : 'nothing';
                const conflictAction = updateCols.length > 0 ? `do update set ${setClause}` : 'do nothing';
                try {
                  const res = await db.query(`
                    insert into public.${table} (${keys.join(', ')})
                    values (${keys.map((_, i) => `$${i + 1}`).join(', ')})
                    on conflict (${conflictCols.join(', ')}) ${conflictAction}
                    returning *
                  `, vals);
                  if (res.rows[0]) results.push(res.rows[0]);
                } catch (err: any) {
                  return reject ? reject(err) : resolve?.({ data: null, error: err });
                }
              }
              resolve?.({ data: results, error: null });
            }
          };
        }
      };
    },
    rpc: async (funcName: string, args: any) => {
        if (funcName === 'importer_replace_pages') {
          const res = await db.query('SELECT importer_replace_pages($1,$2::jsonb) count', [args.p_chapter_id, JSON.stringify(args.p_pages)]);
          return { data: res.rows[0].count, error: null };
        }
        if (funcName === 'importer_acquire_job') {
          const res = await db.query(
            `select * from public.importer_acquire_job($1, $2::interval, $3)`,
            [args.p_worker_id, args.p_lease_duration || '5 minutes', args.p_source || null]
          );
          return { data: res.rows, error: null };
        }
        if (funcName === 'importer_renew_lease') {
          const res = await db.query(
            `select public.importer_renew_lease($1, $2, $3::interval) as ok`,
            [args.p_job_id, args.p_worker_id, args.p_lease_duration || '5 minutes']
          );
          return { data: (res.rows[0] as any)?.ok, error: null };
        }
        if (funcName === 'importer_release_job') {
          const res = await db.query(
            `select public.importer_release_job($1, $2, $3, $4, $5::interval) as ok`,
            [args.p_job_id, args.p_worker_id, args.p_status, args.p_error, args.p_retry_delay]
          );
          return { data: (res.rows[0] as any)?.ok, error: null };
        }
        return { data: null, error: null };
      }
    };

    storage = new MockStorageProvider();
    const rateLimiter = new HostRateLimiter(100);
    const registry = new SourceRegistry(rateLimiter);
    registry.register(mfAdapter);
    registry.register(kuroAdapter);

    const testConfig: Config = {
      SUPABASE_URL: 'http://localhost:54321',
      SUPABASE_SERVICE_ROLE_KEY: 'test-key',
      STORAGE_PROVIDER: 'mock',
      WORKER_ID: 'test-multi-worker',
      POLL_INTERVAL_SECONDS: 60,
      QUEUE_LEASE_DURATION_SECONDS: 300,
      QUEUE_HEARTBEAT_INTERVAL_SECONDS: 60,
      MAX_CONCURRENT_CHAPTERS: 1,
      BATCH_PAGE_DOWNLOAD_CONCURRENCY: 2,
      LOG_LEVEL: 'warn',
    };

    engine = new ImporterEngine(supabaseMock, storage, registry, rateLimiter, testConfig);
  });

  afterAll(async () => {
    globalThis.fetch = originalFetch;
    await db?.close();
  });

  it('correctly computes canonical chapter keys for fractional, prologue, and specials', () => {
    const p0 = computeCanonicalChapterKey(0, 'Prólogo');
    expect(p0.sortKey).toBe(0);
    expect(p0.specialCategory).toBe('prologue');

    const c1 = computeCanonicalChapterKey(1, 'Capítulo 1');
    expect(c1.sortKey).toBe(1);

    const c15 = computeCanonicalChapterKey(1.5, 'Capítulo 1.5 Extra');
    expect(c15.sortKey).toBe(1.5);
    expect(c15.specialCategory).toBe('extra');

    const c1025 = computeCanonicalChapterKey(10.25, 'Episódio 10.25');
    expect(c1025.sortKey).toBe(10.25);

    const sp = computeCanonicalChapterKey(0, 'Especial de Natal');
    expect(sp.sortKey).toBe(0.0001);
    expect(sp.specialCategory).toBe('special');
  });

  it('multi-source pipeline: MangaFlix imports Chapter 1, then Kuro links to the canonical chapter without re-downloading', async () => {
    // 1. MangaFlix discovers works
    await engine.queue.enqueue(
      'DISCOVER_WORKS',
      'mangaflix',
      'mf:discover:1',
      {},
      10
    );

    // Step 1: processes DISCOVER_WORKS -> creates SYNC_WORK for mangaflix
    const step1 = await engine.step('mangaflix');
    expect(step1).toBe(true);

    // Step 2: processes SYNC_WORK for mangaflix -> creates IMPORT_CHAPTER for Chapter 1 and Chapter 2
    const step2 = await engine.step('mangaflix');
    expect(step2).toBe(true);

    // Step 3: processes IMPORT_CHAPTER 1 from MangaFlix
    const step3 = await engine.step('mangaflix');
    expect(step3).toBe(true);

    // Verify Chapter 1 is published in public.chapters
    const chaptersRes = await db.query(`select * from public.chapters where number = 1`);
    expect(chaptersRes.rows.length).toBe(1);
    const chapter1 = chaptersRes.rows[0] as any;
    expect(chapter1.published_at).toBeTruthy();

    const mfMappings = await db.query(`select * from public.importer_chapter_mappings where source = 'mangaflix' and source_chapter_id = 'mf-chap-1'`);
    expect(mfMappings.rows.length).toBe(1);
    expect((mfMappings.rows[0] as any).chapter_id).toBe(chapter1.id);
    expect((mfMappings.rows[0] as any).status).toBe('COMPLETED');

    const initialUploadCount = storage.uploads.size;

    // 2. Kuro now syncs the exact same work (slug 'apocalyptic-knight-multi')
    await engine.queue.enqueue(
      'SYNC_WORK',
      'kuro',
      'kuro:sync:multi-1',
      {
        workId: chapter1.work_id,
        sourceWorkId: 'kuro-work-multi-1',
        title: 'Apocalyptic Knight Multi',
        slug: 'apocalyptic-knight-multi',
      },
      20
    );

    // Process Kuro SYNC_WORK
    const stepKuroSync = await engine.step('kuro');
    expect(stepKuroSync).toBe(true);

    // Kuro should have recognized that Chapter 1 is ALREADY PUBLISHED:
    // It must create an importer_chapter_mappings entry pointing to chapter1.id as COMPLETED
    const kuroMappings = await db.query(`select * from public.importer_chapter_mappings where source = 'kuro' and source_chapter_id = 'kuro-chap-1'`);
    expect(kuroMappings.rows.length).toBe(1);
    expect((kuroMappings.rows[0] as any).chapter_id).toBe(chapter1.id);
    expect((kuroMappings.rows[0] as any).status).toBe('COMPLETED');

    // Storage uploads should NOT have increased for Chapter 1
    expect(storage.uploads.size).toBe(initialUploadCount);

    // Kuro enqueued Chapter 0 (Prologue) and Chapter 2 (in strict sort order)
    const pendingKuroJobs = await db.query(`select * from public.importer_queue where status = 'QUEUED' and source = 'kuro' and task_type = 'IMPORT_CHAPTER' order by chapter_sort_key asc`);
    expect(pendingKuroJobs.rows.length).toBe(2);
    expect(Number((pendingKuroJobs.rows[0] as any).chapter_sort_key)).toBe(0); // Prologue first
    expect(Number((pendingKuroJobs.rows[1] as any).chapter_sort_key)).toBe(2); // Chapter 2
  });

  it('fallback mechanism: when preferred Kuro fails permanently on Chapter 2, MangaFlix can fulfill Chapter 2', async () => {
    // 1. Kuro's Chapter 2 is currently in queue. Mark it FAILED permanently.
    const chap2Jobs = await db.query(`select * from public.importer_queue where dedupe_key = 'kuro:chapter:kuro-chap-2'`);
    expect(chap2Jobs.rows.length).toBe(1);
    const kuroChap2Job = chap2Jobs.rows[0] as any;

    await engine.queue.releaseJob(kuroChap2Job.id, 'FAILED', 'Upstream 404 from Kuro');
    const failedCheck = await db.query(`select * from public.importer_queue where id = $1`, [kuroChap2Job.id]);
    expect((failedCheck.rows[0] as any).status).toBe('FAILED');

    // 2. MangaFlix now has Chapter 2 available and re-syncs the work to check for missing chapters
    mfAvailableChapters.push({
      sourceChapterId: 'mf-chap-2',
      number: 2,
      title: 'Chapter 2: The Second Awakening',
    });

    const workRes = await db.query(`select id from public.works where slug = 'apocalyptic-knight-multi'`);
    const workId = (workRes.rows[0] as any).id;

    await engine.queue.enqueue(
      'SYNC_WORK',
      'mangaflix',
      `mf:sync:resync-${Date.now()}`,
      {
        workId,
        sourceWorkId: 'mf-work-multi-1',
        title: 'Apocalyptic Knight Multi',
        slug: 'apocalyptic-knight-multi',
      },
      20
    );

    // Step MangaFlix SYNC_WORK
    const stepMfSync = await engine.step('mangaflix');
    expect(stepMfSync).toBe(true);

    // Because Kuro FAILED on Chapter 2, MangaFlix should detect that Chapter 2 is missing and has no active Kuro job!
    // Therefore, MangaFlix enqueues Chapter 2
    const mfChap2Queue = await db.query(`select * from public.importer_queue where dedupe_key = 'mangaflix:chapter:mf-chap-2' and status = 'QUEUED'`);
    expect(mfChap2Queue.rows.length).toBe(1);

    // Import Chapter 2 from MangaFlix via step
    const stepMfImport = await engine.step('mangaflix');
    expect(stepMfImport).toBe(true);

    // Check that Chapter 2 is published now
    const c2InDb = await db.query(`select * from public.chapters where work_id = $1 and number = 2`, [workId]);
    expect(c2InDb.rows.length).toBe(1);
    expect((c2InDb.rows[0] as any).published_at).toBeTruthy();
  });
});
