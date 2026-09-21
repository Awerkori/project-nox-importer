import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { ImporterEngine } from '../src/core/engine.js';
import { SourceRegistry } from '../src/sources/registry.js';
import { SourceAdapter } from '../src/sources/types.js';
import { MockStorageProvider } from '../src/storage/mock.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { Config } from '../src/config.js';

describe('ImporterEngine End-to-End Execution', () => {
  let db: PGlite;
  let supabaseMock: any;
  let storage: MockStorageProvider;
  let engine: ImporterEngine;
  let mockAdapter: SourceAdapter;
  const botUserId = '00000000-0000-4000-8000-000000000001';

  // Minimal valid 1x1 PNG bytes for pages
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
    await db.exec(readFileSync(resolve('migrations/006_importer_publication_barrier.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/007_importer_lease_recovery.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/20260914171000_atomic_reader_repair.sql'), 'utf8'));
    await db.exec(`ALTER TABLE public.chapters ADD COLUMN IF NOT EXISTS is_fresh_release boolean DEFAULT false;`);
    await db.exec(`ALTER TABLE public.works ADD COLUMN IF NOT EXISTS latest_chapter_published_at timestamptz;`);

    // Create bot admin member via auth.users trigger
    await db.query(`insert into auth.users (id, email, email_confirmed_at) values ($1, 'bot@projectnox.com', now())`, [botUserId]);
    await db.query(`update public.access_roles set role = 'ADMIN' where user_id = $1`, [botUserId]);
    await db.query(`update public.importer_sources set enabled = false where id != 'nexus'`);
    await db.query(`insert into public.settings (key, value) values ('catalog_discovery_enabled', 'ENABLED') on conflict (key) do update set value = 'ENABLED'`);

    // Build adapter mock
    mockAdapter = {
      id: 'nexus',
      name: 'Nexus Mangas',
      baseUrl: 'https://www.nexusmangas.com',
      fetchUpdatedWorks: async () => ({
        works: [
          {
            sourceWorkId: 'nx-pilot-work-1',
            title: 'Pilot Chronicles',
            slug: 'pilot-chronicles',
            coverUrl: 'https://cdn.nexusmangas.com/pilot-cover.png',
          }
        ],
        nextCursor: '2026-09-08T10:00:00Z',
      }),
      fetchWorkDetails: async (id: string) => ({
        sourceWorkId: id,
        title: 'Pilot Chronicles',
        slug: 'pilot-chronicles',
        synopsis: 'A brave test journey in Project Nox.',
        kind: 'MANHWA',
        status: 'ONGOING',
        coverUrl: 'https://cdn.nexusmangas.com/pilot-cover.png',
      }),
      fetchChapters: async () => [
        {
          sourceChapterId: 'nx-chap-1',
          number: 1,
          title: 'The Beginning',
          pageCount: 2,
        }
      ],
      fetchChapterPages: async () => [
        'https://cdn.nexusmangas.com/page-1.png',
        'https://cdn.nexusmangas.com/page-2.png',
      ]
    };

    // Distinct valid PNGs so each page has unique media_id
    const samplePng1 = new Uint8Array(samplePngBytes);
    const samplePng2 = new Uint8Array(samplePngBytes);
    // Make byte 16 (width in IHDR) 2 for samplePng2
    samplePng2[19] = 2; // width = 2
    // Recalculate CRC or use different valid IDAT
    const samplePngCover = new Uint8Array(samplePngBytes);
    samplePngCover[19] = 3;

    // Minimal valid JPEG for page 2
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

    const originalFetch = globalThis.fetch;
    globalThis.fetch = async (url: any, init?: any) => {
      const urlStr = url.toString();
      if (urlStr.includes('cover')) {
        return new Response(sampleCoverJpeg as Uint8Array<ArrayBuffer>, { status: 200, headers: { 'Content-Type': 'image/jpeg' } });
      }
      if (urlStr.includes('page-1')) {
        return new Response(samplePngBytes as Uint8Array<ArrayBuffer>, { status: 200, headers: { 'Content-Type': 'image/png' } });
      }
      if (urlStr.includes('page-2')) {
        return new Response(sampleJpeg as Uint8Array<ArrayBuffer>, { status: 200, headers: { 'Content-Type': 'image/jpeg' } });
      }
      return originalFetch(url, init);
    };

    // Construct Supabase client adapter for PGlite
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
        if (funcName === 'importer_check_publication_barrier') {
          const res = await db.query(
            `select * from public.importer_check_publication_barrier($1, $2)`,
            [args.p_work_id, args.p_target_sort_key]
          );
          return { data: res.rows, error: null };
        }
        throw new Error(`Unmocked RPC: ${funcName}`);
      }
    };

    storage = new MockStorageProvider();
    const rateLimiter = new HostRateLimiter(100);
    const registry = new SourceRegistry(rateLimiter);
    registry.register(mockAdapter);

    const testConfig: Config = {
      SUPABASE_URL: 'https://placeholder.supabase.co',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder',
      STORAGE_PROVIDER: 'mock',
      WORKER_ID: 'test-worker-1',
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
    await db.close();
  });

  it('runs full autonomous pipeline: discover -> sync work -> import chapter -> verify pages -> publish', async () => {
    // 1. Trigger source scheduling to enqueue DISCOVER_WORKS
    const didWork1 = await engine.step();
    expect(didWork1).toBe(true);

    // Verify DISCOVER_WORKS created a SYNC_WORK job in queue
    const syncJobs = await db.query(
      `select * from public.importer_queue where task_type = 'SYNC_WORK'`
    );
    expect(syncJobs.rows.length).toBe(1);
    expect((syncJobs.rows[0] as any).dedupe_key).toBe('nexus:work:nx-pilot-work-1');

    // 2. Run engine step to process SYNC_WORK
    const didWork2 = await engine.step();
    expect(didWork2).toBe(true);

    // Verify work mapping created
    const workMapping = await db.query(
      `select * from public.importer_work_mappings where source = 'nexus' and source_work_id = 'nx-pilot-work-1'`
    );
    expect(workMapping.rows.length).toBe(1);
    expect((workMapping.rows[0] as any).sync_status).toBe('SYNCED');
    const assignedWorkId = (workMapping.rows[0] as any).work_id;
    expect(assignedWorkId).toBeTruthy();

    // Verify IMPORT_CHAPTER was enqueued
    const chapJobs = await db.query(
      `select * from public.importer_queue where task_type = 'IMPORT_CHAPTER'`
    );
    expect(chapJobs.rows.length).toBe(1);

    // 3. Run engine step to process IMPORT_CHAPTER
    const didWork3 = await engine.step();
    expect(didWork3).toBe(true);

    // Verify chapter was inserted and PUBLISHED!
    const chapters = await db.query(
      `select * from public.chapters where work_id = $1 and number = 1`,
      [assignedWorkId]
    );
    expect(chapters.rows.length).toBe(1);
    expect((chapters.rows[0] as any).published_at).not.toBeNull();
    const chapterId = (chapters.rows[0] as any).id;

    // Verify pages were inserted into public.pages
    const pages = await db.query(
      `select * from public.pages where chapter_id = $1 order by position asc`,
      [chapterId]
    );
    expect(pages.rows.length).toBe(2);
    expect((pages.rows[0] as any).position).toBe(1);
    expect((pages.rows[1] as any).position).toBe(2);

    // Verify media deduplication / storage uploads occurred
    expect(storage.uploads.size).toBeGreaterThanOrEqual(1);

    // Verify work is now marked as published
    const work = await db.query(`select published from public.works where id = $1`, [assignedWorkId]);
    expect((work.rows[0] as any).published).toBe(true);

    // Verify importer_chapter_mappings status is COMPLETED
    const chapMap = await db.query(
      `select * from public.importer_chapter_mappings where source_chapter_id = 'nx-chap-1'`
    );
    expect(chapMap.rows.length).toBe(1);
    expect((chapMap.rows[0] as any).status).toBe('COMPLETED');
    expect((chapMap.rows[0] as any).page_count).toBe(2);
  });

  it('safely skips already completed chapters on subsequent sync runs without duplicate downloads', async () => {
    const uploadCountBefore = storage.uploads.size;

    // Enqueue SYNC_WORK again
    await db.query(`
      insert into public.importer_queue (task_type, source, priority, dedupe_key, payload, status)
      values ('SYNC_WORK', 'nexus', 20, 'manual:resync:nx-pilot-work-1', jsonb_build_object('sourceWorkId', 'nx-pilot-work-1'), 'QUEUED')
    `);

    // Process SYNC_WORK
    await engine.step();

    // No new IMPORT_CHAPTER jobs should be added because nx-chap-1 is already COMPLETED
    const newChapJobs = await db.query(
      `select * from public.importer_queue where task_type = 'IMPORT_CHAPTER' and status = 'QUEUED'`
    );
    expect(newChapJobs.rows.length).toBe(0);
    expect(storage.uploads.size).toBe(uploadCountBefore);
  });
  it('repairs an already published chapter instead of treating publication as integrity proof', async () => {
    const job: any = (await db.query("SELECT * FROM importer_queue WHERE task_type='IMPORT_CHAPTER' LIMIT 1")).rows[0];
    const chapter: any = (await db.query('SELECT id,published_at FROM chapters WHERE work_id=$1 AND number=1',[job.payload.workId])).rows[0];
    await db.query('DELETE FROM pages WHERE chapter_id=$1 AND position=2',[chapter.id]);
    const uploadsBefore = storage.uploads.size;
    await (engine as any).handleImportChapter({...job,payload:{...job.payload,readerRepair:true}});
    const pages = await db.query('SELECT position FROM pages WHERE chapter_id=$1 ORDER BY position',[chapter.id]);
    expect(pages.rows.map((p:any)=>p.position)).toEqual([1,2]);
    expect(storage.uploads.size).toBe(uploadsBefore); // Existing image bytes deduplicate; no redundant upload.
    const after: any = (await db.query('SELECT published_at FROM chapters WHERE id=$1',[chapter.id])).rows[0];
    expect(after.published_at).toEqual(chapter.published_at);
  });

});
