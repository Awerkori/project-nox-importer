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

describe('24/7 Daemon Simulation & Restart Recovery', () => {
  let db: PGlite;
  let supabaseMock: any;
  let storage: MockStorageProvider;
  let rateLimiter: HostRateLimiter;
  let registry: SourceRegistry;
  const botUserId = '00000000-0000-4000-8000-000000000001';

  const samplePngBytes = new Uint8Array([
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
    0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
    0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89,
    0x00, 0x00, 0x00, 0x0a, 0x49, 0x44, 0x41, 0x54,
    0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00, 0x05, 0x00, 0x01,
    0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00,
    0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
  ]);

  const sampleJpeg = new Uint8Array([
    0xff, 0xd8, 0xff, 0xc0, 0x00, 0x0b, 0x08, 0x00, 0x02, 0x00, 0x02, 0x01, 0x01, 0x11, 0x00,
    0xff, 0xda, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00, 0x3f, 0x00, 0xff, 0xd9,
  ]);

  let lastDiscoveredMode: string | undefined;

  beforeAll(async () => {
    db = new PGlite({ extensions: { pg_trgm } });

    await db.exec(`
      create role anon;
      create role authenticated;
      create role service_role bypassrls;
      create schema if not exists auth;
      create schema if not exists storage;
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
        .replace('create extension if not exists pgcrypto;', '');
      await db.exec(sql);
    }
    await db.exec(readFileSync(resolve('migrations/001_importer_schema.sql'), 'utf8'));

    await db.query(`insert into auth.users (id, email, email_confirmed_at) values ($1, 'bot@projectnox.com', now())`, [botUserId]);
    await db.query(`update public.access_roles set role = 'ADMIN' where user_id = $1`, [botUserId]);

    const mockAdapter: SourceAdapter = {
      id: 'nexus',
      name: 'Nexus Mangas',
      baseUrl: 'https://www.nexusmangas.com',
      fetchUpdatedWorks: async (_cursor, options) => {
        lastDiscoveredMode = options?.mode;
        if (options?.mode === 'bootstrap') {
          return {
            works: [
              {
                sourceWorkId: 'nx-sim-work-1',
                title: 'Simulation Chronicles',
                slug: 'simulation-chronicles',
                coverUrl: 'https://cdn.nexusmangas.com/sim-cover.png',
              }
            ],
            nextCursor: null, // Signals end of historical catalog bootstrap!
          };
        } else {
          return {
            works: [],
            nextCursor: '2026-09-08T12:00:00Z',
          };
        }
      },
      fetchWorkDetails: async (id: string) => ({
        sourceWorkId: id,
        title: 'Simulation Chronicles',
        slug: 'simulation-chronicles',
        synopsis: 'A continuous 24/7 simulation test in Project Nox.',
        kind: 'MANHWA',
        status: 'ONGOING',
        coverUrl: 'https://cdn.nexusmangas.com/sim-cover.png',
      }),
      fetchChapters: async () => [
        {
          sourceChapterId: 'nx-sim-chap-1',
          number: 1,
          title: 'Bootstrap Chapter',
          pageCount: 2,
        }
      ],
      fetchChapterPages: async () => [
        'https://cdn.nexusmangas.com/page-1.png',
        'https://cdn.nexusmangas.com/page-2.png',
      ]
    };

    const originalFetch = globalThis.fetch;
    globalThis.fetch = async (url: any, init?: any) => {
      const urlStr = url.toString();
      if (urlStr.includes('page-1') || urlStr.includes('cover')) {
        return {
          ok: true,
          status: 200,
          headers: new Headers({ 'Content-Type': 'image/png' }),
          arrayBuffer: async () => samplePngBytes.buffer.slice(samplePngBytes.byteOffset, samplePngBytes.byteOffset + samplePngBytes.byteLength),
        } as any;
      }
      if (urlStr.includes('page-2')) {
        return {
          ok: true,
          status: 200,
          headers: new Headers({ 'Content-Type': 'image/jpeg' }),
          arrayBuffer: async () => sampleJpeg.buffer.slice(sampleJpeg.byteOffset, sampleJpeg.byteOffset + sampleJpeg.byteLength),
        } as any;
      }
      return originalFetch(url, init);
    };

    supabaseMock = {
      from: (table: string) => ({
        select: (...cols: any[]) => ({
          eq: (col: string, val: any) => ({
            eq: (col2: string, val2: any) => {
              const execQuery = async () => {
                const res = await db.query(`select * from public.${table} where ${col} = $1 and ${col2} = $2 limit 1`, [val, val2]);
                return { data: res.rows[0] || null, error: null };
              };
              return {
                maybeSingle: execQuery,
                single: execQuery,
                limit: (n: number) => ({ maybeSingle: execQuery, single: execQuery }),
              };
            },
            lt: (col2: string, val2: any) => ({
              then: (resolve: any) => {
                db.query(`select * from public.${table} where ${col} = $1 and ${col2} < $2`, [val, val2])
                  .then(r => resolve({ data: r.rows, error: null }));
              }
            }),
            maybeSingle: async () => {
              const res = await db.query(`select * from public.${table} where ${col} = $1 limit 1`, [val]);
              return { data: res.rows[0] || null, error: null };
            },
            single: async () => {
              const res = await db.query(`select * from public.${table} where ${col} = $1 limit 1`, [val]);
              return { data: res.rows[0] || null, error: null };
            },
            limit: (n: number) => ({
              maybeSingle: async () => {
                const res = await db.query(`select * from public.${table} where ${col} = $1 limit 1`, [val]);
                return { data: res.rows[0] || null, error: null };
              }
            }),
            then: (resolve: any) => {
              db.query(`select * from public.${table} where ${col} = $1`, [val]).then(r => resolve({ data: r.rows, error: null }));
            }
          }),
          ilike: (col: string, val: any) => ({
            then: (resolve: any) => {
              db.query(`select * from public.${table} where ${col} ilike $1`, [val]).then(r => resolve({ data: r.rows, error: null }));
            }
          }),
          in: (col: string, vals: any[]) => ({
            then: (resolve: any) => {
              if (vals.length === 0) return resolve({ data: [], error: null });
              const placeholders = vals.map((_, i) => `$${i + 1}`).join(',');
              db.query(`select * from public.${table} where ${col} in (${placeholders})`, vals)
                .then(r => resolve({ data: r.rows, error: null }));
            }
          }),
          then: (resolve: any) => {
            db.query(`select * from public.${table}`).then(r => resolve({ data: r.rows, error: null }));
          }
        }),
        insert: (row: any) => ({
          then: (resolve: any) => {
            const keys = Object.keys(row);
            const vals = Object.values(row);
            const cols = keys.join(', ');
            const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
            db.query(`insert into public.${table} (${cols}) values (${placeholders}) returning *`, vals)
              .then(r => resolve({ data: r.rows[0], error: null }))
              .catch(err => resolve({ data: null, error: err }));
          }
        }),
        update: (row: any) => ({
          eq: (col: string, val: any) => ({
            eq: (col2: string, val2: any) => ({
              then: (resolve: any) => {
                const keys = Object.keys(row);
                const vals = Object.values(row);
                const setClause = keys.map((k, i) => `${k} = $${i + 1}`).join(', ');
                db.query(`update public.${table} set ${setClause} where ${col} = $${keys.length + 1} and ${col2} = $${keys.length + 2} returning *`, [...vals, val, val2])
                  .then(r => resolve({ data: r.rows, error: null }))
                  .catch(err => resolve({ data: null, error: err }));
              }
            }),
            then: (resolve: any) => {
              const keys = Object.keys(row);
              const vals = Object.values(row);
              const setClause = keys.map((k, i) => `${k} = $${i + 1}`).join(', ');
              db.query(`update public.${table} set ${setClause} where ${col} = $${keys.length + 1} returning *`, [...vals, val])
                .then(r => resolve({ data: r.rows, error: null }))
                .catch(err => resolve({ data: null, error: err }));
            }
          })
        }),
        upsert: (row: any, opts?: any) => ({
          select: () => ({
            single: async () => {
              const keys = Object.keys(row);
              const vals = Object.values(row);
              const conflictCols = (opts?.onConflict || 'id').split(',');
              const updateCols = keys.filter(k => !conflictCols.includes(k));
              const setClause = updateCols.map(k => `${k} = excluded.${k}`).join(', ');
              const res = await db.query(`
                insert into public.${table} (${keys.join(', ')})
                values (${keys.map((_, i) => `$${i + 1}`).join(', ')})
                on conflict (${conflictCols.join(', ')}) do update set ${setClause}
                returning *
              `, vals);
              return { data: res.rows[0], error: null };
            }
          }),
          then: (resolve: any) => {
            const keys = Object.keys(row);
            const vals = Object.values(row);
            const conflictCols = (opts?.onConflict || 'id').split(',');
            const updateCols = keys.filter(k => !conflictCols.includes(k));
            const setClause = updateCols.map(k => `${k} = excluded.${k}`).join(', ');
            db.query(`
              insert into public.${table} (${keys.join(', ')})
              values (${keys.map((_, i) => `$${i + 1}`).join(', ')})
              on conflict (${conflictCols.join(', ')}) do update set ${setClause}
              returning *
            `, vals)
              .then(r => resolve({ data: r.rows[0], error: null }))
              .catch(err => resolve({ data: null, error: err }));
          }
        })
      }),
      rpc: async (funcName: string, args: any) => {
        if (funcName === 'importer_acquire_job') {
          const res = await db.query(
            `select * from public.importer_acquire_job($1, $2::interval)`,
            [args.p_worker_id, args.p_lease_duration || '5 minutes']
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
        throw new Error(`Unmocked RPC: ${funcName}`);
      }
    };

    storage = new MockStorageProvider();
    rateLimiter = new HostRateLimiter(100);
    registry = new SourceRegistry(rateLimiter);
    registry.register(mockAdapter);
  });

  afterAll(async () => {
    await db.close();
  });

  it('transitions cleanly from bootstrap to maintenance mode upon catalog completion', async () => {
    const config: Config = {
      SUPABASE_URL: 'https://placeholder.supabase.co',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder',
      STORAGE_PROVIDER: 'mock',
      WORKER_ID: 'daemon-worker-1',
      POLL_INTERVAL_SECONDS: 60,
      QUEUE_LEASE_DURATION_SECONDS: 300,
      QUEUE_HEARTBEAT_INTERVAL_SECONDS: 60,
      MAX_CONCURRENT_CHAPTERS: 1,
      BATCH_PAGE_DOWNLOAD_CONCURRENCY: 2,
      LOG_LEVEL: 'warn',
    };

    const engine = new ImporterEngine(supabaseMock, storage, registry, rateLimiter, config);

    // Initial step: schedules DISCOVER_WORKS and executes it in bootstrap mode
    await engine.step(); // DISCOVER_WORKS
    expect(lastDiscoveredMode).toBe('bootstrap');

    // Verify checkpoint now has catalog_completed = true because nextCursor was null
    const cp = await db.query(`select * from public.importer_checkpoints where source = 'nexus'`);
    expect(cp.rows.length).toBe(1);
    const metadata = (cp.rows[0] as any).metadata;
    expect(metadata.catalog_completed).toBe(true);
    expect(metadata.catalog_completed_at).toBeDefined();

    // Process the enqueued SYNC_WORK
    await engine.step(); // SYNC_WORK

    // Process the enqueued IMPORT_CHAPTER
    await engine.step(); // IMPORT_CHAPTER

    // Trigger next interval DISCOVER_WORKS pass with a fresh dedupe key for the new maintenance window
    await db.query(`delete from public.importer_queue where task_type = 'DISCOVER_WORKS'`);
    await db.query(`update public.importer_sources set last_sync_at = now() - interval '1 hour' where id = 'nexus'`);
    const didMaintenanceStep = await engine.step(); // DISCOVER_WORKS (should now run in maintenance mode!)
    expect(didMaintenanceStep).toBe(true);
    expect(lastDiscoveredMode).toBe('maintenance');
  });

  it('recovers interrupted jobs with expired leases after worker crash without duplicate records', async () => {
    // 1. Manually insert a job locked by a crashed worker whose lease has expired
    const crashedWorkerId = 'crashed-worker-pid-999';
    const expiredLeaseAt = new Date(Date.now() - 60_000).toISOString(); // 1 minute in the past

    const { rows: workMapRows } = await db.query(`select id, work_id from public.importer_work_mappings where source = 'nexus'`);
    const workMappingId = (workMapRows[0] as any).id;
    const workId = (workMapRows[0] as any).work_id;

    await db.query(`
      insert into public.importer_queue (
        task_type, source, priority, dedupe_key, payload, status, attempts, locked_by, locked_at, lease_expires_at
      ) values (
        'IMPORT_CHAPTER',
        'nexus',
        30,
        'nexus:chapter:nx-sim-chap-recovered',
        jsonb_build_object(
          'sourceWorkId', 'nx-sim-work-1',
          'sourceChapterId', 'nx-sim-chap-recovered',
          'workId', $1::text,
          'workMappingId', $2::text,
          'chapterNumber', 2,
          'chapterTitle', 'Recovered Chapter',
          'expectedPageCount', 2
        ),
        'IMPORTING',
        1,
        $3,
        now() - interval '10 minutes',
        $4
      )
    `, [workId, workMappingId, crashedWorkerId, expiredLeaseAt]);

    // 2. Start a new worker daemon instance
    const rebootConfig: Config = {
      SUPABASE_URL: 'https://placeholder.supabase.co',
      SUPABASE_SERVICE_ROLE_KEY: 'placeholder',
      STORAGE_PROVIDER: 'mock',
      WORKER_ID: 'rebooted-worker-pid-1000',
      POLL_INTERVAL_SECONDS: 60,
      QUEUE_LEASE_DURATION_SECONDS: 300,
      QUEUE_HEARTBEAT_INTERVAL_SECONDS: 60,
      MAX_CONCURRENT_CHAPTERS: 1,
      BATCH_PAGE_DOWNLOAD_CONCURRENCY: 2,
      LOG_LEVEL: 'warn',
    };

    const rebootedEngine = new ImporterEngine(supabaseMock, storage, registry, rateLimiter, rebootConfig);

    // Startup recovery check
    await rebootedEngine.runStartupRecovery();

    // 3. Next step: acquireNextJob() must atomically reclaim the expired job from the crashed worker
    const didWork = await rebootedEngine.step();
    expect(didWork).toBe(true);

    // Verify job completed successfully with attempts = 2
    const jobRes = await db.query(`select * from public.importer_queue where dedupe_key = 'nexus:chapter:nx-sim-chap-recovered'`);
    expect(jobRes.rows.length).toBe(1);
    expect((jobRes.rows[0] as any).status).toBe('COMPLETED');
    expect((jobRes.rows[0] as any).attempts).toBe(2);

    // 4. Verify ZERO DUPLICATION in works, chapters, and pages
    const worksCount = await db.query(`select count(*) from public.works where slug = 'simulation-chronicles'`);
    expect(parseInt((worksCount.rows[0] as any).count, 10)).toBe(1);

    const chapCount = await db.query(`select count(*) from public.chapters where work_id = $1 and number = 2`, [workId]);
    expect(parseInt((chapCount.rows[0] as any).count, 10)).toBe(1);

    const pagesCount = await db.query(`
      select count(*) from public.pages p
      join public.chapters c on p.chapter_id = c.id
      where c.work_id = $1 and c.number = 2
    `, [workId]);
    expect(parseInt((pagesCount.rows[0] as any).count, 10)).toBe(2);

    const chapMap = await db.query(`select status from public.importer_chapter_mappings where source_chapter_id = 'nx-sim-chap-recovered'`);
    expect(chapMap.rows.length).toBe(1);
    expect((chapMap.rows[0] as any).status).toBe('COMPLETED');
  });
});
