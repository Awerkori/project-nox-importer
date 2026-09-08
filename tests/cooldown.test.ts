import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { ImporterEngine } from '../src/core/engine.js';
import { ImporterQueue } from '../src/core/queue.js';
import { DeduplicationEngine } from '../src/core/deduplication.js';
import { CheckpointManager } from '../src/core/checkpoint.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { MockStorageProvider } from '../src/storage/mock.js';
import { SourceRegistry } from '../src/sources/registry.js';
import { SourceAdapter } from '../src/sources/types.js';

describe('Source Status Lifecycle & Persistent COOLDOWN', () => {
  let db: PGlite;
  let supabaseMock: any;
  let rateLimiter: HostRateLimiter;
  let registry: SourceRegistry;
  let queue: ImporterQueue;
  let deduplication: DeduplicationEngine;
  let checkpoints: CheckpointManager;
  let storage: MockStorageProvider;
  let engine: ImporterEngine;

  const botUserId = '00000000-0000-0000-0000-000000000001';

  beforeAll(async () => {
    db = new PGlite({ extensions: { pg_trgm } });

    await db.exec(`
      create role anon;
      create role authenticated;
      create role service_role bypassrls;
      create schema if not exists auth;
      create schema if not exists storage;
      create table if not exists auth.users (
        id uuid primary key,
        email text,
        email_confirmed_at timestamptz,
        raw_user_meta_data jsonb
      );
      create or replace function auth.uid() returns uuid language sql stable as $$
        select nullif(current_setting('request.jwt.claim.sub', true), '')::uuid
      $$;
      create table if not exists storage.buckets (
        id text primary key,
        name text,
        public boolean,
        file_size_limit bigint,
        allowed_mime_types text[]
      );
      create table if not exists storage.objects (
        id uuid primary key default gen_random_uuid(),
        bucket_id text,
        name text,
        owner uuid,
        created_at timestamptz default now(),
        updated_at timestamptz default now()
      );
      grant usage on schema public, auth, storage to anon, authenticated, service_role;
    `);

    const mangaMigrationsDir = resolve('/home/awerkori/.Projects/project-nox-manga/supabase/migrations');
    const migrationFiles = readdirSync(mangaMigrationsDir).filter((f) => f.endsWith('.sql')).sort();
    for (const f of migrationFiles) {
      const sql = readFileSync(resolve(mangaMigrationsDir, f), 'utf8')
        .replace('create extension if not exists pgcrypto;', '');
      await db.exec(sql);
    }
    await db.exec(readFileSync(resolve('migrations/001_importer_schema.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/002_importer_sources_status.sql'), 'utf8'));

    await db.query(`insert into auth.users (id, email, email_confirmed_at) values ($1, 'bot@projectnox.com', now())`, [botUserId]);
    await db.query(`update public.access_roles set role = 'ADMIN' where user_id = $1`, [botUserId]);

    supabaseMock = {
      from: (table: string) => {
        let filters: Array<{ col: string; op: string; val: any }> = [];
        let limitVal: number | null = null;
        let orderCol: string | null = null;
        let isSingle = false;
        let isMaybeSingle = false;

        const builder: any = {
          select: () => builder,
          eq: (col: string, val: any) => { filters.push({ col, op: '=', val }); return builder; },
          lt: (col: string, val: any) => { filters.push({ col, op: '<', val }); return builder; },
          gt: (col: string, val: any) => { filters.push({ col, op: '>', val }); return builder; },
          limit: (n: number) => { limitVal = n; return builder; },
          order: (col: string) => { orderCol = col; return builder; },
          single: () => { isSingle = true; return builder; },
          maybeSingle: () => { isMaybeSingle = true; return builder; },
          then: async (resolve: any) => {
            let q = `select * from public.${table}`;
            const params: any[] = [];
            if (filters.length > 0) {
              const conds = filters.map((f, i) => {
                params.push(f.val);
                return `${f.col} ${f.op} $${params.length}`;
              });
              q += ` where ${conds.join(' and ')}`;
            }
            if (orderCol) q += ` order by ${orderCol}`;
            if (limitVal) q += ` limit ${limitVal}`;

            try {
              const res = await db.query(q, params);
              if (isSingle) {
                if (res.rows.length === 0) resolve({ data: null, error: new Error('Row not found') });
                else resolve({ data: res.rows[0], error: null });
              } else if (isMaybeSingle) {
                resolve({ data: res.rows[0] || null, error: null });
              } else {
                resolve({ data: res.rows, error: null });
              }
            } catch (err: any) {
              resolve({ data: null, error: err });
            }
          },
          insert: (row: any) => ({
            then: async (resolve: any) => {
              const keys = Object.keys(row);
              const vals = Object.values(row);
              const cols = keys.join(', ');
              const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
              try {
                const res = await db.query(`insert into public.${table} (${cols}) values (${placeholders}) returning *`, vals);
                resolve({ data: res.rows[0], error: null });
              } catch (err: any) {
                resolve({ data: null, error: err });
              }
            },
          }),
          update: (values: Record<string, any>) => ({
            eq: async (col: string, val: any) => {
              const keys = Object.keys(values);
              const sets = keys.map((k, i) => `${k} = $${i + 1}`).join(', ');
              const params = keys.map((k) => values[k]);
              params.push(val);
              await db.query(`update public.${table} set ${sets} where ${col} = $${params.length}`, params);
              return { error: null };
            },
          }),
        };
        return builder;
      },
      rpc: async (fn: string, args: Record<string, any>) => {
        if (fn === 'importer_acquire_job') {
          const res = await db.query(
            `select * from public.importer_acquire_job($1, ($2 || ' seconds')::interval)`,
            [args.p_worker_id, args.p_lease_duration_seconds || 300]
          );
          return { data: res.rows, error: null };
        }
        if (fn === 'importer_release_job') {
          await db.query(
            `select public.importer_release_job($1, $2, $3, $4, ($5 || ' minutes')::interval)`,
            [args.p_job_id, args.p_worker_id, args.p_status, args.p_error || null, args.p_retry_delay_minutes || null]
          );
          return { error: null };
        }
        return { error: null };
      },
    };

    rateLimiter = new HostRateLimiter(10.0);
    registry = new SourceRegistry(rateLimiter);
    storage = new MockStorageProvider();

    const config = {
      WORKER_ID: 'test-cooldown-worker',
      POLL_INTERVAL_SECONDS: 0.05,
      QUEUE_LEASE_DURATION_SECONDS: 30,
      QUEUE_HEARTBEAT_INTERVAL_SECONDS: 5,
      MAX_CONCURRENT_JOBS: 1,
    } as any;

    engine = new ImporterEngine(
      supabaseMock,
      storage,
      registry,
      rateLimiter,
      config
    );
    queue = (engine as any).queue;
  });

  afterAll(async () => {
    await db.close();
  });

  it('does not schedule jobs for sources with status = PAUSED or DISABLED', async () => {
    // kuro, mangaflix, manhastro, toonlivre are seeded as PAUSED and enabled=false
    await (engine as any).scheduleSources();

    const queuedJobs = await db.query(`select * from public.importer_queue where status = 'QUEUED'`);
    // Only 'nexus' (ACTIVE) could be scheduled, none of the PAUSED ones
    const sourcesInQueue = queuedJobs.rows.map((r: any) => r.source);
    expect(sourcesInQueue).not.toContain('kuro');
    expect(sourcesInQueue).not.toContain('toonlivre');
    expect(sourcesInQueue).not.toContain('mangaflix');
    expect(sourcesInQueue).not.toContain('manhastro');
  });

  it('skips scheduling for sources in active COOLDOWN', async () => {
    // Put nexus in COOLDOWN until 1 hour in future
    const future = new Date(Date.now() + 3600 * 1000).toISOString();
    await db.query(`update public.importer_sources set status = 'COOLDOWN', cooldown_until = $1 where id = 'nexus'`, [future]);

    // Clear queue
    await db.query(`delete from public.importer_queue`);

    await (engine as any).scheduleSources();

    const queued = await db.query(`select * from public.importer_queue where source = 'nexus'`);
    expect(queued.rows.length).toBe(0);
  });

  it('automatically transitions expired COOLDOWN back to ACTIVE and schedules jobs', async () => {
    // Put nexus in COOLDOWN in the past (expired)
    const past = new Date(Date.now() - 5000).toISOString();
    await db.query(`
      update public.importer_sources
      set status = 'COOLDOWN', cooldown_until = $1, last_sync_at = null
      where id = 'nexus'
    `, [past]);

    await (engine as any).scheduleSources();

    // Verify source status flipped back to ACTIVE and cooldown_until is cleared
    const res = await db.query(`select status, cooldown_until from public.importer_sources where id = 'nexus'`);
    expect((res.rows[0] as any).status).toBe('ACTIVE');
    expect((res.rows[0] as any).cooldown_until).toBeNull();

    // Verify job was scheduled
    const queued = await db.query(`select * from public.importer_queue where source = 'nexus'`);
    expect(queued.rows.length).toBeGreaterThanOrEqual(1);
  });

  it('transitions source to COOLDOWN upon receiving HTTP 429 rate limit error', async () => {
    // Register mock adapter that triggers 429
    const failingAdapter: SourceAdapter = {
      id: 'failing-source',
      name: 'Failing Source',
      baseUrl: 'https://fail.example.com',
      fetchUpdatedWorks: async () => {
        const err: any = new Error('HTTP 429 Too Many Requests');
        err.status = 429;
        err.retryAfter = '120'; // 2 minutes
        throw err;
      },
      fetchWorkDetails: async () => ({} as any),
      fetchChapters: async () => [],
      fetchChapterPages: async () => [],
    };
    registry.register(failingAdapter);

    await db.query(`
      insert into public.importer_sources (id, name, base_url, enabled, status, rate_limit_per_second, sync_interval_minutes)
      values ('failing-source', 'Failing Source', 'https://fail.example.com', true, 'ACTIVE', 2.0, 30)
      on conflict (id) do update set status = 'ACTIVE', cooldown_until = null;
    `);

    // Clear any previous jobs from earlier tests
    await db.query(`delete from public.importer_queue`);
    await db.query(`update public.importer_sources set last_sync_at = now()`);

    // Enqueue a job for failing-source
    await queue.enqueue('DISCOVER_WORKS', 'failing-source', 'fail:discover:1');

    // Run discrete engine step
    await engine.step();

    // Verify source transitioned to COOLDOWN with cooldown_until ~ 120s in the future
    const res = await db.query(`select status, cooldown_until from public.importer_sources where id = 'failing-source'`);
    expect((res.rows[0] as any).status).toBe('COOLDOWN');
    expect((res.rows[0] as any).cooldown_until).not.toBeNull();

    const cooldownDate = new Date((res.rows[0] as any).cooldown_until).getTime();
    expect(cooldownDate).toBeGreaterThan(Date.now());
  });
});
