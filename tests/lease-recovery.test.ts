import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { ImporterQueue } from '../src/core/queue.js';

describe('Generic Lease Recovery System', () => {
  let db: PGlite;
  let supabaseMock: any;
  let queue: ImporterQueue;
  const testWorkerId = 'nox-worker-test-lease-rec';

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
    await db.exec(readFileSync(resolve('migrations/007_importer_lease_recovery.sql'), 'utf8'));

    // Create a mock Supabase client that routes to PGlite
    supabaseMock = {
      from: (table: string) => ({
        select: (cols = '*') => ({
          eq: (col: string, val: any) => ({
            lt: async (ltCol: string, ltVal: any) => {
              const res = await db.query(
                `select ${cols} from public.${table} where ${col} = $1 and ${ltCol} < $2`,
                [val, ltVal]
              );
              return { data: res.rows, error: null };
            },
          }),
        }),
        update: (updates: Record<string, any>) => ({
          in: (inCol: string, inVals: any[]) => ({
            eq: async (eqCol: string, eqVal: any) => {
              if (inVals.length === 0) return { data: [], error: null };
              const setClauses: string[] = [];
              const params: any[] = [];
              let idx = 1;

              for (const [k, v] of Object.entries(updates)) {
                setClauses.push(`${k} = $${idx++}`);
                params.push(v);
              }

              const placeholders = inVals.map(() => `$${idx++}`).join(',');
              params.push(...inVals);

              const eqParamIdx = idx++;
              params.push(eqVal);

              const sql = `update public.${table} set ${setClauses.join(', ')} where ${inCol} in (${placeholders}) and ${eqCol} = $${eqParamIdx}`;
              try {
                await db.query(sql, params);
                return { error: null };
              } catch (e: any) {
                return { error: e };
              }
            },
          }),
        }),
      }),
      rpc: async (fn: string, params: Record<string, any> = {}) => {
        try {
          if (fn === 'importer_recover_stalled_leases') {
            const res = await db.query(`select * from public.importer_recover_stalled_leases()`);
            return { data: res.rows, error: null };
          }
          return { data: null, error: new Error(`Unknown RPC ${fn}`) };
        } catch (e: any) {
          return { data: null, error: e };
        }
      },
    };

    queue = new ImporterQueue(supabaseMock, testWorkerId);
  });

  afterAll(async () => {
    await db.close();
  });

  it('atomically resets expired IMPORTING jobs under max_attempts to QUEUED', async () => {
    // Insert test source
    await db.query(`
      insert into public.importer_sources (id, name, base_url, enabled)
      values ('test_source', 'Test Source', 'https://test.example', true)
      on conflict (id) do nothing
    `);

    // Insert an expired job that crashed while IMPORTING (attempts: 1, max_attempts: 5)
    const expiredJobRes = await db.query(`
      insert into public.importer_queue (
        task_type, source, priority, dedupe_key, status,
        attempts, max_attempts, locked_by, locked_at, lease_expires_at, next_run_at
      ) values (
        'IMPORT_CHAPTER', 'test_source', 30, 'test:chap:expired-1', 'IMPORTING',
        1, 5, 'crashed-worker-old', now() - interval '20 minutes', now() - interval '15 minutes', now() - interval '20 minutes'
      ) returning id
    `);
    const expiredJobId = (expiredJobRes.rows[0] as any).id;

    // Run recovery
    const result = await queue.recoverExpiredLeases();
    expect(result.recovered).toBeGreaterThanOrEqual(1);

    // Verify job in database
    const checkRes = await db.query(`select * from public.importer_queue where id = $1`, [expiredJobId]);
    const job = checkRes.rows[0] as any;

    expect(job.status).toBe('QUEUED');
    expect(job.locked_by).toBeNull();
    expect(job.locked_at).toBeNull();
    expect(job.lease_expires_at).toBeNull();
    expect(job.attempts).toBe(1); // Attempts preserved!
    expect(new Date(job.next_run_at).getTime()).toBeLessThanOrEqual(Date.now() + 1000);
  });

  it('marks expired IMPORTING jobs at or exceeding max_attempts as FAILED', async () => {
    // Insert an expired job that has exhausted all attempts (attempts: 5, max: 5)
    const deadJobRes = await db.query(`
      insert into public.importer_queue (
        task_type, source, priority, dedupe_key, status,
        attempts, max_attempts, locked_by, locked_at, lease_expires_at, next_run_at
      ) values (
        'IMPORT_CHAPTER', 'test_source', 30, 'test:chap:expired-dead', 'IMPORTING',
        5, 5, 'crashed-worker-dead', now() - interval '10 minutes', now() - interval '5 minutes', now() - interval '10 minutes'
      ) returning id
    `);
    const deadJobId = (deadJobRes.rows[0] as any).id;

    const result = await queue.recoverExpiredLeases();
    expect(result.failed).toBeGreaterThanOrEqual(1);

    const checkRes = await db.query(`select * from public.importer_queue where id = $1`, [deadJobId]);
    const job = checkRes.rows[0] as any;

    expect(job.status).toBe('FAILED');
    expect(job.locked_by).toBeNull();
    expect(job.lease_expires_at).toBeNull();
    expect(job.attempts).toBe(5);
    expect(job.last_error).toContain('max attempts');
  });

  it('does NOT touch active jobs with valid unexpired leases', async () => {
    // Insert an active job with lease in the future
    const activeJobRes = await db.query(`
      insert into public.importer_queue (
        task_type, source, priority, dedupe_key, status,
        attempts, max_attempts, locked_by, locked_at, lease_expires_at, next_run_at
      ) values (
        'IMPORT_CHAPTER', 'test_source', 30, 'test:chap:active-valid', 'IMPORTING',
        1, 5, 'live-worker-1', now(), now() + interval '5 minutes', now()
      ) returning id
    `);
    const activeJobId = (activeJobRes.rows[0] as any).id;

    await queue.recoverExpiredLeases();

    const checkRes = await db.query(`select * from public.importer_queue where id = $1`, [activeJobId]);
    const job = checkRes.rows[0] as any;

    expect(job.status).toBe('IMPORTING');
    expect(job.locked_by).toBe('live-worker-1');
  });
});
