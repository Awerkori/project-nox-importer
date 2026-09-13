import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { RetryPolicy } from '../src/core/retry-policy.js';
import { NoxWorkerStorageError } from '../src/storage/worker.js';

describe('Persistent Jobs & Absolute Priority Guarantees', () => {
  let db: PGlite;
  const editorId = '00000000-0000-0000-0000-000000000001';

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

    // Set up editor user in auth and access_roles
    await db.query(`
      insert into auth.users (id, email, email_confirmed_at)
      values ($1, 'editor@nox.test', now())
      on conflict (id) do update set email_confirmed_at = now()
    `, [editorId]);

    await db.query(`
      insert into public.members (id, username, display_name)
      values ($1, 'editor_test', 'Editor Test')
      on conflict (id) do nothing
    `, [editorId]);

    await db.query(`
      insert into public.access_roles (user_id, role, suspended)
      values ($1, 'ADMIN', false)
      on conflict (user_id) do update set role = 'ADMIN', suspended = false
    `, [editorId]);

    // Insert active source
    await db.query(`
      insert into public.importer_sources (id, name, base_url, enabled, status)
      values ('nexus', 'Nexus', 'https://nexus.test', true, 'ACTIVE')
      on conflict (id) do update set enabled = true, status = 'ACTIVE'
    `);
  });

  afterAll(async () => {
    await db.close();
  });

  it('never marks technical errors as FAILED on max_attempts (normal jobs persist)', () => {
    const err502 = new NoxWorkerStorageError('http', 502, 'Bad Gateway 502');
    const c502 = RetryPolicy.classify(err502);

    // Run across 10 attempts
    for (let attempt = 1; attempt <= 10; attempt++) {
      const decision = RetryPolicy.decide(c502, attempt, 5);
      expect(decision.status).toBe('RETRY');
      expect(decision.delaySeconds).toBeGreaterThanOrEqual(30);
      expect(decision.delaySeconds).toBeLessThanOrEqual(300);
    }
  });

  it('strictly pauses other works when an Absolute Priority work is in RETRY', async () => {
    const workA = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
    const workB = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';

    await db.query(`
      insert into public.works (id, title, slug)
      values
        ($1, 'Obra A (Prioridade)', 'obra-a'),
        ($2, 'Obra B (Normal)', 'obra-b')
      on conflict (id) do nothing
    `, [workA, workB]);

    // Put Obra A in Absolute Priority (status: RETRYING)
    await db.query(`
      insert into public.importer_staff_requests (id, work_id, requested_by, priority_boost, status)
      values ('11111111-1111-1111-1111-111111111111', $1, $2, 100, 'RETRYING')
    `, [workA, editorId]);

    // Queue a normal job for Obra B
    await db.query(`
      insert into public.importer_queue (task_type, source, priority, dedupe_key, status, payload, next_run_at)
      values ('SYNC_WORK', 'nexus', 30, 'sync:b', 'QUEUED', ('{"workId":"' || $1::text || '"}')::jsonb, now() - interval '1 minute')
    `, [workB]);

    // Also queue a retrying job for Obra A whose next_run_at is 5 minutes in the future
    await db.query(`
      insert into public.importer_queue (task_type, source, priority, dedupe_key, status, payload, next_run_at)
      values ('SYNC_WORK', 'nexus', 100, 'sync:a', 'RETRY', ('{"workId":"' || $1::text || '"}')::jsonb, now() + interval '5 minutes')
    `, [workA]);

    // Try to acquire job: must return 0 rows (strictly blocks Obra B while Obra A is retrying)
    const acquired = await db.query(`
      select * from public.importer_acquire_job('worker-test-1', interval '5 minutes', null)
    `);
    expect(acquired.rows.length).toBe(0);
  });

  it('only explicit human cancellation marks staff request as CANCELLED with STAFF_CANCELLED', async () => {
    const reqId = '11111111-1111-1111-1111-111111111111';

    // Call importer_cancel_staff_request as editor
    await db.query(`set request.jwt.claim.sub = '${editorId}'`);
    const cancelRes = await db.query(`select public.importer_cancel_staff_request($1) as res`, [reqId]);
    const res = (cancelRes.rows[0] as any).res;
    expect(res.success).toBe(true);

    const checkReq = await db.query(`select * from public.importer_staff_requests where id = $1`, [reqId]);
    const row = checkReq.rows[0] as any;
    expect(row.status).toBe('CANCELLED');
    expect(row.cancel_reason).toBe('STAFF_CANCELLED');
    expect(row.cancelled_by).toBe(editorId);
    expect(row.cancelled_at).not.toBeNull();

    // Now that priority is cancelled, regular queue resumes and Obra B can be acquired!
    const acquiredNow = await db.query(`
      select * from public.importer_acquire_job('worker-test-1', interval '5 minutes', null)
    `);
    expect(acquiredNow.rows.length).toBe(1);
    expect((acquiredNow.rows[0] as any).payload.workId).toBe('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb');
  });
});
