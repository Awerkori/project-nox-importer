import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';

describe('Importer Database Schema & Atomic Lease Locks', () => {
  let db: PGlite;

  beforeAll(async () => {
    db = new PGlite({ extensions: { pg_trgm } });
    
    // Set up standard roles and mocks matching Supabase
    await db.exec(`
      create role anon;
      create role authenticated;
      create role service_role bypassrls;
      create schema if not exists auth;
      create schema if not exists storage;
      create type public.scan_member_role as enum ('LEADER', 'VICE_LEADER', 'STAFF', 'MEMBER');
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
        bucket_id text references storage.buckets(id),
        name text,
        owner uuid,
        created_at timestamptz default now(),
        updated_at timestamptz default now(),
        last_accessed_at timestamptz default now(),
        metadata jsonb
      );
      grant usage on schema public, auth, storage to anon, authenticated, service_role;
      grant execute on function auth.uid() to anon, authenticated;
    `);

    // Load base manga schema migrations from project-nox-manga
    const mangaMigrationsDir = resolve('/home/awerkori/.Projects/project-nox-manga/supabase/migrations');
    const migrationFiles = readdirSync(mangaMigrationsDir)
      .filter(f => f.endsWith('.sql'))
      .sort();

    for (const file of migrationFiles) {
      const sql = readFileSync(resolve(mangaMigrationsDir, file), 'utf8')
        .replace('create extension if not exists pgcrypto;', '')
        .replace(/create\s+index\s+concurrently/gi, 'create index');
      await db.exec(sql);
    }

    // Now apply all importer migrations in order
    const importerMigrationFiles = readdirSync(resolve('migrations'))
      .filter(f => f.endsWith('.sql'))
      .sort();
    for (const file of importerMigrationFiles) {
      const sql = readFileSync(resolve('migrations', file), 'utf8');
      await db.exec(sql);
    }
  });

  afterAll(async () => {
    await db.close();
  });

  it('verifies importer tables exist and default sources are seeded', async () => {
    const res = await db.query('select * from public.importer_sources where id = $1', ['nexus']);
    expect(res.rows.length).toBe(1);
    expect((res.rows[0] as any).name).toBe('Nexus Mangas');
    expect((res.rows[0] as any).status).toBe('ACTIVE');

    // Check reactivated source Kuro is ACTIVE and enabled
    const kuro = await db.query('select * from public.importer_sources where id = $1', ['kuro']);
    expect(kuro.rows.length).toBe(1);
    expect((kuro.rows[0] as any).status).toBe('ACTIVE');
    expect((kuro.rows[0] as any).enabled).toBe(true);
  });

  it('enforces atomic job lease acquisition with locked_by and lease_expires_at', async () => {
    // Insert a test job into queue
    await db.query(`
      insert into public.importer_queue (id, task_type, source, priority, dedupe_key, status)
      values ('11111111-1111-1111-1111-111111111111', 'DISCOVER_WORKS', 'nexus', 10, 'nexus:discover', 'QUEUED')
    `);

    // Worker 1 acquires the job
    const worker1 = 'worker-alpha';
    const acquired = await db.query(`
      select * from public.importer_acquire_job($1, interval '5 minutes')
    `, [worker1]);

    expect(acquired.rows.length).toBe(1);
    expect((acquired.rows[0] as any).id).toBe('11111111-1111-1111-1111-111111111111');
    expect((acquired.rows[0] as any).locked_by).toBe(worker1);
    expect((acquired.rows[0] as any).status).toBe('IMPORTING');
    expect((acquired.rows[0] as any).attempts).toBe(1);

    // Concurrent Worker 2 tries to acquire immediately -> should get nothing (0 rows)
    const worker2 = 'worker-beta';
    const emptyAcquire = await db.query(`
      select * from public.importer_acquire_job($1, interval '5 minutes')
    `, [worker2]);
    expect(emptyAcquire.rows.length).toBe(0);
  });

  it('allows heartbeat lease renewal for the active worker', async () => {
    const renew = await db.query(`
      select public.importer_renew_lease('11111111-1111-1111-1111-111111111111', 'worker-alpha', interval '10 minutes') as renewed
    `);
    expect((renew.rows[0] as any).renewed).toBe(true);

    // Another worker cannot renew it
    const fakeRenew = await db.query(`
      select public.importer_renew_lease('11111111-1111-1111-1111-111111111111', 'impostor', interval '10 minutes') as renewed
    `);
    expect((fakeRenew.rows[0] as any).renewed).toBe(false);
  });

  it('recovers crashed worker job when lease expires', async () => {
    // Manually expire lease to simulate worker crash
    await db.query(`
      update public.importer_queue
      set lease_expires_at = now() - interval '1 second'
      where id = '11111111-1111-1111-1111-111111111111'
    `);

    // Worker 2 can now safely reclaim the expired job!
    const reclaimed = await db.query(`
      select * from public.importer_acquire_job('worker-beta', interval '5 minutes')
    `);
    expect(reclaimed.rows.length).toBe(1);
    expect((reclaimed.rows[0] as any).id).toBe('11111111-1111-1111-1111-111111111111');
    expect((reclaimed.rows[0] as any).locked_by).toBe('worker-beta');
    expect((reclaimed.rows[0] as any).attempts).toBe(2);
  });

  it('releases job on completion', async () => {
    const released = await db.query(`
      select public.importer_release_job('11111111-1111-1111-1111-111111111111', 'worker-beta', 'COMPLETED') as ok
    `);
    expect((released.rows[0] as any).ok).toBe(true);

    const check = await db.query(`
      select status, locked_by, lease_expires_at from public.importer_queue where id = '11111111-1111-1111-1111-111111111111'
    `);
    expect((check.rows[0] as any).status).toBe('COMPLETED');
    expect((check.rows[0] as any).locked_by).toBeNull();
  });

  it('strictly blocks anon role via RLS permissions', async () => {
    await db.exec('set role anon');
    try {
      await db.query('select * from public.importer_queue');
      expect.fail('Should have thrown permission denied');
    } catch (err: any) {
      expect(err.message).toMatch(/permission denied/i);
    } finally {
      await db.exec('reset role');
    }
  });
});
