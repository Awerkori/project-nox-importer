import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { ImporterQueue } from '../src/core/queue.js';

describe('Deterministic Chapter Ordering', () => {
  let db: PGlite;
  let supabaseMock: any;
  let queue: ImporterQueue;

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
        .replace('create extension if not exists pgcrypto;', '')
        .replace(/create\s+index\s+concurrently/gi, 'create index');
      await db.exec(sql);
    }
    await db.exec(readFileSync(resolve('migrations/001_importer_schema.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/002_importer_sources_status.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/003_importer_sort_key_and_concurrency.sql'), 'utf8'));

    supabaseMock = {
      from: (table: string) => ({
        insert: (row: any) => ({
          then: (resolve: any) => {
            const keys = Object.keys(row);
            const vals = Object.values(row);
            const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
            db.query(`insert into public.${table} (${keys.join(', ')}) values (${placeholders}) returning *`, vals)
              .then((r) => resolve({ data: r.rows[0], error: null }))
              .catch((err) => resolve({ data: null, error: err }));
          },
        }),
      }),
      rpc: async (funcName: string, args: any) => {
        if (funcName === 'importer_acquire_job') {
          const res = await db.query(
            `select * from public.importer_acquire_job($1, $2::interval, $3)`,
            [args.p_worker_id, args.p_lease_duration || '5 minutes', args.p_source || null]
          );
          return { data: res.rows, error: null };
        }
        return { data: null, error: null };
      },
    };

    queue = new ImporterQueue(supabaseMock, 'test-ordering-worker');
  });

  afterAll(async () => {
    await db.close();
  });

  it('acquires chapters strictly ascending (1 -> 2 -> 3 ... -> 100) regardless of insertion order', async () => {
    // Deliberately insert chapters out-of-order: 100 first, then 50, then 1, then 2, then 1.5 (prologue/special)
    const testChapters = [
      { num: 100, sortKey: 100 },
      { num: 50, sortKey: 50 },
      { num: 1, sortKey: 1 },
      { num: 2, sortKey: 2 },
      { num: 1.5, sortKey: 1.5 },
      { num: 0, sortKey: 0 },
    ];

    for (const ch of testChapters) {
      await queue.enqueue(
        'IMPORT_CHAPTER',
        'nexus',
        `nexus:test:${ch.num}`,
        { chapterNumber: ch.num },
        30,
        ch.sortKey
      );
    }

    // Now acquire each job one by one and assert strictly ascending order: 0 -> 1 -> 1.5 -> 2 -> 50 -> 100
    const expectedSequence = [0, 1, 1.5, 2, 50, 100];
    const acquiredSequence: number[] = [];

    for (let i = 0; i < expectedSequence.length; i++) {
      const job = await queue.acquireNextJob(5, 'nexus');
      expect(job).not.toBeNull();
      acquiredSequence.push(job!.payload.chapterNumber);
    }

    expect(acquiredSequence).toEqual(expectedSequence);
  });
});
