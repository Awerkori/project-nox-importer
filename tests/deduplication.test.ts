import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { createClient } from '@supabase/supabase-js';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { DeduplicationEngine } from '../src/core/deduplication.js';

describe('DeduplicationEngine', () => {
  let db: PGlite;
  let supabaseMock: any;
  let engine: DeduplicationEngine;

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
    await db.exec(readFileSync(resolve('migrations/002_importer_sources_status.sql'), 'utf8'));

    // Minimal mock wrapping PGlite to emulate Supabase client for DeduplicationEngine
    supabaseMock = {
      from: (table: string) => ({
        select: (...cols: any[]) => ({
          eq: (col: string, val: any) => ({
            eq: (col2: string, val2: any) => ({
              maybeSingle: async () => {
                const res = await db.query(`select * from public.${table} where ${col} = $1 and ${col2} = $2 limit 1`, [val, val2]);
                return { data: res.rows[0] || null, error: null };
              },
            }),
            maybeSingle: async () => {
              const res = await db.query(`select * from public.${table} where ${col} = $1 limit 1`, [val]);
              return { data: res.rows[0] || null, error: null };
            },
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
            then: (resolve: any) => {
              const keys = Object.keys(row);
              const vals = Object.values(row);
              const setClause = keys.map((k, i) => `${k} = $${i + 1}`).join(', ');
              db.query(`update public.${table} set ${setClause} where ${col} = $${keys.length + 1} returning *`, [...vals, val])
                .then(r => resolve({ data: r.rows[0], error: null }))
                .catch(err => resolve({ data: null, error: err }));
            }
          })
        }),
        upsert: (row: any, opts?: any) => ({
          select: () => ({
            single: async () => {
              const res = await db.query(`
                insert into public.${table} (source, source_work_id, work_id, source_slug, source_title, sync_status, metadata, last_synced_at)
                values ($1, $2, $3, $4, $5, $6, $7, $8)
                on conflict (source, source_work_id) do update set
                  work_id = excluded.work_id,
                  sync_status = excluded.sync_status,
                  metadata = excluded.metadata,
                  last_synced_at = excluded.last_synced_at
                returning *
              `, [row.source, row.source_work_id, row.work_id, row.source_slug, row.source_title, row.sync_status, JSON.stringify(row.metadata || {}), row.last_synced_at]);
              return { data: res.rows[0], error: null };
            }
          })
        })
      })
    };

    engine = new DeduplicationEngine(supabaseMock);
  });

  afterAll(async () => {
    await db.close();
  });

  it('registers brand new work cleanly when no match exists', async () => {
    const res = await engine.resolveWork({
      source: 'nexus',
      sourceWorkId: 'nx-work-100',
      title: 'Solo Hero Legend',
      slug: 'solo-hero-legend',
      synopsis: 'A great journey',
      kind: 'MANHWA',
    });

    expect(res.status).toBe('NEW_WORK');
    expect(res.workId).toBeTruthy();
    expect(res.slug).toBe('solo-hero-legend');

    // Confirm work row in public.works
    const dbWork = await db.query('select * from public.works where id = $1', [res.workId]);
    expect(dbWork.rows.length).toBe(1);
    expect((dbWork.rows[0] as any).title).toBe('Solo Hero Legend');
    expect((dbWork.rows[0] as any).published).toBe(false); // Unreleased draft until verified
  });

  it('re-uses existing mapping for the same source and source_work_id', async () => {
    const res = await engine.resolveWork({
      source: 'nexus',
      sourceWorkId: 'nx-work-100',
      title: 'Solo Hero Legend (Updated)',
      slug: 'solo-hero-legend',
    });

    expect(res.status).toBe('EXISTING_MAPPING');
    expect(res.workId).toBeTruthy();
  });

  it('flags as AMBIGUOUS when a different source work collides with an already claimed work', async () => {
    // Attempt to register another source work with the exact same title/slug
    const res = await engine.resolveWork({
      source: 'nexus',
      sourceWorkId: 'nx-work-200-impostor',
      title: 'Solo Hero Legend',
      slug: 'solo-hero-legend',
    });

    expect(res.status).toBe('AMBIGUOUS');
    expect(res.workId).toBeNull();

    // Confirm mapping marked as AMBIGUOUS in database
    const map = await db.query('select * from public.importer_work_mappings where source_work_id = $1', ['nx-work-200-impostor']);
    expect((map.rows[0] as any).sync_status).toBe('AMBIGUOUS');
  });
});
