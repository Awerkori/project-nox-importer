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

    // Minimal mock wrapping PGlite to emulate Supabase client for DeduplicationEngine
    supabaseMock = {
      from: (table: string) => {
        const filters: Array<{ sql: string; params: any[] }> = [];
        const builder: any = {
          select: () => builder,
          eq: (col: string, val: any) => {
            filters.push({ sql: `${col} = $PARAM`, params: [val] });
            return builder;
          },
          ilike: (col: string, val: any) => {
            filters.push({ sql: `${col} ilike $PARAM`, params: [val] });
            return builder;
          },
          in: (col: string, vals: any[]) => {
            if (!vals || vals.length === 0) {
              filters.push({ sql: `1 = 0`, params: [] });
            } else {
              filters.push({ sql: `${col} = ANY($PARAM)`, params: [vals] });
            }
            return builder;
          },
          overlaps: (col: string, vals: any[]) => {
            if (!vals || vals.length === 0) {
              filters.push({ sql: `1 = 0`, params: [] });
            } else {
              filters.push({ sql: `${col} && $PARAM`, params: [vals] });
            }
            return builder;
          },
          not: (col: string, op: string, val: any) => {
            if (op === 'is' && val === null) {
              filters.push({ sql: `${col} IS NOT NULL`, params: [] });
            } else {
              filters.push({ sql: `${col} != $PARAM`, params: [val] });
            }
            return builder;
          },
          maybeSingle: async () => {
            const res = await builder._execute();
            return { data: res.data?.[0] || null, error: res.error };
          },
          single: async () => {
            const res = await builder._execute();
            return { data: res.data?.[0] || null, error: res.error };
          },
          _execute: async () => {
            let whereClause = '';
            const allParams: any[] = [];
            if (filters.length > 0) {
              const clauses = filters.map(f => {
                let s = f.sql;
                for (const p of f.params) {
                  allParams.push(p);
                  s = s.replace('$PARAM', `$${allParams.length}`);
                }
                return s;
              });
              whereClause = ' WHERE ' + clauses.join(' AND ');
            }
            try {
              const res = await db.query(`SELECT * FROM public.${table}${whereClause}`, allParams);
              return { data: res.rows || [], error: null };
            } catch (err: any) {
              return { data: null, error: err };
            }
          },
          then: (resolve: any, reject?: any) => {
            builder._execute().then(resolve, reject);
          }
        };
        return {
          select: () => builder,
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
      };
    },
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

  it('matches canonical work via alias and binds cleanly without creating duplicates (Demon King)', async () => {
    // 1. Canonical work exists with title "Imperador Demoníaco" and alias "The Servant Is the Demon King?!"
    const canonicalWork = await db.query(`
      insert into public.works (title, slug, aliases, kind, status)
      values ('Imperador Demoníaco', 'imperador-demoniaco', ARRAY['The Servant Is the Demon King?!', 'Demonic Emperor'], 'MANHWA', 'ONGOING')
      returning id
    `);
    const canonicalId = (canonicalWork.rows[0] as any).id;

    // 2. Incoming work from mangaflix with title "The Servant Is the Demon King?!"
    const res = await engine.resolveWork({
      source: 'mangaflix',
      sourceWorkId: 'mf-demon-king-1',
      title: 'The Servant Is the Demon King?!',
      slug: 'the-servant-is-the-demon-king',
      kind: 'MANHWA',
    });

    // 3. Must match canonical work, NOT create duplicate
    expect(res.status).toBe('EXISTING_MAPPING');
    expect(res.workId).toBe(canonicalId);

    // 4. Verify ZERO new duplicate works were created in works table
    const dupCheck = await db.query('select * from public.works where slug = $1', ['the-servant-is-the-demon-king']);
    expect(dupCheck.rows.length).toBe(0);

    // 5. Verify mapping is bound to canonical work
    const mapCheck = await db.query('select * from public.importer_work_mappings where source = $1 and source_work_id = $2', ['mangaflix', 'mf-demon-king-1']);
    expect((mapCheck.rows[0] as any).work_id).toBe(canonicalId);
    expect((mapCheck.rows[0] as any).sync_status).toBe('SYNCED');
  });

  it('rejects match between a Novel and a Comic', async () => {
    const novelRes = await db.query(`
      insert into public.works (title, slug, kind, status)
      values ('Second Life Ranker (Novel)', 'second-life-ranker-novel', 'MANHWA', 'ONGOING')
      returning id
    `);
    const novelId = (novelRes.rows[0] as any).id;

    const res = await engine.resolveWork({
      source: 'kuro',
      sourceWorkId: 'kuro-slr-comic',
      title: 'Second Life Ranker',
      slug: 'second-life-ranker',
      kind: 'MANHWA',
    });

    // Must NOT bind to the Novel!
    expect(res.workId).not.toBe(novelId);
  });

  it('rejects match between different seasons', async () => {
    const s1Res = await db.query(`
      insert into public.works (title, slug, kind, status)
      values ('Tower of God Season 1', 'tower-of-god-season-1', 'MANHWA', 'COMPLETED')
      returning id
    `);
    const s1Id = (s1Res.rows[0] as any).id;

    const res = await engine.resolveWork({
      source: 'kuro',
      sourceWorkId: 'kuro-tog-s2',
      title: 'Tower of God Season 2',
      slug: 'tower-of-god-season-2',
      kind: 'MANHWA',
    });

    // Must NOT bind to Season 1!
    expect(res.workId).not.toBe(s1Id);
  });

  it('rejects match between main story and spin-off', async () => {
    const mainRes = await db.query(`
      insert into public.works (title, slug, kind, status)
      values ('Omniscient Reader', 'omniscient-reader', 'MANHWA', 'ONGOING')
      returning id
    `);
    const mainId = (mainRes.rows[0] as any).id;

    const res = await engine.resolveWork({
      source: 'kuro',
      sourceWorkId: 'kuro-orv-side',
      title: 'Omniscient Reader Side Story',
      slug: 'omniscient-reader-side-story',
      kind: 'MANHWA',
    });

    // Must NOT bind to the main story!
    expect(res.workId).not.toBe(mainId);
  });
});
