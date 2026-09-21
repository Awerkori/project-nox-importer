import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { matchWorkCandidate } from '../src/core/matching.js';
import { DeduplicationEngine, CandidateWork } from '../src/core/deduplication.js';

describe('Canonical Gate Regression Suite (Section 35 Mandatory Cases)', () => {
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

    // Setup supabaseMock for DeduplicationEngine
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
            select: () => ({
              single: async () => {
                const keys = Object.keys(row);
                const vals = Object.values(row);
                const cols = keys.join(', ');
                const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
                const r = await db.query(`insert into public.${table} (${cols}) values (${placeholders}) returning *`, vals);
                return { data: r.rows[0], error: null };
              }
            }),
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
            }),
            then: (resolve: any) => {
              db.query(`
                insert into public.${table} (source, source_work_id, work_id, source_slug, source_title, sync_status, metadata, last_synced_at)
                values ($1, $2, $3, $4, $5, $6, $7, $8)
                on conflict (source, source_work_id) do update set
                  work_id = excluded.work_id,
                  sync_status = excluded.sync_status,
                  metadata = excluded.metadata,
                  last_synced_at = excluded.last_synced_at
                returning *
              `, [row.source, row.source_work_id, row.work_id, row.source_slug, row.source_title, row.sync_status, JSON.stringify(row.metadata || {}), row.last_synced_at])
                .then(r => resolve({ data: r.rows[0], error: null }))
                .catch(err => resolve({ data: null, error: err }));
            }
          })
        };
      }
    };

    engine = new DeduplicationEngine(supabaseMock);
  });

  afterAll(async () => {
    await db?.close();
  });

  it('CASE 1: "Imperador Mágico" vs "Imperador Demoníaco" -> SAME CANONICAL', () => {
    const target = {
      title: 'Imperador Mágico',
      slug: 'imperador-magico',
      aliases: ['Magic Emperor'],
      kind: 'MANHUA'
    };

    const candidate = {
      title: 'Imperador Demoníaco',
      slug: 'imperador-demoniaco',
      aliases: ['Demonic Emperor'],
      kind: 'MANHUA'
    };

    const res = matchWorkCandidate(target, candidate);
    expect(res.matched).toBe(true);
    expect(res.confidenceScore).toBeGreaterThanOrEqual(0.85);
    expect(res.matchMethod).toBe('ALIAS_EXACT');
  });

  it('CASE 2: mesma obra, source diferente, capa diferente -> SAME CANONICAL', () => {
    const target = {
      title: 'Solo Leveling',
      slug: 'solo-leveling',
      aliases: ['Na Honjaman Rebeleob'],
      kind: 'MANHWA'
    };

    const candidate = {
      title: 'Only I Level Up',
      slug: 'only-i-level-up',
      aliases: [],
      kind: 'MANHWA'
    };

    const res = matchWorkCandidate(target, candidate);
    expect(res.matched).toBe(true);
    expect(res.confidenceScore).toBeGreaterThanOrEqual(0.85);
  });

  it('CASE 3: mesma obra, título alternativo conhecido -> SAME CANONICAL', () => {
    const target = {
      title: 'The Beginning After the End',
      slug: 'the-beginning-after-the-end',
      aliases: ['TBATE', 'O Começo Depois do Fim'],
      kind: 'WEBTOON'
    };

    const candidate = {
      title: 'O Começo Depois do Fim',
      slug: 'o-comeco-depois-do-fim',
      aliases: [],
      kind: 'WEBTOON'
    };

    const res = matchWorkCandidate(target, candidate);
    expect(res.matched).toBe(true);
    expect(res.confidenceScore).toBeGreaterThanOrEqual(0.85);
    expect(res.matchMethod).toBe('ALIAS_EXACT');
  });

  it('CASE 4: nomes parecidos porém obras realmente diferentes -> DO NOT MERGE', () => {
    const target = {
      title: 'Tower of God',
      slug: 'tower-of-god',
      aliases: [],
      kind: 'MANHWA'
    };

    const candidate = {
      title: 'Tower of God Season 2',
      slug: 'tower-of-god-season-2',
      aliases: [],
      kind: 'MANHWA'
    };

    const res = matchWorkCandidate(target, candidate);
    expect(res.matched).toBe(false);
    expect(res.confidenceScore).toBe(0.0);
    expect(res.reason).toContain('Season mismatch');

    // Novel vs Comic check
    const comic = { title: 'Second Life Ranker', kind: 'MANHWA' };
    const novel = { title: 'Second Life Ranker (Novel)', kind: 'NOVEL' };
    const formatRes = matchWorkCandidate(comic, novel);
    expect(formatRes.matched).toBe(false);
    expect(formatRes.reason).toContain('Novel vs Comic');
  });

  it('CASE 5: match ambíguo -> DO NOT CREATE AUTOMATICALLY (flagged for review)', async () => {
    // 1. Create a work via nexus
    const cand1: CandidateWork = {
      source: 'nexus',
      sourceWorkId: 'nx-work-100',
      title: 'Martial Peak',
      slug: 'martial-peak'
    };
    const res1 = await engine.resolveWork(cand1);
    expect(res1.status).toBe('NEW_WORK');

    // 2. Same source attempts to register another ID for the same work -> Conflict/Ambiguity!
    const cand2: CandidateWork = {
      source: 'nexus',
      sourceWorkId: 'nx-work-100-impostor',
      title: 'Martial Peak',
      slug: 'martial-peak'
    };

    const res2 = await engine.resolveWork(cand2);
    expect(res2.status).toBe('AMBIGUOUS');
    expect(res2.workId).toBeNull();

    // Verify in DB that ZERO new works were created for cand2
    const worksCount = await db.query('SELECT count(*)::int as c FROM works WHERE title = $1', ['Martial Peak']);
    expect(worksCount.rows[0].c).toBe(1);
  });

  it('CASE 6: duas sources descobrindo simultaneamente mesma obra -> ONLY ONE CANONICAL WORK CREATED', async () => {
    const initialWorksCount = (await db.query('SELECT count(*)::int as c FROM works')).rows[0].c;

    // Source A registers Nano Machine
    const resA = await engine.resolveWork({
      source: 'mangalivreto',
      sourceWorkId: 'nano-machine-src-a',
      title: 'Nano Machine',
      slug: 'nano-machine'
    });
    expect(resA.status).toBe('NEW_WORK');

    // Source B arrives immediately after with the same work
    const resB = await engine.resolveWork({
      source: 'montetai',
      sourceWorkId: 'nano-machine-src-b',
      title: 'Nano Machine',
      slug: 'nano-machine'
    });
    expect(resB.status).toBe('EXISTING_MAPPING');
    expect(resB.workId).toBe(resA.workId);

    // EXACTLY ONE canonical work created!
    const finalWorksCount = (await db.query('SELECT count(*)::int as c FROM works')).rows[0].c;
    expect(finalWorksCount).toBe(initialWorksCount + 1);
  });
});
