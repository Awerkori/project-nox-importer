import { describe, it, expect, beforeAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { DeduplicationEngine } from '../src/core/deduplication.js';

describe('Metadata Precedence & Field-Level Provenance', () => {
  let db: PGlite;
  let supabaseMock: any;
  let deduplication: DeduplicationEngine;

  const adminUserId = '10000000-0000-4000-8000-000000000001';
  const editorUserId = '20000000-0000-4000-8000-000000000002';
  const memberUserId = '30000000-0000-4000-8000-000000000003';

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
    const files = readdirSync(mangaMigrationsDir).filter((f) => f.endsWith('.sql')).sort();
    for (const f of files) {
      const sql = readFileSync(resolve(mangaMigrationsDir, f), 'utf8').replace('create extension if not exists pgcrypto;', '')
        .replace(/create\s+index\s+concurrently/gi, 'create index');
      await db.exec(sql);
    }
    await db.exec(readFileSync(resolve('migrations/001_importer_schema.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/002_importer_sources_status.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/003_importer_sort_key_and_concurrency.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/004_importer_telemetry_and_provenance.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/005_importer_page_provider_column.sql'), 'utf8'));

    // Create users with distinct roles
    await db.query(`insert into auth.users (id, email, email_confirmed_at) values ($1, 'admin@projectnox.com', now())`, [adminUserId]);
    await db.query(`insert into public.access_roles (user_id, role) values ($1, 'ADMIN') on conflict (user_id) do update set role = 'ADMIN'`, [adminUserId]);

    await db.query(`insert into auth.users (id, email, email_confirmed_at) values ($1, 'editor@projectnox.com', now())`, [editorUserId]);
    await db.query(`insert into public.access_roles (user_id, role) values ($1, 'EDITOR') on conflict (user_id) do update set role = 'EDITOR'`, [editorUserId]);

    await db.query(`insert into auth.users (id, email, email_confirmed_at) values ($1, 'member@projectnox.com', now())`, [memberUserId]);
    await db.query(`insert into public.access_roles (user_id, role) values ($1, 'USER') on conflict (user_id) do update set role = 'USER'`, [memberUserId]);

    // Construct mock Supabase client wrapping PGlite
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
          then: async (resolve: any) => {
            const keys = Object.keys(row);
            const vals = Object.values(row);
            const placeholders = keys.map((_, i) => `$${i + 1}`).join(',');
            try {
              await db.query(`insert into public.${table} (${keys.join(',')}) values (${placeholders})`, vals);
              resolve({ error: null });
            } catch (err) {
              resolve({ error: err });
            }
          },
        }),
        update: (updates: any) => ({
          eq: (col: string, val: any) => ({
            then: async (resolve: any) => {
              const keys = Object.keys(updates);
              const vals = Object.values(updates);
              const setClause = keys.map((k, i) => `${k} = $${i + 1}`).join(',');
              try {
                await db.query(`update public.${table} set ${setClause} where ${col} = $${keys.length + 1}`, [...vals, val]);
                resolve({ error: null });
              } catch (err) {
                resolve({ error: err });
              }
            },
          }),
        }),
        upsert: (row: any, opts?: any) => {
          const runUpsert = async () => {
            const keys = Object.keys(row);
            const vals = Object.values(row);
            const placeholders = keys.map((_, i) => `$${i + 1}`).join(',');
            const conflictCols = opts?.onConflict || 'id';
            const updateClause = keys
              .filter((k) => !conflictCols.split(',').includes(k))
              .map((k) => `${k} = excluded.${k}`)
              .join(',');
            const sql = `
              insert into public.${table} (${keys.join(',')}) values (${placeholders})
              on conflict (${conflictCols}) do update set ${updateClause}
              returning *
            `;
            const res = await db.query(sql, vals);
            return { data: res.rows[0], error: null };
          };
          return {
            select: () => ({
              single: runUpsert,
            }),
            then: (resolve: any) => runUpsert().then(resolve),
          };
        },
      };
    },
  };

    deduplication = new DeduplicationEngine(supabaseMock);
  });

  it('initializes metadata_provenance correctly on new work creation by automated source', async () => {
    const res = await deduplication.resolveWork({
      source: 'nexus',
      sourceWorkId: 'nx-work-1',
      title: 'Original Nexus Title',
      slug: 'original-nexus-title',
      synopsis: 'Initial synopsis from nexus.',
      author: 'Nexus Author',
      artist: 'Nexus Artist',
      kind: 'MANHWA',
      status: 'ONGOING',
    });

    expect(res.status).toBe('NEW_WORK');
    expect(res.workId).toBeTruthy();

    const dbWork = await db.query('select * from public.works where id = $1', [res.workId]);
    const prov = (dbWork.rows[0] as any).metadata_provenance;
    expect(prov.title.source).toBe('nexus');
    expect(prov.synopsis.source).toBe('nexus');
    expect(prov.author.source).toBe('nexus');
  });

  it('allows Kuro to upgrade non-manual fields of an existing work created by another source', async () => {
    // 1. Kuro processes the same work
    const res = await deduplication.resolveWork({
      source: 'kuro',
      sourceWorkId: 'kuro-work-1',
      title: 'Original Nexus Title',
      slug: 'original-nexus-title',
      synopsis: 'Superior comprehensive synopsis provided by Kuro editorial team.',
      author: 'Kuro Verified Author',
    });

    expect(res.status).toBe('EXISTING_MAPPING');

    const dbWork = await db.query('select * from public.works where slug = $1', ['original-nexus-title']);
    const work = dbWork.rows[0] as any;
    const prov = work.metadata_provenance;

    // Kuro upgraded the synopsis and author
    expect(work.synopsis).toBe('Superior comprehensive synopsis provided by Kuro editorial team.');
    expect(prov.synopsis.source).toBe('kuro');
    expect(work.author).toBe('Kuro Verified Author');
    expect(prov.author.source).toBe('kuro');
    // Artist was not in Kuro candidate, so Nexus artist is preserved
    expect(work.artist).toBe('Nexus Artist');
    expect(prov.artist.source).toBe('nexus');
  });

  it('prevents another automated source (e.g. MangaFlix) from overwriting fields populated by Kuro', async () => {
    const res = await deduplication.resolveWork({
      source: 'mangaflix',
      sourceWorkId: 'mf-work-1',
      title: 'Original Nexus Title',
      slug: 'original-nexus-title',
      synopsis: 'Inferior synopsis from mangaflix that should be ignored.',
      author: 'Mangaflix Wrong Author',
    });

    expect(res.status).toBe('EXISTING_MAPPING');

    const dbWork = await db.query('select * from public.works where slug = $1', ['original-nexus-title']);
    const work = dbWork.rows[0] as any;
    const prov = work.metadata_provenance;

    // Kuro's values remain intact
    expect(work.synopsis).toBe('Superior comprehensive synopsis provided by Kuro editorial team.');
    expect(prov.synopsis.source).toBe('kuro');
    expect(work.author).toBe('Kuro Verified Author');
    expect(prov.author.source).toBe('kuro');
  });

  it('never replaces an existing populated field with null or empty from candidate', async () => {
    await deduplication.resolveWork({
      source: 'kuro',
      sourceWorkId: 'kuro-work-1',
      title: 'Original Nexus Title',
      slug: 'original-nexus-title',
      synopsis: '', // Empty synopsis in incoming candidate
      author: undefined, // Undefined author
    });

    const dbWork = await db.query('select * from public.works where slug = $1', ['original-nexus-title']);
    const work = dbWork.rows[0] as any;

    expect(work.synopsis).toBe('Superior comprehensive synopsis provided by Kuro editorial team.');
    expect(work.author).toBe('Kuro Verified Author');
  });

  it('protects fields manually edited by Admin via trigger: title.source becomes manual and Kuro cannot overwrite it', async () => {
    const workRes = await db.query('select id from public.works where slug = $1', ['original-nexus-title']);
    const workId = (workRes.rows[0] as any).id;

    // Simulate Admin editing title through authenticated session (auth.uid = adminUserId)
    await db.exec(`set "request.jwt.claim.sub" = '${adminUserId}';`);
    await db.query(`update public.works set title = 'Admin Definitive Title' where id = $1`, [workId]);
    await db.exec(`reset "request.jwt.claim.sub";`);

    // Verify trigger marked title as manual
    const checkWork = await db.query('select * from public.works where id = $1', [workId]);
    const provAfterAdmin = (checkWork.rows[0] as any).metadata_provenance;
    expect(provAfterAdmin.title.source).toBe('manual');
    expect((checkWork.rows[0] as any).title).toBe('Admin Definitive Title');

    // Kuro attempts to update title and synopsis
    await deduplication.applyMetadataPrecedence(
      workId,
      {
        source: 'kuro',
        sourceWorkId: 'kuro-work-1',
        title: 'Kuro Attempted Overwrite Title',
        slug: 'original-nexus-title',
        synopsis: 'Even better Kuro synopsis V2.',
      },
      'kuro'
    );

    const finalWork = await db.query('select * from public.works where id = $1', [workId]);
    const finalWorkRow = finalWork.rows[0] as any;

    // Title is PROTECTED (remained Admin Definitive Title)
    expect(finalWorkRow.title).toBe('Admin Definitive Title');
    expect(finalWorkRow.metadata_provenance.title.source).toBe('manual');

    // Synopsis was NOT manually edited by Admin, so Kuro successfully upgraded it!
    expect(finalWorkRow.synopsis).toBe('Even better Kuro synopsis V2.');
    expect(finalWorkRow.metadata_provenance.synopsis.source).toBe('kuro');
  });

  it('prevents regular users (role USER) from triggering manual edit provenance', async () => {
    const workRes = await db.query('select id from public.works where slug = $1', ['original-nexus-title']);
    const workId = (workRes.rows[0] as any).id;

    // Regular member attempts update with auth.uid = memberUserId
    await db.exec(`set "request.jwt.claim.sub" = '${memberUserId}';`);
    await db.query(`update public.works set year = 2025 where id = $1`, [workId]);
    await db.exec(`reset "request.jwt.claim.sub";`);

    const checkWork = await db.query('select * from public.works where id = $1', [workId]);
    const prov = (checkWork.rows[0] as any).metadata_provenance;
    // year should NOT be marked as manual because member is not an editor
    expect(prov.year?.source).not.toBe('manual');
  });
});
