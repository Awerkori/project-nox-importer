import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { DeduplicationEngine, ADULT_SOURCES } from '../src/core/deduplication.js';

describe('Adult Classification & Monotonicity Guarantee', () => {
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
    await db.exec(readFileSync(resolve('migrations/003_importer_sort_key_and_concurrency.sql'), 'utf8'));

    // Insert sources
    await db.exec(`
      insert into public.importer_sources (id, name, base_url, enabled, status) values
        ('mangaflix', 'MangaFlix', 'https://mangaflix.org', true, 'ACTIVE'),
        ('kuro', 'Kuro', 'https://kuro.moe', true, 'ACTIVE'),
        ('acervohentai', 'Acervo Hentai', 'https://acervohentai.com', true, 'ACTIVE'),
        ('blackoutcomics', 'Blackout Comics', 'https://blackoutcomics.com', true, 'ACTIVE'),
        ('hanamiheaven', 'Hanami Heaven', 'https://hanamiheaven.com', true, 'ACTIVE'),
        ('hipercool', 'HipercooL', 'https://lerhentais.com', true, 'ACTIVE'),
        ('inkapk', 'Inkapk', 'https://inkapk.com', true, 'ACTIVE'),
        ('instahentai', 'InstaHentai', 'https://instahentai.com', true, 'ACTIVE'),
        ('megahentai', 'MegaHentai', 'https://megahentai.com', true, 'ACTIVE'),
        ('tiamanhwa', 'Tia Manhwa', 'https://tiamanhwa.com', true, 'ACTIVE')
      on conflict (id) do nothing;
    `);

    // Insert canonical tags
    await db.exec(`
      insert into public.tags (id, name, slug, kind) values
        (gen_random_uuid(), '+18', '18', 'TAG'),
        (gen_random_uuid(), 'Adulto', 'adulto', 'GENRE'),
        (gen_random_uuid(), 'Adulto (+18)', 'adulto-18', 'GENRE'),
        (gen_random_uuid(), 'Pornhwa', 'pornhwa', 'TAG'),
        (gen_random_uuid(), 'Romance', 'romance', 'GENRE')
      on conflict do nothing;
    `);

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
        upsert: (rows: any, opts?: any) => ({
          select: () => ({
            single: async () => {
              const row = Array.isArray(rows) ? rows[0] : rows;
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
          then: async (resolve: any) => {
            const arr = Array.isArray(rows) ? rows : [rows];
            for (const r of arr) {
              const keys = Object.keys(r);
              const vals = Object.values(r);
              const cols = keys.join(', ');
              const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
              await db.query(`insert into public.${table} (${cols}) values (${placeholders}) on conflict do nothing`, vals);
            }
            resolve({ data: arr, error: null });
          }
        })
      })
    };

    engine = new DeduplicationEngine(supabaseMock);
  });

  afterAll(async () => {
    await db.close();
  });

  it('recognizes all 8 adult source identifiers in ADULT_SOURCES', () => {
    const required = [
      'acervohentai',
      'blackoutcomics',
      'hanamiheaven',
      'hipercool',
      'inkapk',
      'instahentai',
      'megahentai',
      'tiamanhwa',
    ];
    for (const src of required) {
      expect(ADULT_SOURCES.has(src)).toBe(true);
    }
  });

  it('automatically classifies works from adult sources as ADULT_18 with age_rating >= 18 and provenance', async () => {
    const res = await engine.resolveWork({
      source: 'blackoutcomics',
      sourceWorkId: 'bc-101',
      title: 'Secret Stepmother Story',
      slug: 'secret-stepmother-story',
      synopsis: 'A spicy drama',
      kind: 'MANHWA',
      genres: ['Romance', 'Drama'],
    });

    expect(res.status).toBe('NEW_WORK');
    expect(res.workId).toBeTruthy();

    const dbWork = await db.query('select * from public.works where id = $1', [res.workId]);
    const work = dbWork.rows[0] as any;
    expect(work.content_rating).toBe('ADULT_18');
    expect(work.age_rating).toBe(18);
    expect(work.kind).toBe('MANHWA');
    expect(work.metadata_provenance.adult_source).toBeDefined();
    expect(work.metadata_provenance.adult_source.source).toBe('blackoutcomics');

    const dbMapping = await db.query('select * from public.importer_work_mappings where work_id = $1', [res.workId]);
    const mapping = dbMapping.rows[0] as any;
    expect(mapping.metadata.adult_source).toBe(true);
    expect(mapping.metadata.adult_source_id).toBe('blackoutcomics');

    // Confirm canonical tags attached
    const dbWorkTags = await db.query(`
      select t.slug, t.name from public.work_tags wt
      join public.tags t on wt.tag_id = t.id
      where wt.work_id = $1
    `, [res.workId]);
    const tagSlugs = dbWorkTags.rows.map((r: any) => r.slug);
    expect(tagSlugs).toContain('18');
    expect(tagSlugs).toContain('adulto');
    expect(tagSlugs).toContain('adulto-18');
    expect(tagSlugs).toContain('pornhwa'); // Because kind is MANHWA
  });

  it('monotonically preserves ADULT_18 when a non-adult source later updates or matches the work', async () => {
    // 1. Create an adult work from tiamanhwa
    const res = await engine.resolveWork({
      source: 'tiamanhwa',
      sourceWorkId: 'tia-200',
      title: 'Campus Queen Adult Affair',
      slug: 'campus-queen-adult-affair',
      synopsis: 'Adult drama on campus',
      kind: 'MANHWA',
    });
    const workId = res.workId!;

    // 2. Non-adult source (Kuro) tries to update metadata with GENERAL content rating
    await engine.applyMetadataPrecedence(workId, {
      source: 'kuro',
      sourceWorkId: 'kuro-999',
      title: 'Campus Queen Adult Affair (Kuro HD)',
      slug: 'campus-queen-adult-affair',
      ageRating: 12, // Lower age rating
      contentRating: 'GENERAL', // Trying to downgrade
    }, 'kuro');

    // 3. Confirm work did NOT downgrade!
    const dbWork = await db.query('select * from public.works where id = $1', [workId]);
    const work = dbWork.rows[0] as any;
    expect(work.content_rating).toBe('ADULT_18'); // Monotonically preserved!
    expect(work.age_rating).toBe(18); // Cannot downgrade below 18!
    expect(work.title).toBe('Campus Queen Adult Affair (Kuro HD)'); // Kuro still upgraded valid non-rating field
  });

  it('promotes GENERAL work to ADULT_18 when an adult source later matches it', async () => {
    // 1. Create general work from MangaFlix
    const res = await engine.resolveWork({
      source: 'mangaflix',
      sourceWorkId: 'mf-300',
      title: 'Hidden Romance',
      slug: 'hidden-romance',
      kind: 'MANGA',
      ageRating: 12,
    });
    const workId = res.workId!;

    const initialDb = await db.query('select * from public.works where id = $1', [workId]);
    expect((initialDb.rows[0] as any).content_rating).toBe('GENERAL');

    // 2. Adult source (Acervo Hentai) matches it
    await engine.applyMetadataPrecedence(workId, {
      source: 'acervohentai',
      sourceWorkId: 'ah-300',
      title: 'Hidden Romance',
      slug: 'hidden-romance',
    }, 'acervohentai');

    // 3. Confirm promoted to ADULT_18
    const updatedDb = await db.query('select * from public.works where id = $1', [workId]);
    const updated = updatedDb.rows[0] as any;
    expect(updated.content_rating).toBe('ADULT_18');
    expect(updated.age_rating).toBe(18);
    expect(updated.metadata_provenance.adult_source.source).toBe('acervohentai');
  });

  it('preserves kind (MANGA, WEBTOON, MANHWA) correctly', async () => {
    // Manga adult work (e.g. from MegaHentai)
    const resManga = await engine.resolveWork({
      source: 'megahentai',
      sourceWorkId: 'mh-manga-1',
      title: 'Adult Doujin Manga',
      slug: 'adult-doujin-manga',
      kind: 'MANGA',
    });
    const workManga = (await db.query('select * from public.works where id = $1', [resManga.workId])).rows[0] as any;
    expect(workManga.kind).toBe('MANGA');
    expect(workManga.content_rating).toBe('ADULT_18');

    // Webtoon adult work
    const resWebtoon = await engine.resolveWork({
      source: 'instahentai',
      sourceWorkId: 'ih-wt-1',
      title: 'Adult Webtoon Series',
      slug: 'adult-webtoon-series',
      kind: 'WEBTOON',
    });
    const workWebtoon = (await db.query('select * from public.works where id = $1', [resWebtoon.workId])).rows[0] as any;
    expect(workWebtoon.kind).toBe('WEBTOON');
    expect(workWebtoon.content_rating).toBe('ADULT_18');
  });
});
