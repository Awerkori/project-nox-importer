import { describe, it, expect, beforeEach, vi } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'fs';
import { resolve } from 'path';
import { PublicationBarrier } from '../src/core/publication.js';

describe('Project Nox — Definitive Releases Rule (Casos A a G)', () => {
  let db: PGlite;
  const botUserId = '732fbe87-5040-41fb-9983-0aedb2af44c8';

  beforeEach(async () => {
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
    await db.exec(readFileSync(resolve('migrations/004_importer_telemetry_and_provenance.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/005_importer_page_provider_column.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/006_importer_publication_barrier.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/007_importer_lease_recovery.sql'), 'utf8'));
    await db.exec(`ALTER TABLE public.chapters ADD COLUMN IF NOT EXISTS is_fresh_release boolean DEFAULT false;`);
    await db.exec(readFileSync(resolve('migrations/20260921133000_fix_releases_and_triggers.sql'), 'utf8'));

    await db.query(`insert into auth.users (id, email, email_confirmed_at) values ($1, 'bot@projectnox.com', now())`, [botUserId]);
    await db.query(`insert into public.access_roles (user_id, role) values ($1, 'ADMIN') on conflict (user_id) do update set role = 'ADMIN'`, [botUserId]);
  });

  async function createTestWork(
    title: string,
    slug: string,
    kind: string = 'MANGA',
    latestPublishedAt?: string
  ) {
    const coverMediaRes = await db.query(
      `INSERT INTO public.media (provider, provider_key, mime, width, height, bytes, storage_ready, purpose, sha256, created_by)
       VALUES ('telegram', $1, 'image/jpeg', 600, 800, 15000, true, 'editorial', 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', $2) RETURNING id`,
      [`cover-${slug}`, botUserId]
    );
    const coverId = coverMediaRes.rows[0].id;

    const workRes = await db.query(
      `INSERT INTO public.works (title, slug, published, kind, cover_id, latest_chapter_published_at)
       VALUES ($1, $2, true, $3, $4, $5) RETURNING id`,
      [title, slug, kind, coverId, latestPublishedAt || null]
    );
    return workRes.rows[0].id;
  }

  async function publishTestChapter(
    workId: string,
    number: number,
    title: string = `Cap ${number}`,
    isFreshRelease: boolean = false,
    publishedAt?: string
  ) {
    const chRes = await db.query(
      `INSERT INTO public.chapters (work_id, number, title, origin, is_fresh_release)
       VALUES ($1, $2, $3, 'IMPORTER', $4) RETURNING id`,
      [workId, number, title, isFreshRelease]
    );
    const chId = chRes.rows[0].id;

    const mediaRes = await db.query(
      `INSERT INTO public.media (provider, provider_key, mime, width, height, bytes, storage_ready, purpose, sha256, created_by)
       VALUES ('telegram', $1, 'image/jpeg', 800, 1200, 15000, true, 'editorial', 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', $2) RETURNING id`,
      [`key-${chId}`, botUserId]
    );
    const mediaId = mediaRes.rows[0].id;

    await db.query(
      `INSERT INTO public.pages (chapter_id, position, media_id, width, height)
       VALUES ($1, 1, $2, 800, 1200)`,
      [chId, mediaId]
    );

    const finalPublishedAt = publishedAt || new Date().toISOString();
    await db.query(
      `UPDATE public.chapters SET published_at = $1 WHERE id = $2`,
      [finalPublishedAt, chId]
    );

    return { chId, mediaId, publishedAt: finalPublishedAt };
  }

  // Mock supabase client wrapping PGlite for PublicationBarrier
  function createMockSupabase(pg: PGlite) {
    return {
      rpc: async (fn: string, params: any) => {
        if (fn === 'importer_check_publication_barrier') {
          const res = await pg.query(
            `SELECT * FROM public.importer_check_publication_barrier($1, $2)`,
            [params.p_work_id, params.p_target_sort_key]
          );
          return { data: res.rows, error: null };
        }
        return { data: null, error: null };
      },
      from: (table: string) => ({
        select: (cols: string = '*') => ({
          eq: (col: string, val: any) => ({
            eq: (col2: string, val2: any) => ({
              order: (ordCol: string, { ascending }: { ascending: boolean }) => ({
                limit: async (lim: number) => {
                  const res = await pg.query(
                    `SELECT ${cols === '*' ? '*' : cols} FROM public.${table} WHERE ${col} = $1 AND ${col2} = $2 ORDER BY ${ordCol} ${ascending ? 'ASC' : 'DESC'} LIMIT ${lim}`,
                    [val, val2]
                  );
                  return { data: res.rows, error: null };
                }
              }),
              maybeSingle: async () => {
                const res = await pg.query(
                  `SELECT ${cols === '*' ? '*' : cols} FROM public.${table} WHERE ${col} = $1 AND ${col2} = $2 LIMIT 1`,
                  [val, val2]
                );
                return { data: res.rows[0] || null, error: null };
              }
            }),
            in: (inCol: string, inVals: any[]) => ({
              order: (ordCol: string, { ascending }: { ascending: boolean }) => ({
                limit: async (lim: number) => {
                  const placeholders = inVals.map((_, i) => `$${i + 2}`).join(',');
                  const res = await pg.query(
                    `SELECT ${cols === '*' ? '*' : cols} FROM public.${table} WHERE ${col} = $1 AND ${inCol} IN (${placeholders}) ORDER BY ${ordCol} ${ascending ? 'ASC' : 'DESC'} LIMIT ${lim}`,
                    [val, ...inVals]
                  );
                  return { data: res.rows, error: null };
                }
              })
            }),
            order: (ordCol: string, { ascending }: { ascending: boolean }) => ({
              limit: async (lim: number) => {
                const res = await pg.query(
                  `SELECT ${cols === '*' ? '*' : cols} FROM public.${table} WHERE ${col} = $1 ORDER BY ${ordCol} ${ascending ? 'ASC' : 'DESC'} LIMIT ${lim}`,
                  [val]
                );
                return { data: res.rows, error: null };
              }
            }),
            maybeSingle: async () => {
              const res = await pg.query(
                `SELECT ${cols === '*' ? '*' : cols} FROM public.${table} WHERE ${col} = $1 LIMIT 1`,
                [val]
              );
              return { data: res.rows[0] || null, error: null };
            }
          })
        }),
        update: (values: Record<string, any>) => ({
          eq: (col: string, val: any) => ({
            eq: async (col2: string, val2: any) => {
              const keys = Object.keys(values);
              const sets = keys.map((k, i) => `${k} = $${i + 1}`).join(', ');
              await pg.query(
                `UPDATE public.${table} SET ${sets} WHERE ${col} = $${keys.length + 1} AND ${col2} = $${keys.length + 2}`,
                [...Object.values(values), val, val2]
              );
              return { error: null };
            },
            then: async (resolve: any) => {
              const keys = Object.keys(values);
              const sets = keys.map((k, i) => `${k} = $${i + 1}`).join(', ');
              await pg.query(
                `UPDATE public.${table} SET ${sets} WHERE ${col} = $${keys.length + 1}`,
                [...Object.values(values), val]
              );
              resolve({ error: null });
            }
          })
        })
      })
    } as any;
  }

  it('Caso A: Obra existente com capítulo novo upstream (P0) -> aparece em lançamentos', async () => {
    const workId = await createTestWork('Obra Alpha', 'obra-alpha', 'MANGA');

    const mockSb = createMockSupabase(db);
    const barrier = new PublicationBarrier(mockSb);

    const chRes = await db.query(
      `INSERT INTO public.chapters (work_id, number, title, origin, is_fresh_release) VALUES ($1, 101, 'Cap 101', 'IMPORTER', true) RETURNING id`,
      [workId]
    );
    const chId = chRes.rows[0].id;

    // Insert valid page and media to pass integrity barrier
    const mediaRes = await db.query(
      `INSERT INTO public.media (provider, provider_key, mime, width, height, bytes, storage_ready, purpose, sha256, created_by)
       VALUES ('telegram', $1, 'image/jpeg', 800, 1200, 15000, true, 'editorial', 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', $2) RETURNING id`,
      [`key-${chId}`, botUserId]
    );
    await db.query(
      `INSERT INTO public.pages (chapter_id, position, media_id, width, height)
       VALUES ($1, 1, $2, 800, 1200)`,
      [chId, mediaRes.rows[0].id]
    );

    const wmRes = await db.query(
      `INSERT INTO public.importer_work_mappings (work_id, source, source_work_id, source_slug, source_title)
       VALUES ($1, 'mangaflix', 'mf-work-101', 'mf-obra-alpha', 'Obra Alpha') RETURNING id`,
      [workId]
    );
    const workMappingId = wmRes.rows[0].id;

    vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({ success: true }), { status: 200 }));

    await db.query(
      `INSERT INTO public.importer_chapter_mappings (chapter_id, work_id, work_mapping_id, source, source_chapter_id, chapter_number, chapter_sort_key, status, page_count) VALUES ($1, $2, $3, 'mangaflix', 'ch-101', 101, 101.0, 'STAGED', 1)`,
      [chId, workId, workMappingId]
    );

    const pub = await barrier.tryPublish(workId, 101.0, chId, true);
    expect(pub.published).toBe(true);

    const releases = await db.query(`SELECT * FROM public.get_recent_releases(10, 50, 'ALL') WHERE work_id = $1`, [workId]);
    expect(releases.rows.length).toBe(1);
    expect(releases.rows[0].chapter_number).toBe('101.00');
    expect(releases.rows[0].chapter_id).toBe(chId);
  });

  it('Caso B: Obra existente com gap preenchido (ex: cap 37 entre 36 e 38) -> aparece em lançamentos com data de publicação atual', async () => {
    const workId = await createTestWork('Obra Beta', 'obra-beta', 'MANHWA', '2025-01-01T00:00:00Z');

    // Existing published chapters 36 and 38 from last year
    await publishTestChapter(workId, 36, 'Cap 36', false, '2025-01-01T00:00:00Z');
    await publishTestChapter(workId, 38, 'Cap 38', false, '2025-01-01T00:00:00Z');

    // Now importer backfills gap chapter 37 today (is_fresh_release = false)
    const { chId: ch37Id } = await publishTestChapter(workId, 37, 'Cap 37 Gap Fill', false, new Date().toISOString());

    // Verify works.latest_chapter_published_at was updated to today by the trigger
    const workCheck = await db.query(`SELECT latest_chapter_published_at FROM public.works WHERE id = $1`, [workId]);
    const pubAt = new Date(workCheck.rows[0].latest_chapter_published_at).getTime();
    expect(pubAt).toBeGreaterThan(new Date('2026-01-01').getTime());

    // Verify chapter 37 appears as the top chapter in get_recent_releases
    const releases = await db.query(`SELECT * FROM public.get_recent_releases(10, 50, 'ALL') WHERE work_id = $1`, [workId]);
    expect(releases.rows.length).toBeGreaterThanOrEqual(1);
    expect(releases.rows[0].chapter_number).toBe('37.00');
    expect(releases.rows[0].chapter_id).toBe(ch37Id);
  });

  it('Caso C: Obra existente com preenchimento em lote (ex: caps 20 a 25) -> TODOS os capítulos aparecem como lançados, nenhum é descartado silenciosamente', async () => {
    const workId = await createTestWork('Obra Gamma', 'obra-gamma', 'MANGA');

    // Publish batch of 6 chapters (20, 21, 22, 23, 24, 25)
    for (let num = 20; num <= 25; num++) {
      const pubAt = new Date(Date.now() + (num - 20) * 1000).toISOString();
      await publishTestChapter(workId, num, `Capítulo ${num}`, false, pubAt);
    }

    const releases = await db.query(`SELECT * FROM public.get_recent_releases(10, 50, 'ALL') WHERE work_id = $1`, [workId]);
    expect(releases.rows.length).toBe(6);

    const returnedNumbers = releases.rows.map(r => Number(r.chapter_number)).sort((a, b) => a - b);
    expect(returnedNumbers).toEqual([20, 21, 22, 23, 24, 25]);
  });

  it('Caso D: Obra nova importada do cap 1 ao 10 -> os 10 capítulos aparecem no histórico de lançamentos', async () => {
    const workId = await createTestWork('Obra Delta Nova', 'obra-delta-nova', 'MANHUA');

    for (let num = 1; num <= 10; num++) {
      const pubAt = new Date(Date.now() + num * 500).toISOString();
      await publishTestChapter(workId, num, `Capítulo ${num}`, false, pubAt);
    }

    const releases = await db.query(`SELECT * FROM public.get_recent_releases(10, 50, 'ALL') WHERE work_id = $1`, [workId]);
    expect(releases.rows.length).toBe(10);
    const returnedNumbers = releases.rows.map(r => Number(r.chapter_number)).sort((a, b) => a - b);
    expect(returnedNumbers).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  });

  it('Caso E: Obra com cap 100 publicado que recebe o cap 50 (backfill) -> cap 50 aparece em lançamentos, mas capítulo mais alto continua sendo 100', async () => {
    const workId = await createTestWork('Obra Epsilon', 'obra-epsilon', 'MANGA', '2025-06-01T00:00:00Z');

    // Cap 100 published in 2025
    await publishTestChapter(workId, 100, 'Cap 100', false, '2025-06-01T00:00:00Z');

    // Backfill cap 50 published today
    const nowIso = new Date().toISOString();
    await publishTestChapter(workId, 50, 'Cap 50 Backfill', false, nowIso);

    // 1. Appears in releases (ordered by published_at DESC)
    const releases = await db.query(`SELECT * FROM public.get_recent_releases(10, 50, 'ALL') WHERE work_id = $1`, [workId]);
    expect(releases.rows.length).toBe(2);
    expect(releases.rows[0].chapter_number).toBe('50.00'); // Most recently published!

    // 2. Numerical ordering on work / reader page: highest chapter is still 100
    const obraChapters = await db.query(`SELECT number FROM public.chapters WHERE work_id = $1 AND published_at IS NOT NULL ORDER BY number DESC`, [workId]);
    expect(Number(obraChapters.rows[0].number)).toBe(100);
    expect(Number(obraChapters.rows[1].number)).toBe(50);
  });

  it('Caso F: Batch de capítulos publicados não quebra paginação nem duplica obras indevidamente', async () => {
    // Insert 5 works, each with 5 chapters
    for (let w = 1; w <= 5; w++) {
      const wId = await createTestWork(`Work ${w}`, `work-${w}`, 'MANGA');
      for (let c = 1; c <= 5; c++) {
        const pubAt = new Date(Date.now() + w * 10000 + c * 100).toISOString();
        await publishTestChapter(wId, c, `Cap ${c}`, false, pubAt);
      }
    }

    // Call RPC with limit 3 works
    const page1 = await db.query(`SELECT * FROM public.get_recent_releases(3, 50, 'ALL')`);
    const distinctWorksPage1 = Array.from(new Set(page1.rows.map(r => r.work_id)));
    expect(distinctWorksPage1.length).toBe(3);

    // Call RPC with limit 5 works
    const all = await db.query(`SELECT * FROM public.get_recent_releases(5, 50, 'ALL')`);
    const distinctWorksAll = Array.from(new Set(all.rows.map(r => r.work_id)));
    expect(distinctWorksAll.length).toBe(5);

    // Verify distinct work IDs: exactly 5, 0 duplicate work rows in the recent_works grouping
    expect(new Set(distinctWorksAll).size).toBe(5);
  });

  it('Caso G: is_fresh_release = false NÃO impede o capítulo de aparecer em lançamentos se published_at for recente', async () => {
    const workId = await createTestWork('Obra Zeta Backfill Only', 'obra-zeta', 'MANHWA');

    // Chapter published with is_fresh_release explicitly false
    const nowIso = new Date().toISOString();
    await publishTestChapter(workId, 15, 'Cap 15 Backfill', false, nowIso);

    const releases = await db.query(`SELECT * FROM public.get_recent_releases(10, 50, 'ALL') WHERE work_id = $1`, [workId]);
    expect(releases.rows.length).toBe(1);
    expect(releases.rows[0].chapter_number).toBe('15.00');
    expect(releases.rows[0].work_title).toBe('Obra Zeta Backfill Only');
  });
});
