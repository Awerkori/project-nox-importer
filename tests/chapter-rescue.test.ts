import { describe, it, expect, beforeAll, vi } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { randomUUID } from 'node:crypto';

// Mock processAndStoreMedia BEFORE importing engine (vi.mock is hoisted automatically)
// This avoids needing valid PNG bytes — the mock returns unique media IDs per call
let mockPgliteDb: PGlite;
vi.mock('../src/storage/media.js', () => ({
  processAndStoreMedia: async (supabase: any, storage: any, bytes: Uint8Array, userId: string, purpose: string) => {
    const mediaId = randomUUID();
    const providerKey = `mock_file_${mediaId}`;
    // Insert directly into PGlite to mirror real behavior
    await mockPgliteDb.query(
      `insert into public.media (id, provider, provider_key, mime, width, height, bytes, sha256, created_by, storage_ready, purpose)
       values ($1, 'telegram', $2, 'image/png', 1, 1, $3, $4, $5, true, $6)`,
      [mediaId, providerKey, bytes.length, randomUUID(), userId, purpose]
    );
    return { mediaId, reused: false, width: 1, height: 1, bytes: bytes.length, mime: 'image/png' };
  },
}));

import { ImporterEngine, classifyPageUrl, NarrativePageUnavailableError } from '../src/core/engine.js';
import { SourceRegistry } from '../src/sources/registry.js';
import { SourceAdapter } from '../src/sources/types.js';
import { MockStorageProvider } from '../src/storage/mock.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { Config } from '../src/config.js';

describe('Cross-Provider Chapter Rescue & Page Classification', () => {
  let db: PGlite;
  let supabaseMock: any;
  let storage: MockStorageProvider;
  let engine: ImporterEngine;
  let mangaflixAdapter: SourceAdapter;
  let manhastroAdapter: SourceAdapter;
  const botUserId = '00000000-0000-4000-8000-000000000001';

  const samplePngBytes = new Uint8Array([
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
    0x00, 0x00, 0x00, 0x0d,
    0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01,
    0x00, 0x00, 0x00, 0x01,
    0x08, 0x06, 0x00, 0x00, 0x00,
    0x1f, 0x15, 0xc4, 0x89,
    0x00, 0x00, 0x00, 0x0a,
    0x49, 0x44, 0x41, 0x54,
    0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00, 0x05, 0x00, 0x01,
    0x0d, 0x0a, 0x2d, 0xb4,
    0x00, 0x00, 0x00, 0x00,
    0x49, 0x45, 0x4e, 0x44,
    0xae, 0x42, 0x60, 0x82,
  ]);

  beforeAll(async () => {
    db = new PGlite({ extensions: { pg_trgm } });
    mockPgliteDb = db;
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
    await db.exec(readFileSync(resolve('migrations/006_importer_publication_barrier.sql'), 'utf8'));
    await db.exec(readFileSync(resolve('migrations/007_importer_lease_recovery.sql'), 'utf8'));
    await db.exec(`ALTER TABLE public.works ADD COLUMN IF NOT EXISTS latest_chapter_published_at timestamptz;`);
    await db.exec(`ALTER TABLE public.chapters ADD COLUMN IF NOT EXISTS is_fresh_release boolean DEFAULT false;`);

    await db.query(`insert into auth.users (id, email, email_confirmed_at) values ($1, 'bot@projectnox.com', now())`, [botUserId]);
    await db.query(`update public.access_roles set role = 'ADMIN' where user_id = $1`, [botUserId]);

    // Activate mangaflix and manhastro sources (seeded as PAUSED/disabled by migrations)
    await db.query(`update public.importer_sources set enabled = true, status = 'ACTIVE' where id in ('mangaflix', 'manhastro')`);


    // Build adapter mocks
    mangaflixAdapter = {
      id: 'mangaflix',
      name: 'MangaFlix',
      baseUrl: 'https://mangaflix.net',
      fetchUpdatedWorks: async () => ({ works: [], nextCursor: 'now' }),
      fetchWorkDetails: async (id) => ({
        sourceWorkId: id,
        title: 'Koko ni Iru yo!',
        slug: 'koko-ni-iru-yo',
        kind: 'MANGA',
        status: 'COMPLETED',
        coverUrl: 'https://static.mangaflix.net/cover.jpg',
      }),
      fetchChapters: async () => [],
      fetchChapterPages: async () => {
        // Returns pages, but page 1 is dead 404
        return [
          'https://static.mangaflix.net/data/mangas/mf-koko/dead-page-1.jpg',
          'https://static.mangaflix.net/data/mangas/mf-koko/credits.jpg',
        ];
      },
      searchWorks: async () => [],
    };

    manhastroAdapter = {
      id: 'manhastro',
      name: 'Manhastro',
      baseUrl: 'https://manhastro.net',
      fetchUpdatedWorks: async () => ({ works: [], nextCursor: 'now' }),
      fetchWorkDetails: async (id) => ({
        sourceWorkId: id,
        title: 'Koko ni Iru yo!',
        slug: 'koko-ni-iru-yo',
        kind: 'MANGA',
        status: 'COMPLETED',
        coverUrl: 'https://manhastro.net/cover.webp',
      }),
      fetchChapters: async () => [
        {
          sourceChapterId: 'mh-ch-12',
          number: 12,
          title: 'Capítulo 12',
          createdAt: new Date().toISOString(),
          pageCount: 2,
        },
      ],
      fetchChapterPages: async () => [
        'https://albums.manhastro.net/manga/page-1.webp',
        'https://albums.manhastro.net/manga/page-2.webp',
      ],
      searchWorks: async () => [],
    };

    const registry = new SourceRegistry();
    registry.register(mangaflixAdapter);
    registry.register(manhastroAdapter);

    // Build mock Supabase
    supabaseMock = {
      from: (table: string) => {
        let selectedColumns = '*';
        let filterStatements: Array<{ sql: string; vals: any[] }> = [];
        let orderStatement = '';
        let limitCount: number | null = null;

        const builder: any = {
          select: (cols: string = '*') => {
            selectedColumns = cols;
            return builder;
          },
          eq: (col: string, val: any) => {
            filterStatements.push({ sql: `${col} = $${filterStatements.length + 1}`, vals: [val] });
            return builder;
          },
          neq: (col: string, val: any) => {
            filterStatements.push({ sql: `${col} != $${filterStatements.length + 1}`, vals: [val] });
            return builder;
          },
          in: (col: string, vals: any[]) => {
            if (!vals || vals.length === 0) {
              filterStatements.push({ sql: `1 = 0`, vals: [] });
            } else {
              filterStatements.push({ sql: `${col} = ANY($${filterStatements.length + 1})`, vals: [vals] });
            }
            return builder;
          },
          ilike: (col: string, val: any) => {
            filterStatements.push({ sql: `${col} ilike $${filterStatements.length + 1}`, vals: [val] });
            return builder;
          },
          overlaps: (col: string, vals: any[]) => {
            if (!vals || vals.length === 0) {
              filterStatements.push({ sql: `1 = 0`, vals: [] });
            } else {
              filterStatements.push({ sql: `${col} && $${filterStatements.length + 1}`, vals: [vals] });
            }
            return builder;
          },
          not: (col: string, op: string, val: any) => {
            if (op === 'is' && val === null) {
              filterStatements.push({ sql: `${col} is not null`, vals: [] });
            }
            return builder;
          },
          order: (col: string, opts?: { ascending?: boolean }) => {
            const dir = opts?.ascending === false ? 'desc' : 'asc';
            orderStatement = `order by ${col} ${dir}`;
            return builder;
          },
          limit: (n: number) => {
            limitCount = n;
            return builder;
          },
          insert: async (rows: any | any[]) => {
            const items = Array.isArray(rows) ? rows : [rows];
            for (const item of items) {
              const keys = Object.keys(item);
              const vals = Object.values(item);
              const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
              await db.query(`insert into public.${table} (${keys.join(', ')}) values (${placeholders})`, vals);
            }
            return { data: items, error: null };
          },
          upsert: async (rows: any | any[], opts?: { onConflict?: string }) => {
            const items = Array.isArray(rows) ? rows : [rows];
            for (const item of items) {
              const keys = Object.keys(item);
              const vals = Object.values(item);
              const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
              let query = `insert into public.${table} (${keys.join(', ')}) values (${placeholders})`;
              if (opts?.onConflict) {
                const updateCols = keys.filter((k) => !opts.onConflict!.split(',').includes(k));
                const updates = updateCols.map((c) => `${c} = excluded.${c}`).join(', ');
                query += ` on conflict (${opts.onConflict}) do update set ${updates}`;
              }
              await db.query(query, vals);
            }
            return { data: items, error: null };
          },
          update: (fields: any) => {
            const updateFilters: Array<{ col: string; val: any }> = [];
            const executeUpdate = async () => {
              const keys = Object.keys(fields);
              const vals = Object.values(fields);
              const setClauses = keys.map((k, i) => `${k} = $${i + 1}`).join(', ');
              const whereClauses = updateFilters.map((f, i) => `${f.col} = $${keys.length + i + 1}`).join(' and ');
              for (const f of updateFilters) {
                vals.push(f.val);
              }
              const where = whereClauses.length > 0 ? `where ${whereClauses}` : '';
              await db.query(`update public.${table} set ${setClauses} ${where}`, vals);
              return { error: null };
            };
            const updateBuilder: any = {
              eq: (col: string, val: any) => {
                updateFilters.push({ col, val });
                return updateBuilder;
              },
              then: (resolveFn: any, rejectFn?: any) => executeUpdate().then(resolveFn, rejectFn),
            };
            return updateBuilder;
          },
          then: async (resolveFn: any, rejectFn?: any) => {
            const allVals: any[] = [];
            let whereClause = '';
            if (filterStatements.length > 0) {
              const parts = filterStatements.map((f) => {
                let s = f.sql;
                for (let i = 0; i < f.vals.length; i++) {
                  allVals.push(f.vals[i]);
                  s = s.replace(`$${i + 1}`, `$${allVals.length}`);
                }
                return s;
              });
              whereClause = `where ${parts.join(' and ')}`;
            }
            let query = `select ${selectedColumns} from public.${table} ${whereClause} ${orderStatement}`;
            if (limitCount !== null) query += ` limit ${limitCount}`;

            try {
              const res = await db.query(query, allVals);
              resolveFn({ data: res.rows, error: null });
            } catch (err: any) {
              resolveFn({ data: null, error: err });
            }
          },
          maybeSingle: async () => {
            const { data, error } = await builder;
            return { data: data && data.length > 0 ? data[0] : null, error };
          },
          single: async () => {
            const { data, error } = await builder;
            if (!data || data.length === 0) return { data: null, error: new Error('Row not found') };
            return { data: data[0], error: null };
          },
        };

        return builder;
      },
      rpc: async (fnName: string, params: any) => {
        if (fnName === 'importer_replace_pages') {
          const res = await db.query('SELECT importer_replace_pages($1,$2::jsonb) count',[params.p_chapter_id,JSON.stringify(params.p_pages)]);
          return {data:res.rows[0].count,error:null};
        }
        // Mock all RPC calls — for publication barrier, return "can publish"
        if (fnName === 'importer_check_publication_barrier') {
          return { data: [], error: null };
        }
        // For lease recovery and other RPCs, return empty success
        return { data: { recovered: 0, failed: 0 }, error: null };
      },
    };

    storage = new MockStorageProvider();
    const rateLimiter = new HostRateLimiter();
    const config: Config = {
      SUPABASE_URL: 'http://localhost',
      SUPABASE_SERVICE_ROLE_KEY: 'test',
      WORKER_ID: 'test-worker',
      STORAGE_PROVIDER: 'mock',
      TELEGRAM_BOT_TOKEN: 'test',
      STORAGE_BRIDGE_SECRET: 'test',
      INTERNAL_API_SECRET: 'test',
      ENABLE_AUTO_TUNING: false,
      BATCH_PAGE_DOWNLOAD_CONCURRENCY: 2,
    };

    engine = new ImporterEngine(supabaseMock, storage, registry, rateLimiter, config);

    // Mock fetchImageBytes to simulate 404 on mangaflix dead page, 200 on others
    (engine as any).fetchImageBytes = async (url: string, source: string) => {
      if (url.includes('dead-page-1')) {
        throw new Error(`Failed to download image from ${url}: HTTP 404`);
      }
      return samplePngBytes;
    };
  });

  it('classifies URLs correctly into content and non-content pages', () => {
    expect(classifyPageUrl('https://example.com/chapter1/credit.jpg', 0, 10)).toBe('CREDIT_PAGE');
    expect(classifyPageUrl('https://example.com/chapter1/recrutamento.jpg', 0, 10)).toBe('RECRUITMENT_PAGE');
    expect(classifyPageUrl('https://example.com/chapter1/aviso.jpg', 0, 10)).toBe('WARNING_PAGE');
    expect(classifyPageUrl('https://example.com/chapter1/discord.png', 0, 10)).toBe('PROMO_PAGE');
    expect(classifyPageUrl('https://example.com/chapter1/01.jpg', 0, 10)).toBe('CONTENT_PAGE');
    expect(classifyPageUrl('https://example.com/chapter1/page_14.jpg', 14, 40)).toBe('CONTENT_PAGE');
  });

  it('performs cross-provider chapter rescue when a narrative page fails with 404 on primary source', async () => {
    // 1. Create work in DB with valid cover
    const coverRes = await db.query(
      `insert into public.media (created_by, provider, provider_key, mime, width, height, bytes, sha256, storage_ready, purpose)
       values ($1, 'telegram', 'tg-cover-rescue', 'image/jpeg', 800, 1200, 50000, 'sha-cover-rescue', true, 'editorial') returning id`,
      [botUserId]
    );
    const coverId = (coverRes.rows[0] as any).id;

    const wRes = await db.query(
      `insert into public.works (title, slug, published, cover_id) values ('Koko ni Iru yo!', 'koko-ni-iru-yo', true, $1) returning id`,
      [coverId]
    );
    const workId = (wRes.rows[0] as any).id;

    // 2. Map mangaflix and manhastro
    const mfWorkMapRes = await db.query(
      `insert into public.importer_work_mappings (source, source_work_id, work_id, source_slug, source_title)
       values ('mangaflix', 'mf-koko', $1, 'koko-ni-iru-yo', 'Koko ni Iru yo!') returning id`,
      [workId]
    );
    const mfWorkMapId = (mfWorkMapRes.rows[0] as any).id;

    const mhWorkMapRes = await db.query(
      `insert into public.importer_work_mappings (source, source_work_id, work_id, source_slug, source_title)
       values ('manhastro', '100060', $1, 'koko-ni-iru-yo', 'Koko ni Iru yo!') returning id`,
      [workId]
    );
    const mhWorkMapId = (mhWorkMapRes.rows[0] as any).id;

    // 3. Register chapter mapping for both sources
    await db.query(
      `insert into public.importer_chapter_mappings (source, source_chapter_id, work_id, work_mapping_id, chapter_number, chapter_sort_key, status, is_page_provider)
       values ('mangaflix', 'mf-ch-12', $1, $2, 12, 12, 'PENDING', true)`,
      [workId, mfWorkMapId]
    );
    await db.query(
      `insert into public.importer_chapter_mappings (source, source_chapter_id, work_id, work_mapping_id, chapter_number, chapter_sort_key, status, is_page_provider)
       values ('manhastro', 'mh-ch-12', $1, $2, 12, 12, 'PENDING', false)`,
      [workId, mhWorkMapId]
    );

    // 4. Create queue job with source mangaflix
    const qRes = await db.query(
      `insert into public.importer_queue (task_type, source, dedupe_key, status, priority, attempts, max_attempts, payload)
       values ('IMPORT_CHAPTER', 'mangaflix', $1, 'QUEUED', 80, 0, 5, $2::jsonb) returning id`,
      [
        `work:${workId}:chapter:12`,
        JSON.stringify({
          workId,
          chapterNumber: 12,
          chapterTitle: 'Capítulo 12',
          sourceWorkId: 'mf-koko',
          sourceChapterId: 'mf-ch-12',
          workMappingId: mfWorkMapId,
          fallbackSources: [
            { source: 'manhastro', sourceChapterId: 'mh-ch-12', mappingId: mhWorkMapId }
          ],
        }),
      ]
    );
    const jobId = (qRes.rows[0] as any).id;

    // 5. Execute job via engine
    const job = {
      id: jobId,
      task_type: 'IMPORT_CHAPTER' as const,
      source: 'mangaflix',
      payload: {
        workId,
        chapterNumber: 12,
        chapterTitle: 'Capítulo 12',
        sourceWorkId: 'mf-koko',
        sourceChapterId: 'mf-ch-12',
        workMappingId: mfWorkMapId,
        fallbackSources: [
          { source: 'manhastro', sourceChapterId: 'mh-ch-12', mappingId: mhWorkMapId }
        ],
      },
      priority: 80,
      attempts: 0,
      max_attempts: 5,
    };

    await (engine as any).handleImportChapter(job);

    // 6. Verify results in DB
    // Job should be staged or completed with last_recovered_error referencing CROSS_PROVIDER_RESCUE
    const jobCheck = await db.query(`select * from public.importer_queue where id = $1`, [jobId]);
    const jobRow = jobCheck.rows[0] as any;
    expect(jobRow.source).toBe('manhastro');
    expect(jobRow.last_recovered_error).toContain('CROSS_PROVIDER_RESCUE');
    expect(jobRow.last_recovered_error).toContain('manhastro');
    expect(jobRow.last_error).toBeNull();

    // The chapter in chapters table should exist and have 2 pages
    const chCheck = await db.query(`select id, number from public.chapters where work_id = $1 and number = 12`, [workId]);
    expect(chCheck.rows.length).toBe(1);
    const chId = (chCheck.rows[0] as any).id;

    const pagesCheck = await db.query(`select position, media_id from public.pages where chapter_id = $1`, [chId]);
    expect(pagesCheck.rows.length).toBe(2);

    // Mangaflix mapping should be marked not page provider with replacement note
    const mfMapCheck = await db.query(
      `select is_page_provider, last_error from public.importer_chapter_mappings where work_id = $1 and source = 'mangaflix' and chapter_number = 12`,
      [workId]
    );
    expect((mfMapCheck.rows[0] as any).is_page_provider).toBe(false);
    expect((mfMapCheck.rows[0] as any).last_error).toContain('CROSS_PROVIDER_RESCUE');

    // Manhastro mapping should be marked page provider
    const mhMapCheck = await db.query(
      `select is_page_provider, status, page_count from public.importer_chapter_mappings where work_id = $1 and source = 'manhastro' and chapter_number = 12`,
      [workId]
    );
    expect((mhMapCheck.rows[0] as any).is_page_provider).toBe(true);
    expect((mhMapCheck.rows[0] as any).page_count).toBe(2);
  });
});
