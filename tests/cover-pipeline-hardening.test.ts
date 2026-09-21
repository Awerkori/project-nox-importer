import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { ImporterEngine } from '../src/core/engine.js';
import { DeduplicationEngine, CandidateWork } from '../src/core/deduplication.js';
import { SourceRegistry } from '../src/sources/registry.js';
import { SourceAdapter } from '../src/sources/types.js';
import { MockStorageProvider } from '../src/storage/mock.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';
import { Config } from '../src/config.js';

describe('Cover Pipeline Hardening & Resilient Ingestion', () => {
  let db: PGlite;
  let supabaseMock: any;
  let storage: MockStorageProvider;
  let engine: ImporterEngine;
  let deduplication: DeduplicationEngine;
  let registry: SourceRegistry;
  let mockAdapter: SourceAdapter;
  const botUserId = '00000000-0000-4000-8000-000000000001';

  // Helper to create synthetic valid JPEG with specified dimensions and minimum bytes
  function createValidJpeg(w: number, h: number, minBytes = 2000): Uint8Array {
    const header = [
      0xff, 0xd8, // SOI
      0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, // APP0
      0xff, 0xc0, 0x00, 0x0b, 0x08, (h >> 8) & 0xff, h & 0xff, (w >> 8) & 0xff, w & 0xff, 0x01, 0x01, 0x11, 0x00, // SOF0
      0xff, 0xda, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00, 0x3f, 0x00, // SOS
    ];
    const paddingSize = Math.max(0, minBytes - header.length - 2);
    const scanData = new Array(paddingSize).fill(0x00);
    const footer = [0xff, 0xd9]; // EOI
    return new Uint8Array([...header, ...scanData, ...footer]);
  }

  // Minimal valid 100x100 GIF padded to >= 2000 bytes for testing GIF rejection
  function createMinimalGif(minBytes = 2000): Uint8Array {
    const header = [
      0x47, 0x49, 0x46, 0x38, 0x39, 0x61, // GIF89a
      0x64, 0x00, 0x64, 0x00, // 100x100
      0x80, 0x00, 0x00, // GCT
      0x00, 0x00, 0x00, 0xff, 0xff, 0xff,
      0x21, 0xfe, // Comment extension
    ];
    const padLen = Math.max(10, minBytes - header.length - 20);
    const commentBytes: number[] = [];
    let remaining = padLen;
    while (remaining > 0) {
      const chunk = Math.min(remaining, 250);
      commentBytes.push(chunk);
      for (let i = 0; i < chunk; i++) commentBytes.push(0x20);
      remaining -= chunk;
    }
    commentBytes.push(0x00); // Block terminator

    const imageDesc = [
      0x2c, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x64, 0x00, 0x00,
      0x02, 0x02, 0x44, 0x01, 0x00, 0x3b,
    ];
    return new Uint8Array([...header, ...commentBytes, ...imageDesc]);
  }

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
      const sql = readFileSync(resolve(mangaMigrationsDir, f), 'utf-8')
        .replace('create extension if not exists pgcrypto;', '')
        .replace(/create\s+index\s+concurrently/gi, 'create index');
      try {
        await db.exec(sql);
      } catch (e: any) {
        // Migration idempotence
      }
    }

    const importerMigrationsDir = resolve('migrations');
    const impFiles = readdirSync(importerMigrationsDir).filter((f) => f.endsWith('.sql')).sort();
    for (const f of impFiles) {
      const sql = readFileSync(resolve(importerMigrationsDir, f), 'utf-8')
        .replace(/create\s+index\s+concurrently/gi, 'create index');
      try {
        await db.exec(sql);
      } catch (e: any) {
        // Migration idempotence
      }
    }
    await db.exec(`ALTER TABLE public.chapters ADD COLUMN IF NOT EXISTS is_fresh_release boolean DEFAULT false;`);
    await db.exec(`ALTER TABLE public.works ADD COLUMN IF NOT EXISTS latest_chapter_published_at timestamptz;`);

    supabaseMock = {
      from: (table: string) => {
        let currentTable = table;
        let selectedFields = '*';
        let filters: Array<{ col: string; op: string; val: any }> = [];
        let orderBy: Array<{ col: string; ascending: boolean }> = [];
        let limitVal: number | null = null;
        let isSingle = false;
        let isMaybeSingle = false;
        let isCount = false;

        const builder: any = {
          select: (fields = '*', opts?: any) => {
            selectedFields = fields;
            if (opts?.count) isCount = true;
            return builder;
          },
          eq: (col: string, val: any) => { filters.push({ col, op: '=', val }); return builder; },
          neq: (col: string, val: any) => { filters.push({ col, op: '!=', val }); return builder; },
          in: (col: string, val: any[]) => { filters.push({ col, op: 'IN', val }); return builder; },
          is: (col: string, val: any) => { filters.push({ col, op: 'IS', val }); return builder; },
          not: (col: string, op: string, val: any) => { filters.push({ col, op: `NOT ${op}`, val }); return builder; },
          or: (expr: string) => { filters.push({ col: '__OR__', op: 'OR', val: expr }); return builder; },
          order: (col: string, { ascending = true } = {}) => { orderBy.push({ col, ascending }); return builder; },
          limit: (n: number) => { limitVal = n; return builder; },
          single: () => { isSingle = true; return builder; },
          maybeSingle: () => { isMaybeSingle = true; return builder; },

          insert: async (data: any) => {
            const rows = Array.isArray(data) ? data : [data];
            if (rows.length === 0) return { data: [], error: null };
            const cols = Object.keys(rows[0]);
            let insertedData: any[] = [];
            for (const r of rows) {
              const vals = cols.map((c) => r[c]);
              const placeholders = vals.map((_, i) => `$${i + 1}`).join(', ');
              const q = `INSERT INTO "${currentTable}" (${cols.map((c) => `"${c}"`).join(', ')}) VALUES (${placeholders}) RETURNING *`;
              const res = await db.query(q, vals);
              insertedData.push(...res.rows);
            }
            return { data: isSingle || !Array.isArray(data) ? insertedData[0] : insertedData, error: null };
          },

          upsert: async (data: any, opts?: { onConflict?: string }) => {
            const rows = Array.isArray(data) ? data : [data];
            if (rows.length === 0) return { data: [], error: null };
            const cols = Object.keys(rows[0]);
            const onConflict = opts?.onConflict || 'id';
            const conflictCols = onConflict.split(',').map((c) => `"${c.trim()}"`).join(', ');
            let upsertedData: any[] = [];

            for (const r of rows) {
              const vals = cols.map((c) => r[c]);
              const placeholders = vals.map((_, i) => `$${i + 1}`).join(', ');
              const updateSet = cols
                .filter((c) => !opts?.onConflict?.includes(c))
                .map((c) => `"${c}" = EXCLUDED."${c}"`)
                .join(', ');

              const q = `INSERT INTO "${currentTable}" (${cols.map((c) => `"${c}"`).join(', ')})
                         VALUES (${placeholders})
                         ON CONFLICT (${conflictCols}) DO UPDATE SET ${updateSet || 'id = EXCLUDED.id'}
                         RETURNING *`;
              const res = await db.query(q, vals);
              upsertedData.push(...res.rows);
            }
            return { data: isSingle || !Array.isArray(data) ? upsertedData[0] : upsertedData, error: null };
          },

          update: (data: any) => {
            const updateCols = Object.keys(data);
            const updateVals = Object.values(data);
            return {
              eq: (col: string, val: any) => { filters.push({ col, op: '=', val }); return executeUpdate(); },
              in: (col: string, val: any[]) => { filters.push({ col, op: 'IN', val }); return executeUpdate(); },
            };

            async function executeUpdate() {
              const setClause = updateCols.map((c, i) => `"${c}" = $${i + 1}`).join(', ');
              let whereClause = '';
              const params = [...updateVals];

              if (filters.length > 0) {
                const conds = filters.map((f) => {
                  params.push(f.val);
                  return `"${f.col}" ${f.op} $${params.length}`;
                });
                whereClause = `WHERE ${conds.join(' AND ')}`;
              }

              const q = `UPDATE "${currentTable}" SET ${setClause} ${whereClause} RETURNING *`;
              const res = await db.query(q, params);
              return { data: res.rows, error: null };
            }
          },

          then: async (resolve: any, reject: any) => {
            try {
              let whereClause = '';
              const params: any[] = [];

              if (filters.length > 0) {
                const conds = filters.map((f) => {
                  if (f.col === '__OR__') {
                    const orParts = (f.val as string).split(',').map((part) => {
                      const [c, rawOp, ...vParts] = part.split('.');
                      const v = vParts.join('.');
                      params.push(v);
                      return `"${c}" = $${params.length}`;
                    });
                    return `(${orParts.join(' OR ')})`;
                  }
                  if (f.op === 'IS' && f.val === null) return `"${f.col}" IS NULL`;
                  if (f.op === 'NOT IS' && f.val === null) return `"${f.col}" IS NOT NULL`;
                  if (f.op === 'IN') {
                    const pList = (f.val as any[]).map((v) => {
                      params.push(v);
                      return `$${params.length}`;
                    }).join(', ');
                    return `"${f.col}" IN (${pList || 'NULL'})`;
                  }
                  params.push(f.val);
                  return `"${f.col}" ${f.op} $${params.length}`;
                });
                whereClause = `WHERE ${conds.join(' AND ')}`;
              }

              let orderClause = '';
              if (orderBy.length > 0) {
                orderClause = `ORDER BY ${orderBy.map((o) => `"${o.col}" ${o.ascending ? 'ASC' : 'DESC'}`).join(', ')}`;
              }

              let limitClause = '';
              if (limitVal !== null) {
                limitClause = `LIMIT ${limitVal}`;
              }

              const fields = isCount ? 'count(*)' : (selectedFields === '*' ? '*' : selectedFields.split(',').map((f) => `"${f.trim()}"`).join(', '));
              const q = `SELECT ${fields} FROM "${currentTable}" ${whereClause} ${orderClause} ${limitClause}`;
              const res = await db.query(q, params);

              if (isCount) {
                return resolve({ count: parseInt((res.rows[0] as any).count, 10), data: res.rows, error: null });
              }
              if (isSingle) {
                return resolve({ data: res.rows[0] || null, error: res.rows.length === 0 ? { message: 'Not found' } : null });
              }
              if (isMaybeSingle) {
                return resolve({ data: res.rows[0] || null, error: null });
              }
              return resolve({ data: res.rows, error: null });
            } catch (err) {
              return resolve({ data: null, error: err });
            }
          },
        };

        return builder;
      },
    };

    // Ensure bot user exists
    await db.query(`
      INSERT INTO auth.users (id, email, raw_user_meta_data)
      VALUES ($1, 'bot@projectnox.internal', '{"role":"service_role"}')
      ON CONFLICT DO NOTHING;
    `, [botUserId]);

    const rateLimiter = new HostRateLimiter(10.0);
    storage = new MockStorageProvider();
    registry = new SourceRegistry(rateLimiter, 'test-token', 'https://mock.manga.workers.dev');

    mockAdapter = {
      id: 'mocksource',
      name: 'Mock Source',
      baseUrl: 'https://mocksource.com',
      getImageHeaders: (url: string) => ({
        Referer: 'https://mocksource.com/',
        'X-Mock-Source-Auth': 'mock-secret-token',
      }),
      fetchUpdatedWorks: async () => ({ works: [], nextCursor: null }),
      fetchWorkDetails: async (id: string) => ({
        sourceWorkId: id,
        title: 'Mock Title',
        slug: 'mock-title',
        coverUrl: 'https://mocksource.com/covers/sample.jpg',
      }),
      fetchChapters: async () => [],
      fetchChapterPages: async () => [],
    } as any;

    registry.register(mockAdapter);

    const config: Config = {
      IMPORTER_USER_ID: botUserId,
      DISCLOUD_APP_ID: 'test-app',
      DISCLOUD_TOKEN: 'test-token',
      SUPABASE_URL: 'http://localhost:54321',
      SUPABASE_SERVICE_ROLE_KEY: 'test-key',
      DIRECT_DATABASE_URL: 'postgres://test',
      TELEGRAM_STORAGE_BOT_TOKENS: ['123:ABC'],
      TELEGRAM_STORAGE_CHANNEL_ID: '-100123456789',
      WORKER_CONCURRENCY: 2,
    };

    await db.query(`
      INSERT INTO importer_sources (id, name, base_url, status, enabled)
      VALUES ('mocksource', 'Mock Source', 'https://mocksource.com', 'ACTIVE', true),
             ('kuro', 'Kuro', 'https://kuromangas.com', 'ACTIVE', true),
             ('mangaflix', 'MangaFlix', 'https://mangaflix.net', 'ACTIVE', true),
             ('mangadex', 'MangaDex', 'https://mangadex.org', 'ACTIVE', true),
             ('hipercool', 'Hipercool', 'https://lerhentais.com', 'ACTIVE', true)
      ON CONFLICT (id) DO NOTHING;
    `);

    deduplication = new DeduplicationEngine(supabaseMock);
    engine = new ImporterEngine(supabaseMock, storage, registry, rateLimiter, config);
  });

  afterAll(async () => {
    await db.close();
  });

  it('downloads and registers a valid JPEG cover image with purpose=editorial', async () => {
    const validJpeg = createValidJpeg(300, 450, 3000);
    const mockUrl = 'https://mocksource.com/covers/valid-cover.jpg';

    // Mock global fetch to return valid JPEG
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input: any) => {
      const urlStr = String(input);
      if (urlStr === mockUrl) {
        return new Response(validJpeg, {
          status: 200,
          headers: { 'Content-Type': 'image/jpeg' },
        });
      }
      return new Response('Not found', { status: 404 });
    });

    const mediaId = await engine.downloadAndRegisterImage(mockUrl, botUserId, 'editorial', 'mocksource');
    expect(mediaId).toBeTruthy();

    // Verify media record in database
    const { data: media } = await supabaseMock.from('media').select('*').eq('id', mediaId).single();
    expect(media).toBeTruthy();
    expect(media.storage_ready).toBe(true);
    expect(media.purpose).toBe('editorial');
    expect(media.width).toBe(300);
    expect(media.height).toBe(450);
    expect(media.bytes).toBe(validJpeg.length);
    expect(media.mime).toBe('image/jpeg');

    fetchSpy.mockRestore();
  });

  it('injects adapter headers and Referer when source is specified', async () => {
    const validJpeg = createValidJpeg(200, 300, 2000);
    const mockUrl = 'https://mocksource.com/covers/auth-cover.jpg';
    let interceptedHeaders: any = null;

    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input: any, init: any) => {
      interceptedHeaders = init?.headers;
      return new Response(validJpeg, {
        status: 200,
        headers: { 'Content-Type': 'image/jpeg' },
      });
    });

    await engine.downloadAndRegisterImage(mockUrl, botUserId, 'editorial', 'mocksource');

    expect(interceptedHeaders).toBeTruthy();
    expect(interceptedHeaders['Referer']).toBe('https://mocksource.com/');
    expect(interceptedHeaders['X-Mock-Source-Auth']).toBe('mock-secret-token');

    fetchSpy.mockRestore();
  });

  it('rejects HTML Cloudflare anti-bot challenge response even if HTTP 200', async () => {
    const challengeHtml = `<!DOCTYPE html><html><head><title>Just a moment...</title></head><body>cf-browser-verification</body></html>`;
    const mockUrl = 'https://mocksource.com/covers/cloudflare-blocked.jpg';

    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async () => {
      return new Response(challengeHtml, {
        status: 200,
        headers: { 'Content-Type': 'text/html; charset=UTF-8' },
      });
    });

    await expect(
      engine.downloadAndRegisterImage(mockUrl, botUserId, 'editorial', 'mocksource')
    ).rejects.toThrow(/Cloudflare challenge HTML received instead of image/i);

    fetchSpy.mockRestore();
  });

  it('rejects tiny placeholder images smaller than 1.5KB', async () => {
    // 500-byte valid JPEG
    const tinyJpeg = createValidJpeg(200, 300, 500);
    const mockUrl = 'https://mocksource.com/covers/tiny-spacer.jpg';

    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async () => {
      return new Response(tinyJpeg, {
        status: 200,
        headers: { 'Content-Type': 'image/jpeg' },
      });
    });

    await expect(
      engine.downloadAndRegisterImage(mockUrl, botUserId, 'editorial', 'mocksource')
    ).rejects.toThrow(/too small/i);

    fetchSpy.mockRestore();
  });

  it('rejects cover URLs matching chapter page patterns', async () => {
    const chapterUrls = [
      'https://mocksource.com/reader/chapter-1/page_01.jpg',
      'https://mocksource.com/capitulo/12/pagina-05.jpg',
      'https://mocksource.com/manga/slug/leitor/ch1_page_02.png',
    ];

    for (const url of chapterUrls) {
      await expect(
        engine.downloadAndRegisterImage(url, botUserId, 'editorial', 'mocksource')
      ).rejects.toThrow(/Rejected cover URL matching chapter pattern/i);
    }
  });

  it('rejects extreme aspect ratio images and animated GIFs as covers', async () => {
    // 100x600 (aspect ratio 6.0 > 2.5)
    const stripJpeg = createValidJpeg(100, 600, 2000);
    const stripUrl = 'https://mocksource.com/covers/tall-strip.jpg';

    const fetchSpy1 = vi.spyOn(globalThis, 'fetch').mockImplementation(async () => {
      return new Response(stripJpeg, {
        status: 200,
        headers: { 'Content-Type': 'image/jpeg' },
      });
    });

    await expect(
      engine.downloadAndRegisterImage(stripUrl, botUserId, 'editorial', 'mocksource')
    ).rejects.toThrow(/Cover aspect ratio rejected/i);

    fetchSpy1.mockRestore();

    // Animated GIF
    const gifBytes = createMinimalGif();
    const gifUrl = 'https://mocksource.com/covers/animated.gif';

    const fetchSpy2 = vi.spyOn(globalThis, 'fetch').mockImplementation(async () => {
      return new Response(gifBytes, {
        status: 200,
        headers: { 'Content-Type': 'image/gif' },
      });
    });

    await expect(
      engine.downloadAndRegisterImage(gifUrl, botUserId, 'editorial', 'mocksource')
    ).rejects.toThrow(/Animated GIF rejected as cover image/i);

    fetchSpy2.mockRestore();
  });

  describe('Cover Precedence in applyMetadataPrecedence', () => {
    it('preserves MANUAL cover strictly and never overwrites it', async () => {
      const workId = crypto.randomUUID();
      const manualCoverId = crypto.randomUUID();

      await db.query(`
        INSERT INTO media (id, created_by, provider, provider_key, mime, width, height, bytes, sha256, storage_ready, purpose)
        VALUES ($1, $2, 'telegram', 'manual-key', 'image/jpeg', 300, 450, 50000, 'dummy-sha-manual', true, 'editorial');
      `, [manualCoverId, botUserId]);

      await db.query(`
        INSERT INTO works (id, slug, title, cover_id, metadata_provenance)
        VALUES ($1, 'manual-work', 'Manual Work', $2, $3);
      `, [workId, manualCoverId, JSON.stringify({ cover: { source: 'manual', updated_at: new Date().toISOString() } })]);

      const candidate: CandidateWork = {
        source: 'kuro',
        sourceWorkId: 'kw-1',
        title: 'Manual Work',
        slug: 'manual-work',
        coverId: crypto.randomUUID(), // New candidate cover
      };

      await deduplication.applyMetadataPrecedence(workId, candidate, 'kuro');

      const { data: updatedWork } = await supabaseMock.from('works').select('cover_id').eq('id', workId).single();
      expect(updatedWork.cover_id).toBe(manualCoverId);
    });

    it('preserves existing HEALTHY CANONICAL cover and rejects overwrite from secondary source', async () => {
      const workId = crypto.randomUUID();
      const healthyCoverId = crypto.randomUUID();

      await db.query(`
        INSERT INTO media (id, created_by, provider, provider_key, mime, width, height, bytes, sha256, storage_ready, purpose)
        VALUES ($1, $2, 'telegram', 'healthy-key', 'image/jpeg', 400, 600, 80000, 'dummy-sha-healthy', true, 'editorial');
      `, [healthyCoverId, botUserId]);

      await db.query(`
        INSERT INTO works (id, slug, title, cover_id, metadata_provenance)
        VALUES ($1, 'healthy-work', 'Healthy Work', $2, $3);
      `, [workId, healthyCoverId, JSON.stringify({ cover: { source: 'mangadex', updated_at: new Date().toISOString() } })]);

      const candidate: CandidateWork = {
        source: 'mangaflix',
        sourceWorkId: 'mf-1',
        title: 'Healthy Work',
        slug: 'healthy-work',
        coverId: crypto.randomUUID(),
      };

      await deduplication.applyMetadataPrecedence(workId, candidate, 'mangaflix');

      const { data: updatedWork } = await supabaseMock.from('works').select('cover_id').eq('id', workId).single();
      expect(updatedWork.cover_id).toBe(healthyCoverId);
    });

    it('adopts valid candidate cover when work has NULL cover_id', async () => {
      const workId = crypto.randomUUID();
      const newCoverId = crypto.randomUUID();

      await db.query(`
        INSERT INTO media (id, created_by, provider, provider_key, mime, width, height, bytes, sha256, storage_ready, purpose)
        VALUES ($1, $2, 'telegram', 'new-key', 'image/jpeg', 300, 450, 40000, 'dummy-sha-new', true, 'editorial');
      `, [newCoverId, botUserId]);

      await db.query(`
        INSERT INTO works (id, slug, title, cover_id)
        VALUES ($1, 'null-cover-work', 'Null Cover Work', NULL);
      `, [workId]);

      const candidate: CandidateWork = {
        source: 'mangaflix',
        sourceWorkId: 'mf-2',
        title: 'Null Cover Work',
        slug: 'null-cover-work',
        coverId: newCoverId,
      };

      await deduplication.applyMetadataPrecedence(workId, candidate, 'mangaflix');

      const { data: updatedWork } = await supabaseMock.from('works').select('cover_id, metadata_provenance').eq('id', workId).single();
      expect(updatedWork.cover_id).toBe(newCoverId);
      expect(updatedWork.metadata_provenance?.cover?.source).toBe('mangaflix');
    });

    it('replaces BROKEN cover (storage_ready=false) with valid candidate cover', async () => {
      const workId = crypto.randomUUID();
      const brokenCoverId = crypto.randomUUID();
      const repairCoverId = crypto.randomUUID();

      await db.query(`
        INSERT INTO media (id, created_by, provider, provider_key, mime, width, height, bytes, sha256, storage_ready, purpose)
        VALUES ($1, $2, 'telegram', 'broken-key', 'image/jpeg', 10, 10, 100, 'dummy-sha-broken', false, 'editorial');
      `, [brokenCoverId, botUserId]);

      await db.query(`
        INSERT INTO media (id, created_by, provider, provider_key, mime, width, height, bytes, sha256, storage_ready, purpose)
        VALUES ($1, $2, 'telegram', 'repair-key', 'image/jpeg', 350, 500, 60000, 'dummy-sha-repair', true, 'editorial');
      `, [repairCoverId, botUserId]);

      await db.query(`
        INSERT INTO works (id, slug, title, cover_id)
        VALUES ($1, 'broken-cover-work', 'Broken Cover Work', $2);
      `, [workId, brokenCoverId]);

      const candidate: CandidateWork = {
        source: 'hipercool',
        sourceWorkId: 'hc-1',
        title: 'Broken Cover Work',
        slug: 'broken-cover-work',
        coverId: repairCoverId,
      };

      await deduplication.applyMetadataPrecedence(workId, candidate, 'hipercool');

      const { data: updatedWork } = await supabaseMock.from('works').select('cover_id').eq('id', workId).single();
      expect(updatedWork.cover_id).toBe(repairCoverId);
    });

    it('never destroys existing canonical cover when incoming candidate has NULL coverId', async () => {
      const workId = crypto.randomUUID();
      const existingCoverId = crypto.randomUUID();

      await db.query(`
        INSERT INTO media (id, created_by, provider, provider_key, mime, width, height, bytes, sha256, storage_ready, purpose)
        VALUES ($1, $2, 'telegram', 'exist-key', 'image/jpeg', 300, 450, 45000, 'dummy-sha-exist', true, 'editorial');
      `, [existingCoverId, botUserId]);

      await db.query(`
        INSERT INTO works (id, slug, title, cover_id)
        VALUES ($1, 'preserve-work', 'Preserve Work', $2);
      `, [workId, existingCoverId]);

      const candidate: CandidateWork = {
        source: 'kuro',
        sourceWorkId: 'kw-null',
        title: 'Preserve Work',
        slug: 'preserve-work',
        coverId: null, // Null cover from sync
      };

      await deduplication.applyMetadataPrecedence(workId, candidate, 'kuro');

      const { data: updatedWork } = await supabaseMock.from('works').select('cover_id').eq('id', workId).single();
      expect(updatedWork.cover_id).toBe(existingCoverId);
    });
  });

  describe('Multi-Source & Metadata Fallbacks', () => {
    it('recovers cover from raw metadata fallback when primary coverUrl fails', async () => {
      const validJpeg = createValidJpeg(250, 375, 2500);
      const posterUrl = 'https://mocksource.com/posters/fallback-poster.jpg';

      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input: any) => {
        const u = String(input);
        if (u === posterUrl) {
          return new Response(validJpeg, { status: 200, headers: { 'Content-Type': 'image/jpeg' } });
        }
        return new Response('Blocked', { status: 403 });
      });

      const rawMetadata = {
        poster: { default_url: posterUrl },
      };

      const mediaId = await engine.tryRawMetadataCoverFallback(
        rawMetadata,
        botUserId,
        'mocksource',
        'https://mocksource.com/covers/failed.jpg'
      );

      expect(mediaId).toBeTruthy();

      const { data: media } = await supabaseMock.from('media').select('*').eq('id', mediaId).single();
      expect(media.storage_ready).toBe(true);
      expect(media.width).toBe(250);
      expect(media.height).toBe(375);

      fetchSpy.mockRestore();
    });

    it('ensures work cover before chapter publish and repairs missing cover', async () => {
      const workId = crypto.randomUUID();
      const validJpeg = createValidJpeg(320, 480, 3200);
      const siblingCoverUrl = 'https://mocksource.com/covers/sibling.jpg';

      await db.query(`
        INSERT INTO works (id, slug, title, cover_id)
        VALUES ($1, 'chapter-work', 'Chapter Work', NULL);
      `, [workId]);

      await db.query(`
        INSERT INTO importer_work_mappings (id, work_id, source, source_work_id, source_slug, source_title, metadata)
        VALUES (gen_random_uuid(), $1, 'mocksource', 'sw-sibling', 'chapter-work', 'Chapter Work', $2);
      `, [workId, JSON.stringify({ poster: { default_url: siblingCoverUrl } })]);

      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input: any) => {
        const u = String(input);
        if (u === siblingCoverUrl) {
          return new Response(validJpeg, { status: 200, headers: { 'Content-Type': 'image/jpeg' } });
        }
        return new Response('Not found', { status: 404 });
      });

      const repairedCoverId = await engine.ensureWorkHasCover(workId, botUserId);
      expect(repairedCoverId).toBeTruthy();

      const { data: workAfter } = await supabaseMock.from('works').select('cover_id').eq('id', workId).single();
      expect(workAfter.cover_id).toBe(repairedCoverId);

      fetchSpy.mockRestore();
    });
  });
});
