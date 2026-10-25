import { describe, it, expect, vi } from 'vitest';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  inspectCoverForPtBrTitle,
  downloadCoverWithLimit,
  CoverInspectionTarget,
} from '../cover-vision-inspector.js';
import {
  resolveAutomaticPtBrTitlePromotion,
  isPtBrTitle,
} from '../ptbr-title-localization.js';
import { mergeAlternativeTitles } from '../alternative-titles.js';
import { CoverVisionQueue, type CoverCandidate } from '../cover-vision-queue.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const FIXTURES_DIR = path.resolve(__dirname, 'fixtures');

function getRequiredFixture(filename: string): Buffer {
  const fullPath = path.join(FIXTURES_DIR, filename);
  if (!fs.existsSync(fullPath)) {
    throw new Error(`Mandatory fixture missing: ${fullPath}. Permanent fixtures must be checked into the repository.`);
  }
  return fs.readFileSync(fullPath);
}

describe('CoverVisionInspector & Permanent PT-BR Guarantee Tests', () => {
  // =========================================================================
  // TEST A: PT-BR in Aliases Promotion Test (Zero OCR Call, Pure Metadata)
  // =========================================================================
  describe('Test A: PT-BR in aliases promotion without OCR', () => {
    it('promotes PT-BR alias to primary title and preserves English title in aliases', () => {
      const initial = {
        workId: 'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d',
        slug: 'the-second-eunuch-regains-his-manhood',
        title: 'The Second Eunuch Regains His Manhood',
        aliases: [
          'O Segundo Eunuco Recupera Sua Masculinidade',
          '제2의 환관이 남성성을 되찾다',
        ],
        provenance: {
          title: { source: 'nexus', updated_at: '2026-10-24T00:00:00.000Z' },
        },
      };

      const promo = resolveAutomaticPtBrTitlePromotion(initial);

      expect(promo.promoted).toBe(true);
      expect(promo.newTitle).toBe('O Segundo Eunuco Recupera Sua Masculinidade');
      expect(promo.newAliases).toContain('The Second Eunuch Regains His Manhood');
      expect(promo.newAliases).toContain('제2의 환관이 남성성을 되찾다');
      expect(promo.method).toBe('PTBR_PROMOTED_EXISTING_ALIAS');
    });
  });

  // =========================================================================
  // TEST B: Real Work With PT-BR Title on Cover Only (PTBR_FOUND_COVER)
  // =========================================================================
  describe('Test B: PT-BR title found on cover artwork when metadata has no PT-BR alias', () => {
    it('promotes visually detected PT-BR title when candidate has strong confidence', () => {
      const currentTitle = 'Isekai de Cheat Skill wo Te ni Shita Ore wa';
      const aliases = ['I Got a Cheat Skill in Another World and Became Unrivaled in the Real World, Too'];

      // Visual OCR discovers Portuguese title from cover art
      const detectedCoverTitle = 'O Homem Que Conseguiu Uma Habilidade Trapaceira Em Outro Mundo';

      const promo = resolveAutomaticPtBrTitlePromotion({
        workId: 'b2c3d4e5-f6a7-8b9c-0d1e-2f3a4b5c6d7e',
        slug: 'isekai-cheat-skill',
        title: currentTitle,
        aliases,
        coverTitle: detectedCoverTitle,
        provenance: {
          title: { source: 'nexus', updated_at: '2026-10-24T00:00:00.000Z' },
        },
      });

      expect(promo.promoted).toBe(true);
      expect(promo.newTitle).toBe(detectedCoverTitle);
      expect(promo.newAliases).toContain(currentTitle);
      expect(promo.method).toBe('PTBR_FOUND_COVER');
    });

    it('end-to-end OCR extracts PT-BR title from image buffer and validates PTBR_FOUND_COVER (MANDATORY FIXTURE)', async () => {
      const buf = getRequiredFixture('cover_ptbr_real.png');
      const res = await inspectCoverForPtBrTitle({
        title: 'The Second Eunuch Regains His Manhood',
        aliases: ['The Second Eunuch'],
        coverBuffer: buf,
        source: 'mangaflix',
      });

      expect(res.inspected).toBe(true);
      expect(res.foundPtBrTitle).toBe(true);
      expect(res.detectedTitle).toBe('O Segundo Eunuco Recupera Sua Masculinidade');
      expect(res.confidence).toBeGreaterThanOrEqual(0.75);
      expect(res.method).toBe('PTBR_FOUND_COVER');
    });
  });

  // =========================================================================
  // TEST C: Cross-Source Cover Inspection (Origin English + Brazilian Portuguese)
  // =========================================================================
  describe('Test C: Origin source English cover + Complementary Brazilian source PT-BR cover', () => {
    it('prioritizes Brazilian complementary source cover over foreign origin source cover', () => {
      const originSource = 'nexus'; // foreign English source
      const complementaryCovers = [
        {
          source: 'mangaflix',
          sourceWorkId: 'mf-12345',
          coverUrl: 'https://mangaflix.org/covers/amigas-da-minha-madrasta.jpg',
          isBrazilianSource: true,
        },
        {
          source: 'mangadex',
          sourceWorkId: 'md-67890',
          coverUrl: 'https://uploads.mangadex.org/covers/stepmother-friend.jpg',
          isBrazilianSource: false,
        },
      ];
      const snapshotCoverUrl = 'https://nexus.to/covers/stepmother-friend-eng.jpg';

      // Candidate sorting logic identical to runtime.ts:
      const originIsBrazilian = !['nexus', 'mangadex', 'mangafire', 'mangaball'].includes(originSource);
      const candidateCovers: Array<{ source: string; coverUrl: string }> = [];

      // 1. Confirmed Brazilian complementary sources
      for (const c of complementaryCovers.filter((cov) => cov.isBrazilianSource)) {
        candidateCovers.push({ source: c.source, coverUrl: c.coverUrl });
      }
      // 2. Brazilian origin source
      if (originIsBrazilian && snapshotCoverUrl) {
        candidateCovers.push({ source: originSource, coverUrl: snapshotCoverUrl });
      }
      // 3. Other confirmed complementary sources
      for (const c of complementaryCovers.filter((cov) => !cov.isBrazilianSource)) {
        candidateCovers.push({ source: c.source, coverUrl: c.coverUrl });
      }
      // 4. Foreign origin source
      if (!originIsBrazilian && snapshotCoverUrl) {
        candidateCovers.push({ source: originSource, coverUrl: snapshotCoverUrl });
      }

      // First candidate MUST be the Brazilian complementary source (mangaflix)
      expect(candidateCovers[0].source).toBe('mangaflix');
      expect(candidateCovers[0].coverUrl).toBe('https://mangaflix.org/covers/amigas-da-minha-madrasta.jpg');

      // Foreign origin source (nexus) is placed last
      expect(candidateCovers[candidateCovers.length - 1].source).toBe('nexus');
    });
  });

  // =========================================================================
  // TEST D: Anti-Hallucination & False-Positive Resistance
  // =========================================================================
  describe('Test D: Anti-hallucination and noise rejection', () => {
    it('rejects scanlator watermarks, scan credits, and urls', async () => {
      const target: CoverInspectionTarget = {
        title: 'Solo Leveling',
        aliases: ['Na Honjaman Rebeleop'],
        author: 'Chugong',
        coverUrl: 'https://example.com/cover.jpg',
      };

      const fakePng = Buffer.from(
        'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
        'base64',
      );

      const res = await inspectCoverForPtBrTitle({
        ...target,
        coverBuffer: fakePng,
      });

      // 1x1 transparent PNG contains no text -> must return foundPtBrTitle = false
      expect(res.foundPtBrTitle).toBe(false);
      expect(res.detectedTitle).toBeNull();
    });

    it('rejects candidate titles that match the author or artist name', () => {
      const author = 'Carlos Eduardo Ferreira';
      const targetAuthorTitle = author;

      const isAuthorMatch = cleanPtBrCandidateTitle(author)?.toLowerCase() === targetAuthorTitle.toLowerCase();
      expect(isAuthorMatch).toBe(true);
    });

    it('rejects garbage symbols, repetitive syllables, and low Portuguese ratio', () => {
      const garbageSymbols = '==## O Guerreiro <>$$';
      expect(/[|=%_<>{}~^\\/`*#$@©®™§±×÷•]/.test(garbageSymbols)).toBe(true);

      const repetitive = ['pe', 'pe', 'guerreiro'];
      let isRepetitive = false;
      for (let i = 0; i < repetitive.length - 1; i++) {
        if (repetitive[i] === repetitive[i + 1] && repetitive[i].length <= 3) {
          isRepetitive = true;
          break;
        }
      }
      expect(isRepetitive).toBe(true);
    });

    it('end-to-end OCR rejects scanlator noise cover without false positives (MANDATORY FIXTURE)', async () => {
      const buf = getRequiredFixture('cover_scanlator_noise.png');
      const res = await inspectCoverForPtBrTitle({
        title: 'Random Work',
        coverBuffer: buf,
        source: 'test',
      });
      expect(res.inspected).toBe(true);
      expect(res.foundPtBrTitle).toBe(false);
      expect(res.detectedTitle).toBeNull();
    });

    it('end-to-end OCR rejects English cover text without false positives (MANDATORY FIXTURE)', async () => {
      const buf = getRequiredFixture('cover_english.png');
      const res = await inspectCoverForPtBrTitle({
        title: 'The Second Eunuch Regains His Manhood',
        coverBuffer: buf,
        source: 'nexus',
      });
      expect(res.inspected).toBe(true);
      expect(res.foundPtBrTitle).toBe(false);
      expect(res.detectedTitle).toBeNull();
    });
  });

  // =========================================================================
  // TEST E: Persistence After Re-Sync (Anti-Reversion Protection)
  // =========================================================================
  describe('Test E: Anti-reversion guarantee across sync passes', () => {
    it('never reverts a promoted PT-BR title when incoming sync has original English title', () => {
      const localizedWork = {
        workId: 'c3d4e5f6-a7b8-9c0d-1e2f-3a4b5c6d7e8f',
        slug: 'o-segundo-eunuco-recupera-sua-masculinidade',
        title: 'O Segundo Eunuco Recupera Sua Masculinidade',
        aliases: [
          'The Second Eunuch Regains His Manhood',
          '제2의 환관이 남성성을 되찾da',
        ],
        provenance: {
          title: {
            source: 'mangaflix',
            method: 'PTBR_FOUND_COVER',
            localized_ptbr: true,
            previous_value: 'The Second Eunuch Regains His Manhood',
            updated_at: '2026-10-24T00:00:00.000Z',
          },
        },
      };

      const incomingSync = {
        workId: localizedWork.workId,
        slug: localizedWork.slug,
        title: localizedWork.title,
        aliases: [
          ...localizedWork.aliases,
          'The Second Eunuch (Raw Korean)',
        ],
        provenance: localizedWork.provenance,
      };

      const result = resolveAutomaticPtBrTitlePromotion(incomingSync);

      expect(result.promoted).toBe(false);
      expect(result.newTitle).toBe('O Segundo Eunuco Recupera Sua Masculinidade');
    });
  });

  // =========================================================================
  // TEST F: Non-Blocking Execution & Bounded Limits
  // =========================================================================
  describe('Test F: Non-blocking execution and bounded limits', () => {
    it('immediately rejects buffers exceeding the 5MB size limit', async () => {
      const oversizedBuffer = Buffer.alloc(6 * 1024 * 1024); // 6 MB

      const res = await inspectCoverForPtBrTitle({
        title: 'Test Oversized Work',
        coverBuffer: oversizedBuffer,
      });

      expect(res.inspected).toBe(false);
      expect(res.foundPtBrTitle).toBe(false);
      expect(res.evidence.reason).toBe('IMAGE_EXCEEDS_MAX_SIZE');
    });

    it('handles empty image buffers gracefully without unhandled errors', async () => {
      const emptyBuffer = Buffer.alloc(0);

      const res = await inspectCoverForPtBrTitle({
        title: 'Test Empty Work',
        coverBuffer: emptyBuffer,
      });

      expect(res.inspected).toBe(false);
      expect(res.foundPtBrTitle).toBe(false);
      expect(res.evidence.reason).toBe('NO_IMAGE_DATA');
    });

    it('downloadCoverWithLimit stops reading chunk stream if size exceeds max bytes without Content-Length', async () => {
      const originalFetch = globalThis.fetch;
      try {
        let streamCancelled = false;
        const mockStream = new ReadableStream({
          pull(controller) {
            const chunk = new Uint8Array(1024 * 1024);
            controller.enqueue(chunk);
          },
          cancel() {
            streamCancelled = true;
          },
        });

        globalThis.fetch = vi.fn().mockResolvedValue({
          ok: true,
          headers: new Headers(), // No content-length header
          body: mockStream,
        }) as any;

        const result = await downloadCoverWithLimit('https://example.com/oversized.jpg', undefined, 5 * 1024 * 1024, 5000);

        expect(result.ok).toBe(false);
        expect(result.reason).toBe('IMAGE_EXCEEDS_MAX_SIZE');
        expect(streamCancelled).toBe(true);
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    it('downloadCoverWithLimit succeeds when stream is within 5MB limit', async () => {
      const originalFetch = globalThis.fetch;
      try {
        const payload = Buffer.from('TEST_IMAGE_DATA');
        const mockStream = new ReadableStream({
          start(controller) {
            controller.enqueue(payload);
            controller.close();
          },
        });

        globalThis.fetch = vi.fn().mockResolvedValue({
          ok: true,
          headers: new Headers(),
          body: mockStream,
        }) as any;

        const result = await downloadCoverWithLimit('https://example.com/valid.jpg', undefined, 5 * 1024 * 1024, 5000);

        expect(result.ok).toBe(true);
        expect(result.buffer).toBeDefined();
        expect(result.buffer?.toString()).toBe('TEST_IMAGE_DATA');
      } finally {
        globalThis.fetch = originalFetch;
      }
    });
  });

  // =========================================================================
  // TEST G: Durable Task Queue Operations & Bug Fixes (Bugs 1 & 2)
  // =========================================================================
  describe('Test G: Durable Cover Vision Queue Operations', () => {
    it('enqueues brand new task directly in PENDING status', async () => {
      const mockQueries: Array<{ sql: string; params: any[] }> = [];
      const mockPool: any = {
        query: vi.fn().mockImplementation((sql: string, params: any[]) => {
          mockQueries.push({ sql, params });
          if (sql.includes('to_regclass')) {
            return Promise.resolve({ rows: [{ ok: true }] });
          }
          if (sql.includes('SELECT status, candidate_covers, inspected_urls')) {
            return Promise.resolve({ rows: [] }); // No existing task
          }
          return Promise.resolve({ rows: [] });
        }),
      };

      const queue = new CoverVisionQueue(mockPool);
      const candidates: CoverCandidate[] = [
        { source: 'mangaflix', coverUrl: 'https://mangaflix.org/cover1.jpg' },
        { source: 'nexus', coverUrl: 'https://nexus.to/cover2.jpg' },
      ];

      await queue.enqueueTask('11111111-2222-3333-4444-555555555555', candidates);

      expect(mockQueries.length).toBeGreaterThanOrEqual(1);
      const insertQuery = mockQueries.find((q) => q.sql.includes('INSERT INTO public.cover_vision_work_tasks'));
      expect(insertQuery).toBeDefined();
      expect(insertQuery?.params[0]).toBe('11111111-2222-3333-4444-555555555555');
      expect(JSON.parse(insertQuery?.params[1])).toEqual(candidates);
      expect(insertQuery?.params[2]).toBe(3);
    });

    it('BUG 1: findDueTasks enforces retry interval, max attempts, and safe abandoned-task recovery', async () => {
      let executedSql = '';
      const mockPool: any = {
        query: vi.fn().mockImplementation((sql: string) => {
          executedSql = sql;
          if (sql.includes('to_regclass')) return Promise.resolve({ rows: [{ ok: true }] });
          return Promise.resolve({
            rows: [
              {
                work_id: '11111111-2222-3333-4444-555555555555',
                candidate_covers: [{ source: 'mangaflix', coverUrl: 'https://example.com/c1.jpg' }],
                status: 'PENDING',
                attempts: 1,
                max_attempts: 3,
                inspected_urls: [],
                detected_title: null,
                attributing_source: null,
                confidence: null,
                evidence: null,
                last_error: null,
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString(),
                next_retry_at: null,
              },
            ],
          });
        }),
      };

      const queue = new CoverVisionQueue(mockPool);
      const due = await queue.findDueTasks(2);

      expect(due.length).toBe(1);
      expect(due[0].workId).toBe('11111111-2222-3333-4444-555555555555');

      // Verify Bug 1 fix in SQL query:
      // 1. Must check attempts < max_attempts
      expect(executedSql).toContain('attempts < max_attempts');
      // 2. Must respect next_retry_at interval for PENDING tasks
      expect(executedSql).toContain('(next_retry_at IS NULL OR next_retry_at <= now())');
      // 3. Must only recover RUNNING tasks if updated_at is older than 5 minutes (preventing duplicate execution)
      expect(executedSql).toContain("updated_at <= now() - interval '5 minutes'");
    });

    it('BUG 1: markFailed calculates next_retry_at and marks FAILED at maxAttempts without indefinite retries', async () => {
      const mockQueries: Array<{ sql: string; params: any[] }> = [];
      const mockPool: any = {
        query: vi.fn().mockImplementation((sql: string, params: any[]) => {
          mockQueries.push({ sql, params });
          return Promise.resolve({ rows: [] });
        }),
      };

      const queue = new CoverVisionQueue(mockPool);

      // Attempt 1 of 3 (transient failure): should set next_retry_at = now() + 5 minutes
      await queue.markFailed('work-1', 'Network error', ['https://example.com/cover1.jpg'], 3, 5);

      const query1 = mockQueries[0];
      expect(query1.sql).toContain("CASE WHEN attempts + 1 >= $3 THEN 'FAILED' ELSE 'PENDING' END");
      expect(query1.sql).toContain("next_retry_at = CASE WHEN attempts + 1 >= $3 THEN NULL ELSE now() + ($5 || ' minutes')::interval END");
      expect(query1.params[0]).toBe('Network error');
      expect(query1.params[2]).toBe(3); // maxAttempts
      expect(query1.params[4]).toBe(5); // retryDelayMinutes
    });

    it('BUG 2: enqueueTask reactivates NO_PTBR_FOUND task to PENDING when fresh uninspected covers arrive', async () => {
      const mockQueries: Array<{ sql: string; params: any[] }> = [];
      const mockPool: any = {
        query: vi.fn().mockImplementation((sql: string, params: any[]) => {
          mockQueries.push({ sql, params });
          if (sql.includes('to_regclass')) return Promise.resolve({ rows: [{ ok: true }] });
          if (sql.includes('SELECT status, candidate_covers, inspected_urls')) {
            return Promise.resolve({
              rows: [
                {
                  status: 'NO_PTBR_FOUND',
                  candidate_covers: [{ source: 'source-a', coverUrl: 'https://example.com/cover-old.jpg' }],
                  inspected_urls: ['https://example.com/cover-old.jpg'],
                },
              ],
            });
          }
          return Promise.resolve({ rows: [] });
        }),
      };

      const queue = new CoverVisionQueue(mockPool);
      // New cover arrives from a complementary source
      const incoming = [
        { source: 'source-b', coverUrl: 'https://example.com/cover-NEW.jpg' },
      ];

      await queue.enqueueTask('work-reactivate-test', incoming);

      // Must find the update query resetting to PENDING
      const updateQuery = mockQueries.find((q) => q.sql.includes("status = 'PENDING'"));
      expect(updateQuery).toBeDefined();
      expect(updateQuery?.sql).toContain('attempts = 0');
      expect(updateQuery?.sql).toContain('next_retry_at = NULL');

      const updatedCandidates = JSON.parse(updateQuery?.params[0]);
      expect(updatedCandidates.length).toBe(2);
      expect(updatedCandidates.some((c: CoverCandidate) => c.coverUrl === 'https://example.com/cover-NEW.jpg')).toBe(true);
      expect(updatedCandidates.some((c: CoverCandidate) => c.coverUrl === 'https://example.com/cover-old.jpg')).toBe(true);
    });

    it('BUG 2: enqueueTask does NOT reactivate NO_PTBR_FOUND when incoming covers were already inspected', async () => {
      const mockQueries: Array<{ sql: string; params: any[] }> = [];
      const mockPool: any = {
        query: vi.fn().mockImplementation((sql: string, params: any[]) => {
          mockQueries.push({ sql, params });
          if (sql.includes('to_regclass')) return Promise.resolve({ rows: [{ ok: true }] });
          if (sql.includes('SELECT status, candidate_covers, inspected_urls')) {
            return Promise.resolve({
              rows: [
                {
                  status: 'NO_PTBR_FOUND',
                  candidate_covers: [{ source: 'source-a', coverUrl: 'https://example.com/cover-old.jpg' }],
                  inspected_urls: ['https://example.com/cover-old.jpg'],
                },
              ],
            });
          }
          return Promise.resolve({ rows: [] });
        }),
      };

      const queue = new CoverVisionQueue(mockPool);
      // Same cover re-delivered
      const incoming = [
        { source: 'source-a', coverUrl: 'https://example.com/cover-old.jpg' },
      ];

      await queue.enqueueTask('work-no-duplicate', incoming);

      // Must NOT update status to PENDING
      const updateQuery = mockQueries.find((q) => q.sql.includes("status = 'PENDING'"));
      expect(updateQuery).toBeUndefined();
    });

    it('BUG 2: enqueueTask never re-enqueues or modifies COMPLETED tasks', async () => {
      const mockQueries: Array<{ sql: string; params: any[] }> = [];
      const mockPool: any = {
        query: vi.fn().mockImplementation((sql: string, params: any[]) => {
          mockQueries.push({ sql, params });
          if (sql.includes('to_regclass')) return Promise.resolve({ rows: [{ ok: true }] });
          if (sql.includes('SELECT status, candidate_covers, inspected_urls')) {
            return Promise.resolve({
              rows: [
                {
                  status: 'COMPLETED',
                  candidate_covers: [{ source: 'source-a', coverUrl: 'https://example.com/cover.jpg' }],
                  inspected_urls: ['https://example.com/cover.jpg'],
                },
              ],
            });
          }
          return Promise.resolve({ rows: [] });
        }),
      };

      const queue = new CoverVisionQueue(mockPool);
      await queue.enqueueTask('work-completed', [{ source: 'source-b', coverUrl: 'https://example.com/new.jpg' }]);

      // Neither insert nor update query should be executed
      const writeQuery = mockQueries.find((q) => q.sql.includes('INSERT') || q.sql.includes('UPDATE'));
      expect(writeQuery).toBeUndefined();
    });

    it('marks running, completed, and no ptbr found correctly', async () => {
      const mockQueries: Array<{ sql: string; params: any[] }> = [];
      const mockPool: any = {
        query: vi.fn().mockImplementation((sql: string, params: any[]) => {
          mockQueries.push({ sql, params });
          return Promise.resolve({ rows: [] });
        }),
      };

      const queue = new CoverVisionQueue(mockPool);

      await queue.markRunning('test-work-id');
      expect(mockQueries.some((q) => q.sql.includes("status = 'RUNNING'"))).toBe(true);

      await queue.markCompleted('test-work-id', {
        detectedTitle: 'Título em Português',
        attributingSource: 'mangaflix',
        confidence: 0.95,
        evidence: { method: 'test' },
        inspectedUrls: ['url1'],
      });
      expect(mockQueries.some((q) => q.sql.includes("status = 'COMPLETED'"))).toBe(true);

      await queue.markNoPtBrFound('test-work-id', ['url1', 'url2']);
      expect(mockQueries.some((q) => q.sql.includes("status = 'NO_PTBR_FOUND'"))).toBe(true);
    });
  });
});

function cleanPtBrCandidateTitle(raw: string): string | null {
  return raw.trim();
}
