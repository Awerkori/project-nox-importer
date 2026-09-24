import { describe, it, expect, vi } from 'vitest';
import {
  isTransientError,
  validatePermanentGapCandidate,
  markPermanentGapSafely,
} from '../src/core/gap-validator.js';

describe('Project Nox — Gap Safety & Permanent Absence Invariant Tests', () => {
  describe('Transient Error Classifier', () => {
    it('prohibits 403 Forbidden as permanent gap', () => {
      expect(isTransientError(403)).toBe(true);
      expect(isTransientError('403 Forbidden')).toBe(true);
      expect(isTransientError('HTTP Error 403: Cloudflare access denied')).toBe(true);
    });

    it('prohibits 429 Too Many Requests / FloodWait as permanent gap', () => {
      expect(isTransientError(429)).toBe(true);
      expect(isTransientError('429 Too Many Requests')).toBe(true);
      expect(isTransientError('FloodWait 42s')).toBe(true);
      expect(isTransientError('rate limit exceeded')).toBe(true);
    });

    it('prohibits 5xx transient server errors as permanent gap', () => {
      expect(isTransientError(500)).toBe(true);
      expect(isTransientError(502)).toBe(true);
      expect(isTransientError(503)).toBe(true);
      expect(isTransientError(504)).toBe(true);
      expect(isTransientError(520)).toBe(true);
      expect(isTransientError(521)).toBe(true);
      expect(isTransientError(522)).toBe(true);
      expect(isTransientError(524)).toBe(true);
      expect(isTransientError('Bad Gateway 502')).toBe(true);
      expect(isTransientError('Service Unavailable 503')).toBe(true);
    });

    it('prohibits Cloudflare / Turnstile challenges as permanent gap', () => {
      expect(isTransientError('Cloudflare challenge active')).toBe(true);
      expect(isTransientError('Turnstile verification required')).toBe(true);
      expect(isTransientError('Just a moment...')).toBe(true);
      expect(isTransientError('Captcha challenge')).toBe(true);
    });

    it('prohibits network timeouts and socket resets as permanent gap', () => {
      expect(isTransientError('ETIMEDOUT')).toBe(true);
      expect(isTransientError('ESOCKETTIMEDOUT')).toBe(true);
      expect(isTransientError('ECONNRESET')).toBe(true);
      expect(isTransientError('ECONNREFUSED')).toBe(true);
      expect(isTransientError('ECONNABORTED')).toBe(true);
      expect(isTransientError('Request timeout after 30000ms')).toBe(true);
    });

    it('prohibits lease expired as permanent gap', () => {
      expect(isTransientError('[LEASE_EXPIRED] Atomic lease lock expired')).toBe(true);
      expect(isTransientError('lease expiry detected')).toBe(true);
    });

    it('prohibits ASN / shared network blocks as permanent gap', () => {
      expect(isTransientError('Shared network block is currently active on datacenter network')).toBe(true);
      expect(isTransientError('ASN block active')).toBe(true);
    });

    it('prohibits CDN transient errors as permanent gap', () => {
      expect(isTransientError('CDN error: edge cache transient failure')).toBe(true);
      expect(isTransientError('WAF blocked request')).toBe(true);
    });
  });

  describe('validatePermanentGapCandidate Invariant', () => {
    const dummyWorkId = '11111111-2222-3333-4444-555555555555';

    it('strictly classifies 403 as TEMPORARY_UNAVAILABLE (safeToMarkGap === false)', async () => {
      const result = await validatePermanentGapCandidate({
        workId: dummyWorkId,
        chapterNumber: 126,
        chapterSortKey: 126,
        source: 'mangalivreto',
        httpStatus: 403,
        errorMessage: 'HTTP 403: Forbidden by Cloudflare WAF',
      });

      expect(result.isPermanentGap).toBe(false);
      expect(result.safeToMarkGap).toBe(false);
      expect(result.classification).toBe('TEMPORARY_UNAVAILABLE');
    });

    it('strictly classifies 429 as TEMPORARY_UNAVAILABLE (safeToMarkGap === false)', async () => {
      const result = await validatePermanentGapCandidate({
        workId: dummyWorkId,
        chapterNumber: 50,
        chapterSortKey: 50,
        source: 'mangaflix',
        httpStatus: 429,
        errorMessage: 'FloodWait 30s',
      });

      expect(result.isPermanentGap).toBe(false);
      expect(result.safeToMarkGap).toBe(false);
      expect(result.classification).toBe('TEMPORARY_UNAVAILABLE');
    });

    it('strictly classifies timeout as TEMPORARY_UNAVAILABLE (safeToMarkGap === false)', async () => {
      const result = await validatePermanentGapCandidate({
        workId: dummyWorkId,
        chapterNumber: 75,
        chapterSortKey: 75,
        source: 'vegitoons',
        errorMessage: 'ETIMEDOUT: Connection timed out',
      });

      expect(result.isPermanentGap).toBe(false);
      expect(result.safeToMarkGap).toBe(false);
      expect(result.classification).toBe('TEMPORARY_UNAVAILABLE');
    });

    it('strictly classifies lease expiry as TEMPORARY_UNAVAILABLE (safeToMarkGap === false)', async () => {
      const result = await validatePermanentGapCandidate({
        workId: dummyWorkId,
        chapterNumber: 3,
        chapterSortKey: 3,
        source: 'hotcabaretscan',
        errorMessage: '[LEASE_EXPIRED] lease lock held too long',
      });

      expect(result.isPermanentGap).toBe(false);
      expect(result.safeToMarkGap).toBe(false);
      expect(result.classification).toBe('TEMPORARY_UNAVAILABLE');
    });

    it('rejects permanent gap even on 404 if an alternative source possesses the chapter', async () => {
      const result = await validatePermanentGapCandidate({
        workId: dummyWorkId,
        chapterNumber: 10,
        chapterSortKey: 10,
        source: 'source_a',
        httpStatus: 404,
        errorMessage: 'Not Found',
        alternativeSources: [
          { source: 'source_b', hasChapter: true },
        ],
      });

      expect(result.isPermanentGap).toBe(false);
      expect(result.safeToMarkGap).toBe(false);
      expect(result.classification).toBe('AVAILABLE_ELSEWHERE');
      expect(result.alternativeSourcesAvailable).toContain('source_b');
    });

    it('rejects permanent gap for single isolated 404 without repeated or upstream catalog proof', async () => {
      const result = await validatePermanentGapCandidate({
          workId: dummyWorkId,
          chapterNumber: 15,
          chapterSortKey: 15,
          source: 'mangaflix',
          httpStatus: 404,
          errorMessage: '404 Chapter Not Found',
          alternativeSources: [],
          // No Proof A or Proof B provided
        });

        expect(result.isPermanentGap).toBe(false);
        expect(result.safeToMarkGap).toBe(false);
        expect(result.classification).toBe('UNVERIFIED');
        expect(result.reason).toContain('Single isolated 404 is insufficient');
      });

      it('permits PERMANENT_GAP with Proof A: chapter confirmed absent from upstream catalog listing', async () => {
        const result = await validatePermanentGapCandidate({
          workId: dummyWorkId,
          chapterNumber: 999,
          chapterSortKey: 999,
          source: 'primary_source',
          httpStatus: 404,
          errorMessage: '404 Chapter Not Found',
          chapterAbsentFromUpstreamCatalog: true,
          alternativeSources: [
            { source: 'secondary_source', hasChapter: false },
          ],
        });

        expect(result.isPermanentGap).toBe(true);
        expect(result.safeToMarkGap).toBe(true);
        expect(result.classification).toBe('PERMANENT_GAP');
        expect(result.reason).toContain('confirmed absent from upstream catalog');
      });

      it('permits PERMANENT_GAP with Proof B: 404 confirmed repeatedly across independent probes', async () => {
        const result = await validatePermanentGapCandidate({
          workId: dummyWorkId,
          chapterNumber: 999,
          chapterSortKey: 999,
          source: 'primary_source',
          httpStatus: 404,
          errorMessage: '404 Chapter Not Found',
          consecutiveNotFoundCount: 2,
          repeatedNotFoundConfirmed: true,
          alternativeSources: [
            { source: 'secondary_source', hasChapter: false },
          ],
        });

        expect(result.isPermanentGap).toBe(true);
        expect(result.safeToMarkGap).toBe(true);
        expect(result.classification).toBe('PERMANENT_GAP');
        expect(result.reason).toContain('confirmed 404 across repeated probes');
      });
    });

  describe('markPermanentGapSafely Authorization Gate', () => {
    it('refuses to execute database mutation when transient error is passed', async () => {
      const mockQuery = vi.fn();
      const mockClient = { query: mockQuery };

      const res = await markPermanentGapSafely(mockClient, {
        workId: 'dummy-work',
        chapterNumber: 126,
        chapterSortKey: 126,
        source: 'mangalivreto',
        httpStatus: 403,
        errorMessage: 'Cloudflare 403',
      });

      expect(res.mutated).toBe(false);
      expect(res.validation.safeToMarkGap).toBe(false);
      expect(mockQuery).not.toHaveBeenCalledWith(
        expect.stringContaining('UPDATE importer_chapter_mappings'),
        expect.anything()
      );
    });

    it('STRICT FAIL-CLOSED: refuses mutation and sets UNVERIFIED when alternative source DB query throws', async () => {
      // Mock client that throws an unexpected database error when checking alternative sources
      const mockQuery = vi.fn().mockImplementation(async (sql: string) => {
        if (sql.includes('importer_work_mappings')) {
          throw new Error('Connection terminated unexpectedly / deadlock');
        }
        return { rows: [] };
      });
      const mockClient = { query: mockQuery };

      const res = await markPermanentGapSafely(mockClient, {
        workId: 'dummy-work',
        chapterNumber: 50,
        chapterSortKey: 50,
        source: 'primary_source',
        httpStatus: 404,
        errorMessage: '404 Not Found',
        chapterAbsentFromUpstreamCatalog: true,
      });

      expect(res.mutated).toBe(false);
      expect(res.validation.safeToMarkGap).toBe(false);
      expect(res.validation.classification).toBe('UNVERIFIED');
      expect(res.validation.reason).toContain('ALTERNATIVE_SOURCE_CHECK_FAILED');
      expect(res.error).toContain('ALTERNATIVE_SOURCE_CHECK_FAILED');
      expect(mockQuery).not.toHaveBeenCalledWith(
        expect.stringContaining('UPDATE importer_chapter_mappings'),
        expect.anything()
      );
    });

    it('refuses mutation when single 404 lacks structural or repeated probe confirmation', async () => {
      const mockQuery = vi.fn().mockImplementation(async (sql: string) => {
        if (sql.includes('importer_work_mappings')) {
          return { rows: [{ source: 'alt_source', has_chapter: false }] };
        }
        return { rows: [] };
      });
      const mockClient = { query: mockQuery };

      const res = await markPermanentGapSafely(mockClient, {
        workId: 'dummy-work',
        chapterNumber: 126,
        chapterSortKey: 126,
        source: 'mangalivreto',
        httpStatus: 404,
        errorMessage: '404 Not Found',
        // single isolated 404: no chapterAbsentFromUpstreamCatalog and consecutiveNotFoundCount = 1
      });

      expect(res.mutated).toBe(false);
      expect(res.validation.safeToMarkGap).toBe(false);
      expect(res.validation.classification).toBe('UNVERIFIED');
      expect(mockQuery).not.toHaveBeenCalledWith(
        expect.stringContaining('UPDATE importer_chapter_mappings'),
        expect.anything()
      );
    });

    it('refuses to execute database mutation when alternative source has chapter in DB', async () => {
      const mockQuery = vi.fn().mockImplementation(async (sql: string) => {
        if (sql.includes('importer_work_mappings')) {
          return {
            rows: [
              { source: 'mangaonlinetv', has_chapter: true }
            ]
          };
        }
        return { rows: [] };
      });
      const mockClient = { query: mockQuery };

      const res = await markPermanentGapSafely(mockClient, {
        workId: 'dummy-work',
        chapterNumber: 126,
        chapterSortKey: 126,
        source: 'mangalivreto',
        httpStatus: 404,
        errorMessage: '404 Not Found',
        chapterAbsentFromUpstreamCatalog: true,
      });

      expect(res.mutated).toBe(false);
      expect(res.validation.safeToMarkGap).toBe(false);
      expect(res.validation.classification).toBe('AVAILABLE_ELSEWHERE');
      expect(mockQuery).not.toHaveBeenCalledWith(
        expect.stringContaining('UPDATE importer_chapter_mappings'),
        expect.anything()
      );
    });

    it('executes database mutation ONLY when structural absence confirmed and safeToMarkGap is true', async () => {
      const mockQuery = vi.fn().mockImplementation(async (sql: string) => {
        if (sql.includes('importer_work_mappings')) {
          return {
            rows: [
              { source: 'alt_source', has_chapter: false }
            ]
          };
        }
        return { rows: [], rowCount: 1 };
      });
      const mockClient = { query: mockQuery };

      const res = await markPermanentGapSafely(mockClient, {
        workId: 'dummy-work',
        chapterNumber: 999,
        chapterSortKey: 999,
        source: 'primary_source',
        httpStatus: 404,
        errorMessage: '404 Not Found on catalog',
        chapterAbsentFromUpstreamCatalog: true,
      });

      expect(res.mutated).toBe(true);
      expect(res.validation.safeToMarkGap).toBe(true);
      expect(mockQuery).toHaveBeenCalledWith(
        expect.stringContaining('UPDATE importer_chapter_mappings'),
        expect.arrayContaining(['dummy-work', 999, 'primary_source'])
      );
    });
  });
});
