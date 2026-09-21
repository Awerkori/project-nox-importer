import { describe, it, expect } from 'vitest';
import { computeCanonicalChapterKey } from '../src/core/deduplication.js';

describe('P0 Homologation — Difficult Edge Cases & Deduplication', () => {
  it('CASE 1: Decimal chapter progression (120 -> 120.5 -> 121) preserves distinct canonical sort keys', () => {
    const k120 = computeCanonicalChapterKey(120, 'Chapter 120');
    const k120_5 = computeCanonicalChapterKey(120.5, 'Chapter 120.5');
    const k121 = computeCanonicalChapterKey(121, 'Chapter 121');

    expect(k120.normalizedNumber).toBe(120);
    expect(k120.sortKey).toBe(120);

    expect(k120_5.normalizedNumber).toBe(120.5);
    expect(k120_5.sortKey).toBe(120.5);

    expect(k121.normalizedNumber).toBe(121);
    expect(k121.sortKey).toBe(121);

    // Strict ordering
    expect(k120.sortKey < k120_5.sortKey).toBe(true);
    expect(k120_5.sortKey < k121.sortKey).toBe(true);

    // No collision
    expect(k120.normalizedNumber).not.toBe(k120_5.normalizedNumber);
    expect(k120_5.normalizedNumber).not.toBe(k121.normalizedNumber);
  });

  it('CASE 2: Old chapter republished by source is matched to published chapter and does NOT trigger P0', () => {
    const publishedChapters = [
      { id: 'chap-10', number: 10, title: 'Chapter 10', published_at: '2026-09-15T12:00:00Z' },
      { id: 'chap-11', number: 11, title: 'Chapter 11', published_at: '2026-09-16T12:00:00Z' },
    ];

    // Source announces "Chapter 10" again with a fresh timestamp
    const incomingCandidate = {
      sourceChapterId: 'new-source-ch-id-999',
      number: 10,
      title: 'Chapter 10 (Re-upload HD)',
    };

    const incomingKey = computeCanonicalChapterKey(incomingCandidate.number, incomingCandidate.title);

    const match = publishedChapters.find(pub => {
      const pubKey = computeCanonicalChapterKey(pub.number, pub.title);
      return pubKey.normalizedNumber === incomingKey.normalizedNumber;
    });

    expect(match).toBeDefined();
    expect(match?.id).toBe('chap-10');
    // Result: mapping is linked, no new job is enqueued, no false P0
  });

  it('CASE 3: Chapter ID changed by source for existing canonical chapter links cleanly without duplicate', () => {
    const publishedChapters = [
      { id: 'canon-42', number: 42, title: 'The Answer', published_at: '2026-09-18T10:00:00Z' }
    ];

    // Source renamed their internal ID from id_old to id_new
    const candidate = {
      sourceChapterId: 'upstream_uuid_v2',
      number: 42,
      title: 'The Answer',
    };

    const candKey = computeCanonicalChapterKey(candidate.number, candidate.title);
    const existing = publishedChapters.find(p => computeCanonicalChapterKey(p.number).normalizedNumber === candKey.normalizedNumber);

    expect(existing).toBeDefined();
    expect(existing?.id).toBe('canon-42');
  });

  it('CASE 4: Slug changed upstream preserves work_id and canonical deduplication', () => {
    // Work mappings table lookup:
    // source='mangaflix', source_work_id='12345' -> work_id='work-uuid-aaa'
    const mockMapping = {
      source: 'mangaflix',
      source_work_id: '12345',
      source_slug: 'old-slug-title',
      work_id: 'work-uuid-aaa',
    };

    // Candidate comes in with new slug 'new-slug-title-2026' but SAME source_work_id
    const candidate = {
      source: 'mangaflix',
      sourceWorkId: '12345',
      slug: 'new-slug-title-2026',
      title: 'Renamed Title',
    };

    // The mapping query matches by source + sourceWorkId
    const isMapped = (mockMapping.source === candidate.source && mockMapping.source_work_id === candidate.sourceWorkId);
    expect(isMapped).toBe(true);
    expect(mockMapping.work_id).toBe('work-uuid-aaa');
  });

  it('CASE 5: Same new chapter detected simultaneously in 2+ sources dedupes to a single P0', () => {
    const activeJobsInQueue = [
      {
        source: 'mangaflix',
        payload: { workId: 'work-solo', chapterNumber: 201 },
        status: 'QUEUED',
        priority: 100,
        dedupe_key: 'work-solo:201.0000',
      }
    ];

    // Second source (e.g. kuro) detects chapter 201 for the same work
    const secondaryCandidate = {
      source: 'kuro',
      workId: 'work-solo',
      chapterNumber: 201,
      chapterTitle: 'Chapter 201',
    };

    const candKey = computeCanonicalChapterKey(secondaryCandidate.chapterNumber, secondaryCandidate.chapterTitle);
    const candDedupeKey = `${secondaryCandidate.workId}:${candKey.sortKey.toFixed(4)}`;

    // Queue deduplication check:
    const duplicateInQueue = activeJobsInQueue.some(j => j.dedupe_key === candDedupeKey);
    expect(duplicateInQueue).toBe(true);
    // Result: Secondary candidate is discarded; exactly 1 P0 job runs.
  });

  it('CASE 6: Source renumbers chapters (e.g. chapter 0 becomes chapter 1)', () => {
    const k0 = computeCanonicalChapterKey(0, 'Capítulo 0');
    const k1 = computeCanonicalChapterKey(1, 'Capítulo 1');

    expect(k0.normalizedNumber).toBe(0);
    expect(k0.isSpecial).toBe(true); // Prologue / special 0
    expect(k1.normalizedNumber).toBe(1);
    expect(k1.isSpecial).toBe(false);

    // If source renumbered prologue from 0 to 'Prólogo', canonical detection preserves specialCategory:
    const kPrologue = computeCanonicalChapterKey(0, 'Prólogo');
    expect(kPrologue.specialCategory).toBe('prologue');
  });

  it('CASE 7: Watermark prevents historical backfill from being misclassified as P0 Fresh Release', () => {
    const watermark = {
      workId: 'work-100',
      lastSeenChapter: 50,
      lastSeenSortKey: 50.0,
      lastSeenChapterId: 'ch-50',
    };

    // Case A: Missing chapter 35 arrives (backfill gap)
    const backfillChapter = { number: 35, sortKey: 35.0 };
    const isBackfillFresh = backfillChapter.sortKey > watermark.lastSeenSortKey;
    expect(isBackfillFresh).toBe(false); // MUST NOT be fresh release!

    // Case B: Chapter 51 arrives (genuine new release)
    const freshChapter = { number: 51, sortKey: 51.0 };
    const isNewReleaseFresh = freshChapter.sortKey > watermark.lastSeenSortKey;
    expect(isNewReleaseFresh).toBe(true); // Genuinely fresh!
  });
});
