import { describe, expect, it } from 'vitest';
import { CANONICAL_FRONTIER_CLAIM_FILTER } from '../scheduler/work-affinity-scheduler.js';

describe('Canonical Claim Filters', () => {
  it('prevents claiming a later frontier while an earlier canonical chapter is queued or importing (UPSTREAM_BLOCKED)', () => {
    // The filter must check for predecessor in importer_queue
    expect(CANONICAL_FRONTIER_CLAIM_FILTER).toContain('FROM importer_queue predecessor');
    // Must assert predecessor is strictly before the candidate chapter
    expect(CANONICAL_FRONTIER_CLAIM_FILTER).toContain('predecessor.chapter_sort_key < q.chapter_sort_key');
    // Must check queued/importing/retry states indicating upstream is still evaluating
    expect(CANONICAL_FRONTIER_CLAIM_FILTER).toContain("predecessor.status IN ('QUEUED', 'RETRY', 'IMPORTING')");
    // Must ensure we do not block if the predecessor is already canonically published
    expect(CANONICAL_FRONTIER_CLAIM_FILTER).toContain('FROM chapters predecessor_canonical');
  });
});
