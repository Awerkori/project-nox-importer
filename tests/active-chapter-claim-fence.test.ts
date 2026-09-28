import { describe, expect, it } from 'vitest';
import { isActiveChapterClaimConflict } from '../src/core/scheduler/work-affinity-scheduler';

describe('active canonical chapter claim fence', () => {
  it('turns the expected partial-index race into a harmless no-claim', () => {
    expect(isActiveChapterClaimConflict({
      code: '23505',
      constraint: 'idx_importer_queue_one_importing_canonical_chapter'
    })).toBe(true);
  });

  it('does not hide unrelated database integrity failures', () => {
    expect(isActiveChapterClaimConflict({ code: '23505', constraint: 'importer_queue_dedupe_key_key' })).toBe(false);
    expect(isActiveChapterClaimConflict({ code: '40001' })).toBe(false);
  });
});
