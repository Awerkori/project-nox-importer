import { describe, expect, it } from 'vitest';
import { resolveChapterClaimConcurrency } from '../src/core/engine.js';

describe('chapter claim concurrency', () => {
  it('lets every bounded execution slot enter claim while the pool bounds SQL itself', () => {
    expect(resolveChapterClaimConcurrency(5, 2)).toBe(5);
    expect(resolveChapterClaimConcurrency(5, 8)).toBe(5);
    expect(resolveChapterClaimConcurrency(1, 2)).toBe(1);
  });

  it('keeps the claim gate safe for malformed or zero configuration', () => {
    expect(resolveChapterClaimConcurrency(0, 0)).toBe(1);
    expect(resolveChapterClaimConcurrency(Number.NaN, Number.NaN)).toBe(1);
  });
});
