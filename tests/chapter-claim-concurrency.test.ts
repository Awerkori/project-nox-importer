import { describe, expect, it } from 'vitest';
import { resolveChapterClaimConcurrency } from '../src/core/engine.js';

describe('chapter claim concurrency', () => {
  it('bounds the DB-backed claim phase by the safe pool without reducing execution capacity', () => {
    expect(resolveChapterClaimConcurrency(5, 2)).toBe(2);
    expect(resolveChapterClaimConcurrency(5, 8)).toBe(5);
    expect(resolveChapterClaimConcurrency(1, 2)).toBe(1);
  });

  it('keeps the claim gate safe for malformed or zero configuration', () => {
    expect(resolveChapterClaimConcurrency(0, 0)).toBe(1);
    expect(resolveChapterClaimConcurrency(Number.NaN, Number.NaN)).toBe(1);
  });
});
