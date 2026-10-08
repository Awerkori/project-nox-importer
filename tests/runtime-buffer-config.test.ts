import { describe, expect, it } from 'vitest';
import { resolveBufferBudgetBytes } from '../src/core/engine.js';

describe('runtime buffer configuration', () => {
  it('uses a valid operational buffer budget instead of a historical constant', () => {
    expect(resolveBufferBudgetBytes(String(40 * 1024 * 1024))).toBe(40 * 1024 * 1024);
  });

  it('keeps a safe default for absent or unsafe values', () => {
    const orig = process.env.MAX_BUFFERED_BYTES;
    delete process.env.MAX_BUFFERED_BYTES;
    expect(resolveBufferBudgetBytes()).toBe(256 * 1024 * 1024);
    expect(resolveBufferBudgetBytes('1')).toBe(256 * 1024 * 1024);
    if (orig !== undefined) process.env.MAX_BUFFERED_BYTES = orig;
  });
});
