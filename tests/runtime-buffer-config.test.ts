import { describe, expect, it } from 'vitest';
import { resolveBufferBudgetBytes } from '../src/core/engine.js';

describe('runtime buffer configuration', () => {
  it('uses a valid operational buffer budget instead of a historical constant', () => {
    expect(resolveBufferBudgetBytes(String(40 * 1024 * 1024))).toBe(40 * 1024 * 1024);
  });

  it('keeps a safe default for absent or unsafe values', () => {
    expect(resolveBufferBudgetBytes()).toBe(64 * 1024 * 1024);
    expect(resolveBufferBudgetBytes('1')).toBe(64 * 1024 * 1024);
  });
});
