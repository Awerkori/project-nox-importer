import { describe, expect, it } from 'vitest';
import { resolveCatalogBackfillCursor } from '../src/core/engine.js';

describe('catalog backfill cursor', () => {
  it('continues an unfinished bootstrap from its durable page cursor', () => {
    expect(resolveCatalogBackfillCursor({
      cursor_value: '128',
      metadata: { catalog_completed: false },
    })).toBe('128');
  });

  it('restarts a completed catalog from its origin rather than a maintenance timestamp', () => {
    expect(resolveCatalogBackfillCursor({
      cursor_value: '2026-10-09T03:16:07.881Z',
      metadata: { catalog_completed: true },
    })).toBeNull();
  });
});
