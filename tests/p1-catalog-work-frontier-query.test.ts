import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const source = readFileSync(
  new URL('../src/core/scheduler/work-affinity-scheduler.ts', import.meta.url),
  'utf8',
);

describe('P1 catalog work-frontier selector', () => {
  it('reduces a source to its earliest unpublished row per work before the bounded result cap', () => {
    const method = source.slice(
      source.indexOf('private async executeClaimCatalogQuery'),
      source.indexOf('private async claimCatalogP1Job'),
    );
    const frontier = method.indexOf('source_work_frontiers AS MATERIALIZED');
    const canonicalFence = method.indexOf('${CANONICAL_FRONTIER_CLAIM_FILTER}');
    const cap = method.indexOf('LIMIT $6');

    expect(frontier).toBeGreaterThan(-1);
    expect(method).toContain("SELECT DISTINCT ON (q.payload->>'workId') q.*");
    expect(canonicalFence).toBeGreaterThan(frontier);
    expect(canonicalFence).toBeLessThan(cap);
  });
});
