import { describe, expect, it } from 'vitest';
import { CANONICAL_FRONTIER_CLAIM_FILTER } from '../scheduler/work-affinity-scheduler.js';

describe('Canonical Claim Filters', () => {
  it('prevents claiming a later frontier while an earlier canonical chapter is queued or importing', () => {
    // The filter must check for predecessor in importer_queue
    expect(CANONICAL_FRONTIER_CLAIM_FILTER).toContain('FROM importer_queue predecessor');
    // Must assert predecessor is strictly before the candidate chapter
    expect(CANONICAL_FRONTIER_CLAIM_FILTER).toContain('predecessor.chapter_sort_key < q.chapter_sort_key');
    // Must check queued/importing/retry states indicating upstream is still evaluating
    expect(CANONICAL_FRONTIER_CLAIM_FILTER).toContain("predecessor.status IN ('QUEUED', 'RETRY', 'IMPORTING')");
    // Must ensure we do not block if the predecessor is already canonically published
    expect(CANONICAL_FRONTIER_CLAIM_FILTER).toContain('FROM chapters predecessor_canonical');
  });

  it('explicitly covers the UPSTREAM_BLOCKED case for watchdogs and scheduler', () => {
    // Se o predecessor está QUEUED, mas em uma fonte com status UPSTREAM_BLOCKED (que não foi recuperada),
    // o predecessor fica parado no banco (não é elegível).
    // O capítulo posterior (q) pode estar em outra fonte ACTIVE e portanto passaria pelo SOURCE_EXECUTION_ELIGIBILITY_SQL
    // e seria considerado "claimable".
    // No entanto, o CANONICAL_FRONTIER_CLAIM_FILTER busca por "predecessor.status IN ('QUEUED', 'RETRY', 'IMPORTING')"
    // e NÃO filtra o status da fonte do predecessor.
    // Isso garante que o capítulo posterior NÃO seja contado como executável enquanto o predecessor canônico
    // estiver bloqueado (UPSTREAM_BLOCKED) na fila.
    const doesNotBypassBlockedSources = CANONICAL_FRONTIER_CLAIM_FILTER.includes('predecessor.source') === false;
    expect(doesNotBypassBlockedSources).toBe(true);

    // Assegurar que a checagem não é contornada por restrições de fonte
    expect(CANONICAL_FRONTIER_CLAIM_FILTER).not.toContain('s.status');
    expect(CANONICAL_FRONTIER_CLAIM_FILTER).not.toMatch(/importer_sources/);
  });
});
