import { describe, expect, it } from 'vitest';
import {
  InMemoryP0CyclePersistence,
  P0CycleCoordinator,
  P2_FROZEN_CANONICAL_WORK_COUNT,
  SourceCatalogMatcher,
  normalizeCatalogTitle,
} from '../p0-cycle-coordinator.js';

const works = [
  { id: 'work-1', title: 'Solo Leveling', aliases: ['I Alone Level Up'] },
  { id: 'work-2', title: 'The Beginning After the End', aliases: ['TBATE'] },
];

describe('P0CycleCoordinator', () => {
  it('persiste início, progresso, conclusão e métricas do ciclo', async () => {
    let current = new Date('2026-11-01T12:00:00.000Z');
    const persistence = new InMemoryP0CyclePersistence(works);
    const coordinator = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence,
      now: () => current,
    });

    expect(coordinator.getCycleMetrics()).toMatchObject({ status: 'IDLE', cycleId: null });
    const started = await coordinator.startCycle();
    expect(started).toMatchObject({
      status: 'RUNNING',
      eligibleWorks: 10_749,
      canonicalWorks: P2_FROZEN_CANONICAL_WORK_COUNT,
      sourcesProcessed: 0,
    });

    await coordinator.recordSourceCatalog('source-a', [
      { sourceWorkId: 'a-1', title: 'Solo Leveling' },
      { sourceWorkId: 'a-2', title: 'The Beginning After the End' },
    ]);
    const afterMatch = await coordinator.matchCandidateWorks();
    expect(afterMatch).toHaveLength(2);

    current = new Date('2026-11-01T14:30:00.000Z');
    const completed = await coordinator.completeCycle();
    expect(completed).toMatchObject({
      status: 'COMPLETED',
      sourcesProcessed: 1,
      catalogItemsSeen: 2,
      worksVerified: 2,
      candidatesMatched: 2,
      newMappingsDiscovered: 2,
      durationMs: 9_000_000,
    });

    const persisted = await persistence.loadLatestCycle();
    expect(persisted?.status).toBe('COMPLETED');
    expect(persisted?.metrics.cycleId).toBe(started.cycleId);
  });

  it('faz matching exato e normalizado, removendo pontuação e acentos', () => {
    const matcher = new SourceCatalogMatcher([
      { id: 'work-1', title: 'Café & Demônios', aliases: ['Coffee Demons'] },
    ]);

    expect(normalizeCatalogTitle('Café & Demônios!')).toBe('cafe demonios');
    expect(matcher.match({ sourceWorkId: '1', title: 'Cafe-Demonios' })?.work.id).toBe('work-1');
    expect(matcher.match({ sourceWorkId: '2', title: 'Coffee Demons' })?.method).toBe('alias');
  });

  it('detecta o mapeamento de catálogo sem criar nenhuma obra em works', async () => {
    const persistence = new InMemoryP0CyclePersistence(works);
    const coordinator = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence,
    });
    await coordinator.startCycle();

    await coordinator.recordSourceCatalog('source-b', [
      { sourceWorkId: 'known', title: 'Solo Leveling', slug: 'solo-leveling' },
      { sourceWorkId: 'unknown', title: 'Obra que não existe no cânone' },
    ]);
    const matches = await coordinator.matchCandidateWorks();

    expect(matches.map((match) => match.work.id)).toEqual(['work-1']);
    expect([...persistence.mappings.values()].map((mapping) => mapping.work.id)).toEqual(['work-1']);
    expect(persistence.existingWorkIds).toEqual(new Set(['work-1', 'work-2']));
    expect(persistence.mappings.has('source-b:unknown')).toBe(false);
  });
});
