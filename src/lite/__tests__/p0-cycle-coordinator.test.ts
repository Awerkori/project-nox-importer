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
  it.each([10_000, 15_000, 20_000, 30_000])(
    'aceita escalonamento dinâmico para %i obras canônicas', async (canonicalWorkCount) => {
      const coordinator = new P0CycleCoordinator({
        matcher: new SourceCatalogMatcher(works),
        canonicalWorkCount,
      });

      await expect(coordinator.startCycle()).resolves.toMatchObject({ canonicalWorks: canonicalWorkCount });
    },
  );

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

  it('com P2 suspenso aceita novas fontes para obras existentes e rejeita criação de obras', async () => {
    let suspended = true;
    const persistence = new InMemoryP0CyclePersistence(works);
    const coordinator = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence,
      p2AdmissionPolicy: { isSuspended: () => suspended },
    });

    await coordinator.startCycle();
    expect(coordinator.isP2Suspended()).toBe(true);
    expect(coordinator.canCreateCanonicalWork()).toBe(false);
    expect(() => coordinator.assertCanonicalWorkCreationAllowed()).toThrow(/suspended/);

    await coordinator.recordSourceCatalog('new-source', [
      { sourceWorkId: 'known-while-suspended', title: 'Solo Leveling' },
      { sourceWorkId: 'new-work-while-suspended', title: 'Nunca esteve no cânone' },
    ]);
    await coordinator.matchCandidateWorks();

    expect(persistence.mappings.has('new-source:known-while-suspended')).toBe(true);
    expect(persistence.mappings.has('new-source:new-work-while-suspended')).toBe(false);
    expect(persistence.existingWorkIds).toEqual(new Set(['work-1', 'work-2']));

    suspended = false;
    expect(coordinator.canCreateCanonicalWork()).toBe(true);
  });

  it('mantém ciclos contínuos, histórico acumulado e recuperação após reinicialização', async () => {
    let current = new Date('2026-11-01T00:00:00.000Z');
    const persistence = new InMemoryP0CyclePersistence(works);
    const first = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence,
      now: () => current,
    });

    const firstStarted = await first.resumeOrCreateCycle();
    await first.recordSourceCatalog('source-a', [{ sourceWorkId: 'a-1', title: 'Solo Leveling' }]);
    await first.matchCandidateWorks();
    current = new Date('2026-11-01T01:00:00.000Z');
    await first.completeCycle();

    const second = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence,
      now: () => current,
    });
    const secondStarted = await second.resumeOrCreateCycle();
    expect(secondStarted.cycleId).not.toBe(firstStarted.cycleId);
    expect(second.getAccumulatedMetrics()).toMatchObject({ cyclesCompleted: 1, newMappingsDiscovered: 1 });
    expect(second.getCycleHistory()).toHaveLength(1);

    await second.recordSourceCatalog('source-b', [{ sourceWorkId: 'b-1', title: 'The Beginning After the End' }]);
    const rebooted = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence,
      now: () => current,
    });
    const resumed = await rebooted.resumeOrCreateCycle();
    expect(resumed.cycleId).toBe(secondStarted.cycleId);
    expect(resumed.status).toBe('RUNNING');
    expect(rebooted.getCycleMetrics().sourcesProcessed).toBe(1);
  });

  it('audita a matriz de cobertura e revalida NOT_FOUND após o TTL', async () => {
    let current = new Date('2026-11-01T00:00:00.000Z');
    const persistence = new InMemoryP0CyclePersistence(works);
    const coordinator = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence,
      now: () => current,
      negativeCacheTtlMs: 24 * 60 * 60 * 1000,
    });

    await coordinator.startCycle();
    await coordinator.recordSourceCatalog('source-c', [{ sourceWorkId: 'unknown', title: 'Ainda não existe' }]);
    expect(coordinator.getCoverageMatrix('source-c')).toEqual(expect.arrayContaining([
      expect.objectContaining({ workId: 'work-1', state: 'PENDING' }),
      expect.objectContaining({ workId: 'work-2', state: 'PENDING' }),
    ]));
    await coordinator.matchCandidateWorks();
    const notFound = coordinator.getCoverageMatrix('source-c').find((entry) => entry.workId === 'work-1');
    expect(notFound).toMatchObject({ state: 'NOT_FOUND' });
    const attemptsBeforeCacheHit = notFound?.attempts;

    current = new Date('2026-11-01T23:00:00.000Z');
    await coordinator.recordSourceCatalog('source-c', [{ sourceWorkId: 'unknown', title: 'Ainda não existe' }]);
    await coordinator.matchCandidateWorks();
    expect(coordinator.getCoverageMatrix('source-c').find((entry) => entry.workId === 'work-1')?.attempts)
      .toBe(attemptsBeforeCacheHit);

    current = new Date('2026-11-02T01:00:00.000Z');
    await coordinator.recordSourceCatalog('source-c', [
      { sourceWorkId: 'known-after-revalidation', title: 'Solo Leveling' },
    ]);
    expect(coordinator.getCoverageMatrix('source-c').find((entry) => entry.workId === 'work-1')?.state)
      .toBe('STALE');
    await coordinator.matchCandidateWorks();
    expect(coordinator.getCoverageMatrix('source-c')).toEqual(expect.arrayContaining([
      expect.objectContaining({ workId: 'work-1', state: 'MAPPED' }),
      expect.objectContaining({ workId: 'work-2', state: 'NOT_FOUND' }),
    ]));

    await expect(coordinator.setCoverageState('source-c', 'work-2', 'FAILED', 'timeout'))
      .resolves.toMatchObject({ state: 'FAILED', error: 'timeout' });
    await expect(coordinator.setCoverageState('source-c', 'work-2', 'DEFERRED'))
      .resolves.toMatchObject({ state: 'DEFERRED' });
    await expect(coordinator.setCoverageState('source-c', 'work-2', 'STALE'))
      .resolves.toMatchObject({ state: 'STALE' });
  });

  it('dispara onNewMappingDiscovered somente para novos vínculos', async () => {
    const discovered: string[] = [];
    const persistence = new InMemoryP0CyclePersistence(works);
    const coordinator = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence,
      onNewMappingDiscovered: async (mapping) => {
        discovered.push(`${mapping.source}:${mapping.work.id}`);
      },
    });

    await coordinator.startCycle();
    await coordinator.recordSourceCatalog('source-d', [
      { sourceWorkId: 'd-1', title: 'Solo Leveling' },
      { sourceWorkId: 'd-2', title: 'The Beginning After the End' },
    ]);
    await coordinator.matchCandidateWorks();
    await coordinator.matchCandidateWorks();

    expect(discovered).toEqual(['source-d:work-1', 'source-d:work-2']);
  });
});
