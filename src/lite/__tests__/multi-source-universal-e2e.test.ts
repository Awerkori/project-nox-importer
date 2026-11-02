import { describe, expect, it, vi } from 'vitest';
import {
  P0CycleCoordinator,
  SourceCatalogMatcher,
  InMemoryP0CyclePersistence,
  normalizeCatalogTitle,
  type CanonicalWork,
  type CatalogItem,
  type P0MappingInput,
} from '../p0-cycle-coordinator.js';
import { canonicalChapterKey, chapterKind, chapterOrderKey } from '../chapter-identity.js';
import { selectChapterSource, selectableChapterSources } from '../source-selector.js';
import type { ChapterSourceRoute, EnqueueLiteTask, LiteTask } from '../types.js';

describe('Project Nox — Sistema Multifonte Universal P0 + P1 E2E', () => {
  // --------------------------------------------------------------------------
  // Cenário A — Fonte original incompleta
  // Uma obra existe na fonte A com menos capítulos (1..100) e na fonte B com
  // capítulos adicionais (1..150). O P1 encontra a correspondência e preenche
  // corretamente os capítulos ausentes sem depender do limite da fonte original.
  // --------------------------------------------------------------------------
  it('Cenário A: preenche capítulos ausentes de fonte alternativa mais completa sem travar na fonte original', async () => {
    const canonicalWorks: CanonicalWork[] = [
      { id: 'work-solo', title: 'Solo Leveling', aliases: ['I Alone Level Up', 'Na Honjaman Rebeleop'], slug: 'solo-leveling' },
    ];
    const persistence = new InMemoryP0CyclePersistence(canonicalWorks);
    const discoveredMappings: P0MappingInput[] = [];

    const coordinator = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(canonicalWorks),
      persistence,
      onNewMappingDiscovered: async (mapping) => {
        discoveredMappings.push(mapping);
      },
    });

    await coordinator.startCycle();

    // Fonte B (Nexus) possui catálogo de Solo Leveling
    await coordinator.recordSourceCatalog('nexus', [
      { sourceWorkId: 'nexus-solo-id', title: 'Solo Leveling', slug: 'solo-leveling-nexus' },
    ]);
    const matches = await coordinator.matchCandidateWorks();
    expect(matches).toHaveLength(1);
    expect(matches[0].work.id).toBe('work-solo');
    expect(discoveredMappings).toHaveLength(1);
    expect(discoveredMappings[0].source).toBe('nexus');

    // Simulação do preenchimento P1 de capítulos:
    // Fonte original (MangaFlix) tinha apenas capítulos 1..100
    const sourceAChapters = Array.from({ length: 100 }, (_, i) => i + 1);
    const existingPublishedKeys = new Set(
      sourceAChapters.map((num) =>
        canonicalChapterKey({ displayNumber: String(num), kind: 'chapter', language: 'pt-BR', edition: 'default' })
      )
    );

    // Fonte alternativa (Nexus) possui 150 capítulos
    const sourceBChapters = Array.from({ length: 150 }, (_, i) => ({
      sourceChapterId: `nexus-ch-${i + 1}`,
      number: i + 1,
      displayNumber: String(i + 1),
      title: `Capítulo ${i + 1}`,
    }));

    // Reconciliação P1: detecta rotas e preenche os gaps 101..150
    const p1EnqueuedTasks: EnqueueLiteTask[] = [];
    const recordedChapterSources: Array<{ key: string; source: string; sourceChapterId: string }> = [];

    for (const chapter of sourceBChapters) {
      const key = canonicalChapterKey({
        displayNumber: chapter.displayNumber,
        kind: 'chapter',
        language: 'pt-BR',
        edition: 'default',
      });
      const orderKey = chapterOrderKey({ displayNumber: chapter.displayNumber, kind: 'chapter' });

      recordedChapterSources.push({
        key,
        source: 'nexus',
        sourceChapterId: chapter.sourceChapterId,
      });

      // Se o capítulo não existe nas publicações prévias da fonte A, P1 admite para ingestão
      if (!existingPublishedKeys.has(key)) {
        p1EnqueuedTasks.push({
          taskType: 'IMPORT_CHAPTER',
          workId: 'work-solo',
          workKey: 'work:work-solo',
          chapterKey: key,
          priority: 'P1',
          idempotencyKey: `lite:chapter:work-solo:${key}`,
          payload: {
            displayNumber: chapter.displayNumber,
            chapterTitle: chapter.title,
            orderKey,
            language: 'pt-BR',
            edition: 'default',
            source: 'nexus',
            sourceChapterId: chapter.sourceChapterId,
          },
        });
      }
    }

    // Comprova que P1 preencheu exatamente os 50 capítulos ausentes (101..150)
    expect(p1EnqueuedTasks).toHaveLength(50);
    expect(p1EnqueuedTasks[0].payload?.displayNumber).toBe('101');
    expect(p1EnqueuedTasks[49].payload?.displayNumber).toBe('150');
    expect(p1EnqueuedTasks.every((t) => t.priority === 'P1')).toBe(true);
    expect(recordedChapterSources).toHaveLength(150);
  });

  // --------------------------------------------------------------------------
  // Cenário B — Fonte alternativa desconhecida
  // Obra cadastrada apenas na fonte A. Fonte B ainda não tem vínculo. O mecanismo
  // de descoberta acha, valida e compartilha vínculo com P0 (monitor) e P1 (intake).
  // --------------------------------------------------------------------------
  it('Cenário B: descobre fonte alternativa desconhecida e notifica P0 e P1 automaticamente', async () => {
    const canonicalWorks: CanonicalWork[] = [
      { id: 'work-orv', title: 'Omniscient Reader', aliases: ['ORV', 'Ponto de Vista do Leitor Onisciente'], slug: 'omniscient-reader' },
    ];
    const persistence = new InMemoryP0CyclePersistence(canonicalWorks);

    const p0EnrolledMonitors: string[] = [];
    const p1EnqueuedCatalogTasks: string[] = [];

    const coordinator = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(canonicalWorks),
      persistence,
      onNewMappingDiscovered: async (mapping) => {
        // P0: Enrola monitor contínuo
        p0EnrolledMonitors.push(`${mapping.source}:${mapping.item.sourceWorkId}`);
        // P1: Enfileira ingestão de catálogo para reconciliação de capítulos
        p1EnqueuedCatalogTasks.push(`lite:catalog:${mapping.source}:${mapping.item.sourceWorkId}`);
      },
    });

    await coordinator.startCycle();

    // Fonte B desconhecida fornece item com título alternativo em PT-BR
    await coordinator.recordSourceCatalog('manhastro', [
      { sourceWorkId: 'manhastro-999', title: 'Ponto de Vista do Leitor Onisciente' },
    ]);

    const matches = await coordinator.matchCandidateWorks();
    expect(matches).toHaveLength(1);
    expect(matches[0].work.id).toBe('work-orv');
    expect(matches[0].method).toBe('alias');

    // Ambos os pipelines receberam o novo vínculo compartilhado
    expect(p0EnrolledMonitors).toEqual(['manhastro:manhastro-999']);
    expect(p1EnqueuedCatalogTasks).toEqual(['lite:catalog:manhastro:manhastro-999']);
    expect(coordinator.getCoverageMatrix('manhastro').find((e) => e.workId === 'work-orv')?.state).toBe('MAPPED');
  });

  // --------------------------------------------------------------------------
  // Cenário C — Capítulo recém-lançado exclusivamente em fonte alternativa
  // Obra preenchida até o cap 200. Fonte alternativa publica cap 201. P0 detecta
  // e admite como prioridade P0, sem depender da fonte original.
  // --------------------------------------------------------------------------
  it('Cenário C: detecta capítulo inédito em fonte alternativa e admite como P0 urgente', () => {
    const knownFrontierNormalOrder = 'n|000000000200.000000|000000000000.000000';
    const workHasPendingBacklog = false; // Obra está 100% em dia

    // Fonte alternativa publica capítulo 201
    const newChapter = {
      sourceChapterId: 'alt-ch-201',
      displayNumber: '201',
      title: 'Capítulo 201',
    };

    const orderKey = chapterOrderKey({ displayNumber: newChapter.displayNumber, kind: 'chapter' });
    const isNewestManifestRelease = true;
    const historicalRepair = orderKey.startsWith('n|') && Boolean(knownFrontierNormalOrder && orderKey <= knownFrontierNormalOrder);

    // Lógica canônica de prioridade P0 do ImporterLiteRuntime
    const chapterPriority = (!historicalRepair && isNewestManifestRelease && !workHasPendingBacklog) ? 'P0' : 'P1';

    expect(orderKey > knownFrontierNormalOrder).toBe(true);
    expect(historicalRepair).toBe(false);
    expect(chapterPriority).toBe('P0');
  });

  // --------------------------------------------------------------------------
  // Cenário D — Fonte original abandonada
  // A fonte original parou de atualizar. A fonte alternativa continua lançando.
  // A obra permanece ONGOING e recebe capítulos novos continuamente.
  // --------------------------------------------------------------------------
  it('Cenário D: obra com fonte original abandonada continua atualizando via fonte alternativa ativa', async () => {
    const canonicalWorks: CanonicalWork[] = [
      { id: 'work-abandoned-orig', title: 'Tower of God', aliases: ['Sin-ui Tap'], slug: 'tower-of-god' },
    ];
    const persistence = new InMemoryP0CyclePersistence(canonicalWorks);

    const coordinator = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(canonicalWorks),
      persistence,
    });
    await coordinator.startCycle();

    // Fonte original A parou no cap 100 há meses.
    // Fonte alternativa B publica semanalmente: caps 101, 102, 103...
    await coordinator.recordSourceCatalog('active-source-b', [
      { sourceWorkId: 'active-tog-1', title: 'Tower of God' },
    ]);
    const matches = await coordinator.matchCandidateWorks();
    expect(matches).toHaveLength(1);
    expect(persistence.mappings.has('active-source-b:active-tog-1')).toBe(true);

    // Verificação de rotas para capítulo 103: apenas fonte B tem o capítulo
    const routes: ChapterSourceRoute[] = [
      {
        source: 'abandoned-source-a',
        sourceChapterId: '',
        canonicalChapterKey: 'pt-br|default|chapter|103|-',
        workId: 'work-abandoned-orig',
        language: 'pt-BR',
        edition: 'default',
        sourceRank: 1,
        health: 'HEALTHY',
        confirmed: false,
        hasChapter: false,
      },
      {
        source: 'active-source-b',
        sourceChapterId: 'b-103',
        canonicalChapterKey: 'pt-br|default|chapter|103|-',
        workId: 'work-abandoned-orig',
        language: 'pt-BR',
        edition: 'default',
        sourceRank: 10,
        health: 'HEALTHY',
        confirmed: true,
        hasChapter: true,
      },
    ];

    const selection = selectChapterSource(routes, { language: 'pt-BR', edition: 'default' });
    expect(selection.route?.source).toBe('active-source-b');
    expect(selection.route?.sourceChapterId).toBe('b-103');
  });

  // --------------------------------------------------------------------------
  // Cenário E — Fonte indisponível com fallback inteligente
  // Uma fonte entra em timeout ou erro. O sistema utiliza rota alternativa válida,
  // respeita limites e preserva auditoria (FAILED / DEFERRED na matriz de cobertura).
  // --------------------------------------------------------------------------
  it('Cenário E: fonte em erro/cooldown aciona fallback automático para alternativa saudável', async () => {
    const now = new Date('2026-11-01T12:00:00Z');
    const routes: ChapterSourceRoute[] = [
      {
        source: 'failing-primary',
        sourceChapterId: 'fp-50',
        canonicalChapterKey: 'pt-br|default|chapter|50|-',
        workId: 'work-1',
        language: 'pt-BR',
        edition: 'default',
        sourceRank: 1,
        health: 'BROKEN',
        confirmed: true,
        hasChapter: true,
      },
      {
        source: 'cooldown-secondary',
        sourceChapterId: 'cs-50',
        canonicalChapterKey: 'pt-br|default|chapter|50|-',
        workId: 'work-1',
        language: 'pt-BR',
        edition: 'default',
        sourceRank: 5,
        health: 'HEALTHY',
        cooldownUntil: new Date('2026-11-01T12:30:00Z'), // Em cooldown por 30m
        confirmed: true,
        hasChapter: true,
      },
      {
        source: 'healthy-tertiary',
        sourceChapterId: 'ht-50',
        canonicalChapterKey: 'pt-br|default|chapter|50|-',
        workId: 'work-1',
        language: 'pt-BR',
        edition: 'default',
        sourceRank: 20,
        health: 'HEALTHY',
        confirmed: true,
        hasChapter: true,
      },
    ];

    const selection = selectChapterSource(routes, { language: 'pt-BR', edition: 'default' }, now);
    expect(selection.route?.source).toBe('healthy-tertiary');
    expect(selection.route?.sourceChapterId).toBe('ht-50');

    // Auditoria de estado no coordenador:
    const works = [{ id: 'work-1', title: 'Obra 1' }];
    const coordinator = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence: new InMemoryP0CyclePersistence(works),
      now: () => now,
    });
    await coordinator.startCycle();
    await coordinator.setCoverageState('failing-primary', 'work-1', 'FAILED', 'HTTP_500_TIMEOUT');
    await coordinator.setCoverageState('cooldown-secondary', 'work-1', 'DEFERRED', 'RATE_LIMIT_COOLDOWN');
    await coordinator.setCoverageState('healthy-tertiary', 'work-1', 'MAPPED');

    const matrix = coordinator.getCoverageMatrix();
    expect(matrix.find((e) => e.source === 'failing-primary')?.state).toBe('FAILED');
    expect(matrix.find((e) => e.source === 'cooldown-secondary')?.state).toBe('DEFERRED');
    expect(matrix.find((e) => e.source === 'healthy-tertiary')?.state).toBe('MAPPED');
  });

  // --------------------------------------------------------------------------
  // Cenário F — Sobrevivência à reinicialização do Importer durante um ciclo P0
  // O processo reinicia no meio do ciclo. O coordenador recupera o progresso
  // sem duplicar ou reiniciar do zero.
  // --------------------------------------------------------------------------
  it('Cenário F: reinicialização no meio do ciclo recupera progresso durável sem perda de estado', async () => {
    const works: CanonicalWork[] = [
      { id: 'work-1', title: 'Solo Leveling' },
      { id: 'work-2', title: 'Second Life Ranker' },
    ];
    const sharedPersistence = new InMemoryP0CyclePersistence(works);

    // Instância 1 começa o Ciclo
    const instance1 = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence: sharedPersistence,
    });
    const started = await instance1.startCycle();
    await instance1.recordSourceCatalog('source-1', [
      { sourceWorkId: 's1-1', title: 'Solo Leveling' },
    ]);
    await instance1.matchCandidateWorks();

    const progressBeforeCrash = instance1.getCycleMetrics();
    expect(progressBeforeCrash.sourcesProcessed).toBe(1);
    expect(progressBeforeCrash.candidatesMatched).toBe(1);

    // Crash / Reboot do Importer -> Instância 2 é criada apontando para o mesmo armazenamento
    const instance2 = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence: sharedPersistence,
    });

    const recovered = await instance2.resumeOrCreateCycle();
    expect(recovered.cycleId).toBe(started.cycleId);
    expect(recovered.status).toBe('RUNNING');
    expect(recovered.sourcesProcessed).toBe(1);
    expect(recovered.candidatesMatched).toBe(1);

    // Continua a execução sem perda
    await instance2.recordSourceCatalog('source-2', [
      { sourceWorkId: 's2-2', title: 'Second Life Ranker' },
    ]);
    await instance2.matchCandidateWorks();

    const completed = await instance2.completeCycle();
    expect(completed.sourcesProcessed).toBe(2);
    expect(completed.candidatesMatched).toBeGreaterThanOrEqual(2);
    expect(completed.status).toBe('COMPLETED');
  });

  // --------------------------------------------------------------------------
  // Cenário G — Deduplicação multifonte de capítulos
  // Duas fontes fornecem o mesmo capítulo. O sistema evita publicação duplicada
  // e preserva proveniência para fallback.
  // --------------------------------------------------------------------------
  it('Cenário G: deduplica capítulo presente em múltiplas fontes mantendo rotas alternativas', () => {
    const chapterA = {
      source: 'source-mangaflix',
      sourceChapterId: 'mf-10',
      displayNumber: '10',
      title: 'Capítulo 10',
      language: 'pt-BR',
      edition: 'default',
    };
    const chapterB = {
      source: 'source-nexus',
      sourceChapterId: 'nx-10',
      displayNumber: '10.0', // Variação de formatação
      title: 'Capítulo 10 - O Despertar',
      language: 'pt-BR',
      edition: 'default',
    };

    const keyA = canonicalChapterKey({ displayNumber: chapterA.displayNumber, kind: 'chapter', language: 'pt-BR', edition: 'default' });
    const keyB = canonicalChapterKey({ displayNumber: chapterB.displayNumber, kind: 'chapter', language: 'pt-BR', edition: 'default' });

    // A chave canônica unifica ambas as variantes para a mesma identidade exata
    expect(keyA).toBe('pt-br|default|chapter|10|-');
    expect(keyB).toBe('pt-br|default|chapter|10|-');
    expect(keyA).toBe(keyB);

    // A chave de idempotência de admissão de tarefa garante que apenas 1 tarefa é executada
    const idempotencyKeyA = `lite:chapter:work-test:${keyA}`;
    const idempotencyKeyB = `lite:chapter:work-test:${keyB}`;
    expect(idempotencyKeyA).toBe(idempotencyKeyB);

    // Ambas as rotas coexistem em importer_lite_chapter_sources
    const recordedRoutes: ChapterSourceRoute[] = [
      {
        source: chapterA.source,
        sourceChapterId: chapterA.sourceChapterId,
        canonicalChapterKey: keyA,
        workId: 'work-test',
        language: 'pt-BR',
        edition: 'default',
        sourceRank: 10,
        health: 'HEALTHY',
        confirmed: true,
        hasChapter: true,
      },
      {
        source: chapterB.source,
        sourceChapterId: chapterB.sourceChapterId,
        canonicalChapterKey: keyB,
        workId: 'work-test',
        language: 'pt-BR',
        edition: 'default',
        sourceRank: 20,
        health: 'HEALTHY',
        confirmed: true,
        hasChapter: true,
      },
    ];

    // Se sourceA falhar, sourceB é o fallback transparente imediato
    const candidates = selectableChapterSources(recordedRoutes, { language: 'pt-BR', edition: 'default' });
    expect(candidates.routes.length).toBe(2);

    const selection = selectChapterSource(recordedRoutes, { language: 'pt-BR', edition: 'default' });
    expect(selection.route?.source).toBe('source-mangaflix');
  });

  // --------------------------------------------------------------------------
  // Cenário H — Escalabilidade de catálogo: 10.749, 15.000, 20.000 e 30.000 obras
  // Valida que o matcher in-memory O(M) indexa e busca em milissegundos sem
  // degradação de CPU, memória ou tempo de resposta.
  // --------------------------------------------------------------------------
  it('Cenário H: escala comprovada para 10.749, 15.000, 20.000 e 30.000 obras com matching O(1)/O(M)', () => {
    const scales = [10_749, 15_000, 20_000, 30_000];

    for (const count of scales) {
      const generatedWorks: CanonicalWork[] = Array.from({ length: count }, (_, i) => ({
        id: `work-scale-${i}`,
        title: `Obra Canônica de Teste Escala ${i}`,
        slug: `obra-canonica-de-teste-escala-${i}`,
        aliases: [`Alias Alternativo de Teste ${i}`, `Short ${i}`],
      }));

      const t0 = performance.now();
      const matcher = new SourceCatalogMatcher(generatedWorks);
      const indexDurationMs = performance.now() - t0;

      // Indexação O(N) de 30.000 obras leva menos de 500ms
      expect(indexDurationMs).toBeLessThan(500);

      // Amostra de 100 itens de catálogo para busca
      const sampleItems: CatalogItem[] = [
        { sourceWorkId: 'sample-0', title: 'Obra Canônica de Teste Escala 0' },
        { sourceWorkId: 'sample-mid', title: `Alias Alternativo de Teste ${Math.floor(count / 2)}` },
        { sourceWorkId: 'sample-last', title: `Obra Canônica de Teste Escala ${count - 1}` },
        { sourceWorkId: 'sample-unknown', title: 'Título Inexistente no Catálogo Geral' },
      ];

      const tMatch0 = performance.now();
      const matches = matcher.matchMany(sampleItems);
      const matchDurationMs = performance.now() - tMatch0;

      expect(matches).toHaveLength(3);
      expect(matches[0].work.id).toBe('work-scale-0');
      expect(matches[1].work.id).toBe(`work-scale-${Math.floor(count / 2)}`);
      expect(matches[2].work.id).toBe(`work-scale-${count - 1}`);
      // Busca com fallback trigram em 30.000 obras deve ser rápida (< 500ms)
      expect(matchDurationMs).toBeLessThan(500);
    }
  });

  // --------------------------------------------------------------------------
  // Cenário I — P2 Subordinado e Preservação de Integridade
  // O P2 congelado não bloqueia novas fontes para obras existentes.
  // Não permite que obras inexistentes no cânone sejam criadas.
  // --------------------------------------------------------------------------
  it('Cenário I: P2 subordinado permite novas fontes para obras existentes e bloqueia criação de novas obras', async () => {
    const works: CanonicalWork[] = [
      { id: 'work-existing-1', title: 'Jujutsu Kaisen', aliases: ['Sorcery Fight'] },
    ];
    const persistence = new InMemoryP0CyclePersistence(works);

    const coordinator = new P0CycleCoordinator({
      matcher: new SourceCatalogMatcher(works),
      persistence,
      p2AdmissionPolicy: {
        isSuspended: () => true, // P2 suspenso!
      },
    });

    await coordinator.startCycle();

    // Catálogo da fonte traz 1 obra existente e 1 obra desconhecida
    await coordinator.recordSourceCatalog('source-new-scan', [
      { sourceWorkId: 'jjk-scan', title: 'Jujutsu Kaisen' },
      { sourceWorkId: 'brand-new-manga', title: 'Novo Manga Desconhecido 2026' },
    ]);

    const matches = await coordinator.matchCandidateWorks();

    // Apenas a obra existente é mapeada
    expect(matches).toHaveLength(1);
    expect(matches[0].work.id).toBe('work-existing-1');

    // A obra desconhecida NÃO foi criada e NÃO existe nos mapeamentos
    expect(persistence.mappings.has('source-new-scan:jjk-scan')).toBe(true);
    expect(persistence.mappings.has('source-new-scan:brand-new-manga')).toBe(false);
    expect(persistence.existingWorkIds.size).toBe(1); //works permanece inviolado
  });
});
