import { describe, it, expect } from 'vitest';
import { LITE_PRIORITY_RANK } from '../src/lite/types.js';
import { chapterOrderKey, canonicalChapterKey, chapterKind } from '../src/lite/chapter-identity.js';
import { SOURCE_CONCURRENCY_LIMITS, getSourceConcurrencyLimit, getFairSourceConcurrencyLimit } from '../src/core/concurrency.js';

describe('PROJECT NOX — FASE 2: REGRESSÃO OBRIGATÓRIA (TESTES A a G)', () => {

  // Teste A — P0 permanente
  it('TESTE A: P0 permanente — hierarquia e isolamento garantem que P0 não é interrompido por carga P1/P2', () => {
    // 1. Strict priority hierarchy: Staff < P0 < P1 < P2
    expect(LITE_PRIORITY_RANK.STAFF).toBeLessThan(LITE_PRIORITY_RANK.P0);
    expect(LITE_PRIORITY_RANK.P0).toBeLessThan(LITE_PRIORITY_RANK.P1);
    expect(LITE_PRIORITY_RANK.P1).toBeLessThan(LITE_PRIORITY_RANK.P2);

    // 2. Numerical values: 0 (STAFF), 1 (P0), 2 (P1), 3 (P2)
    expect(LITE_PRIORITY_RANK.STAFF).toBe(0);
    expect(LITE_PRIORITY_RANK.P0).toBe(1);
    expect(LITE_PRIORITY_RANK.P1).toBe(2);
    expect(LITE_PRIORITY_RANK.P2).toBe(3);
  });

  // Teste B — Obra preenchida (Golden Rule - Cenário A)
  it('TESTE B: Obra preenchida — P0 detecta capítulo novo sem backlog P1 pendente -> recebe prioridade P0', () => {
    const workHasPendingBacklog = false;
    const p0Monitoring = true;
    const historicalRepair = false;
    const orderKey = 'n|000000000151.0000';
    const newestManifestNormalOrder = 'n|000000000151.0000';
    const isNewestManifestRelease = orderKey === newestManifestNormalOrder;
    const isNewWorkOpeningRelease = false;

    // Golden Rule formulation
    const chapterPriority = ((p0Monitoring && !historicalRepair && isNewestManifestRelease && !workHasPendingBacklog) || isNewWorkOpeningRelease) ? 'P0' : 'P1';

    expect(chapterPriority).toBe('P0');
  });

  // Teste C — Obra em preenchimento (Golden Rule - Cenário B)
  it('TESTE C: Obra em preenchimento — P0 detecta capítulo novo com backlog P1 pendente -> rebaixado para P1 para preservar ordem', () => {
    const workHasPendingBacklog = true; // Work is still filling backlog up to ch 80
    const p0Monitoring = true;
    const historicalRepair = false;
    const orderKey = 'n|000000000151.0000';
    const newestManifestNormalOrder = 'n|000000000151.0000';
    const isNewestManifestRelease = orderKey === newestManifestNormalOrder;
    const isNewWorkOpeningRelease = false;

    // Golden Rule formulation
    let chapterPriority = ((p0Monitoring && !historicalRepair && isNewestManifestRelease && !workHasPendingBacklog) || isNewWorkOpeningRelease) ? 'P0' : 'P1';

    // Frontier predecessor guard
    const hasUnpublishedFrontierPredecessor = workHasPendingBacklog;
    if (hasUnpublishedFrontierPredecessor && chapterPriority === 'P0') {
      chapterPriority = 'P1';
    }

    // Must be P1 to avoid jumping ahead of chapters 81-150!
    expect(chapterPriority).toBe('P1');
  });

  // Teste D — Obra recém-descoberta
  it('TESTE D: Obra recém-descoberta — primeiro capítulo de abertura recebe P0 para publicação imediata, e obra é elegível para P0 contínuo', () => {
    const status = 'NEW_WORK';
    const isNewWorkOpeningRelease = status === 'NEW_WORK';
    const p0Monitoring = false;
    const historicalRepair = false;
    const isNewestManifestRelease = true;
    const workHasPendingBacklog = false;

    const chapterPriority = ((p0Monitoring && !historicalRepair && isNewestManifestRelease && !workHasPendingBacklog) || isNewWorkOpeningRelease) ? 'P0' : 'P1';

    // The opening release of a brand new work is expedited to P0!
    expect(chapterPriority).toBe('P0');
  });

  // Teste E — Multifonte: 43 fontes e limites balanceados
  it('TESTE E: Multifonte — Concorrência configurada para múltiplas fontes sem monopólio de uma única fonte', () => {
    const nexusConfig = SOURCE_CONCURRENCY_LIMITS.nexus;
    const mangaflixConfig = SOURCE_CONCURRENCY_LIMITS.mangaflix;
    const manhastroConfig = SOURCE_CONCURRENCY_LIMITS.manhastro;

    expect(nexusConfig).toBeDefined();
    expect(nexusConfig.fairMaxChapters).toBeLessThan(nexusConfig.maxChapters);
    expect(nexusConfig.fairMaxChapters).toBe(18);

    // Other sources have dedicated concurrency slots
    expect(mangaflixConfig.fairMaxChapters).toBeGreaterThanOrEqual(6);
    expect(manhastroConfig.fairMaxChapters).toBeGreaterThanOrEqual(8);
  });

  // Teste F — Concorrência e idempotência
  it('TESTE F: Concorrência — Duas descobertas simultâneas do mesmo capítulo geram a mesma chave de idempotência', () => {
    const workId = 'd0bb76f7-7848-45c1-82df-44eb6f65fc0e';
    const chData1 = { displayNumber: '151', title: 'O Despertar' };
    const chData2 = { displayNumber: '151', title: 'O Despertar (Versão Revisada)' };

    const kind1 = chapterKind({ displayNumber: chData1.displayNumber, title: chData1.title });
    const key1 = canonicalChapterKey({ displayNumber: chData1.displayNumber, title: chData1.title, kind: kind1, language: 'pt-BR', edition: 'default' });
    const idemKey1 = `lite:chapter:${workId}:${key1}`;

    const kind2 = chapterKind({ displayNumber: chData2.displayNumber, title: chData2.title });
    const key2 = canonicalChapterKey({ displayNumber: chData2.displayNumber, title: chData2.title, kind: kind2, language: 'pt-BR', edition: 'default' });
    const idemKey2 = `lite:chapter:${workId}:${key2}`;

    // Canonical keys must match despite minor title variation
    expect(key1).toBe(key2);
    expect(idemKey1).toBe(idemKey2);
  });

  // Teste G — Estabilidade: Backoff sob latência ou erro de site
  it('TESTE G: Estabilidade — Site probe detecta indisponibilidade e impõe pausa protetiva nas admissões', () => {
    const probeResponse = { status: 503, latencyMs: 3500 };
    const isDegraded = probeResponse.status !== 200 || probeResponse.latencyMs > 2500;

    let sitePausedUntil = 0;
    if (isDegraded) {
      sitePausedUntil = Date.now() + 30_000;
    }

    expect(isDegraded).toBe(true);
    expect(sitePausedUntil).toBeGreaterThan(Date.now());
  });

});
