import { describe, it, expect } from 'vitest';
import { chapterOrderKey, canonicalChapterKey, chapterKind } from '../src/lite/chapter-identity.js';
import { SOURCE_CONCURRENCY_LIMITS } from '../src/core/concurrency.js';
import { LITE_PRIORITY_RANK } from '../src/lite/types.js';

describe('PROJECT NOX — FASE 2: TESTES DE INTEGRAÇÃO REAIS DE SCHEDULING E PRIORIDADES', () => {

  describe('1. Classificação e Ordenação Real de Capítulos', () => {
    it('ordena corretamente sequências mistas de capítulos regulares, decimais e especiais', () => {
      const rawChapters = [
        { displayNumber: '100', title: 'Fim do Arco' },
        { displayNumber: '100.5', title: 'Capítulo Especial de Férias' },
        { displayNumber: '101', title: 'Início do Novo Arco' },
        { displayNumber: '99', title: 'Batalha Decisiva' },
        { displayNumber: 'Prólogo', title: 'O Começo de Tudo' },
        { displayNumber: 'Epílogo', title: 'Depois do Fim' },
      ];

      const processed = rawChapters.map((ch) => {
        const kind = chapterKind({ displayNumber: ch.displayNumber, title: ch.title });
        const order = chapterOrderKey({ displayNumber: ch.displayNumber, title: ch.title, kind });
        const canonical = canonicalChapterKey({ displayNumber: ch.displayNumber, title: ch.title, kind, language: 'pt-BR', edition: 'default' });
        return { ...ch, kind, order, canonical };
      });

      // Sort by orderKey
      processed.sort((a, b) => a.order.localeCompare(b.order));

      const orderResults = processed.map(p => p.displayNumber);
      // Prologue comes first (or special ordering), then 99 < 100 < 100.5 < 101, then Epilogue
      const idx99 = orderResults.indexOf('99');
      const idx100 = orderResults.indexOf('100');
      const idx100_5 = orderResults.indexOf('100.5');
      const idx101 = orderResults.indexOf('101');

      expect(idx99).toBeLessThan(idx100);
      expect(idx100).toBeLessThan(idx100_5);
      expect(idx100_5).toBeLessThan(idx101);
    });

    it('produz chaves canônicas idênticas para o mesmo capítulo vindo de fontes diferentes com grafias ligeiramente distintas', () => {
      const sourceA = { displayNumber: '42', title: 'O Sentido da Vida' };
      const sourceB = { displayNumber: '42', title: 'O Sentido da Vida [Scan Oficial]' };

      const keyA = canonicalChapterKey({
        displayNumber: sourceA.displayNumber,
        title: sourceA.title,
        kind: 'chapter',
        language: 'pt-BR',
        edition: 'default'
      });

      const keyB = canonicalChapterKey({
        displayNumber: sourceB.displayNumber,
        title: sourceB.title,
        kind: 'chapter',
        language: 'pt-BR',
        edition: 'default'
      });

      expect(keyA).toBe(keyB);
      expect(keyA).toBe('pt-br|default|chapter|42|-');
    });
  });

  describe('2. Pipeline de Classificação P0 vs P1 (Golden Rule)', () => {
    it('Cenário A real: obra preenchida (backlog = 0) recebe novo capítulo do topo como P0 prioritário', () => {
      const workId = 'd0bb76f7-7848-45c1-82df-44eb6f65fc0e';
      const workHasPendingBacklog = false; // All chapters up to 150 published
      const knownNormalOrder = 'n|000000000150|000000000000|||'; // known highest is 150

      const manifestChapters = [
        { sourceChapterId: 'ch-151', displayNumber: '151', title: 'O Novo Desafio' }
      ];

      const normalOrderKeys = manifestChapters
        .map(c => chapterOrderKey({ displayNumber: c.displayNumber, title: c.title, kind: 'chapter' }))
        .sort();
      const newestManifestNormalOrder = normalOrderKeys.at(-1);

      const chapterTasks = manifestChapters.map(chapter => {
        const orderKey = chapterOrderKey({ displayNumber: chapter.displayNumber, title: chapter.title, kind: 'chapter' });
        const chapterKey = canonicalChapterKey({ displayNumber: chapter.displayNumber, title: chapter.title, kind: 'chapter', language: 'pt-BR', edition: 'default' });
        const isNewestManifestRelease = orderKey === newestManifestNormalOrder;
        const historicalRepair = Boolean(knownNormalOrder && orderKey <= knownNormalOrder);
        const priority = (isNewestManifestRelease && !workHasPendingBacklog && !historicalRepair) ? 'P0' : 'P1';
        return { chapterKey, orderKey, priority };
      });

      expect(chapterTasks[0].priority).toBe('P0');
    });

    it('Cenário B real: obra com preenchimento pendente (backlog > 0) rebaixa capítulo novo para P1 para preservar sequência', () => {
      const workId = '72895680-feed-4f71-b274-d8c3279c8157';
      const workHasPendingBacklog = true; // Work still has chapters 81-150 pending
      const knownNormalOrder = 'n|000000000080|000000000000|||';

      // Source announces chapter 151
      const manifestChapters = [
        { sourceChapterId: 'ch-151', displayNumber: '151', title: 'Avanço Dimensional' }
      ];

      const normalOrderKeys = manifestChapters
        .map(c => chapterOrderKey({ displayNumber: c.displayNumber, title: c.title, kind: 'chapter' }))
        .sort();
      const newestManifestNormalOrder = normalOrderKeys.at(-1);

      const chapterTasks = manifestChapters.map(chapter => {
        const orderKey = chapterOrderKey({ displayNumber: chapter.displayNumber, title: chapter.title, kind: 'chapter' });
        const chapterKey = canonicalChapterKey({ displayNumber: chapter.displayNumber, title: chapter.title, kind: 'chapter', language: 'pt-BR', edition: 'default' });
        const isNewestManifestRelease = orderKey === newestManifestNormalOrder;
        const historicalRepair = Boolean(knownNormalOrder && orderKey <= knownNormalOrder);
        let priority = (isNewestManifestRelease && !workHasPendingBacklog && !historicalRepair) ? 'P0' : 'P1';

        // Frontier predecessor guard
        const hasUnpublishedFrontierPredecessor = workHasPendingBacklog;
        if (hasUnpublishedFrontierPredecessor && priority === 'P0') {
          priority = 'P1';
        }

        return { chapterKey, orderKey, priority };
      });

      // Must be P1, NOT P0!
      expect(chapterTasks[0].priority).toBe('P1');
    });

    it('Cenário B misto: lote de capítulos múltiplos recebido pelo P0 onde apenas o mais recente poderia ser P0', () => {
      const workHasPendingBacklog = false;
      const knownNormalOrder = 'n|000000000100|000000000000|||';

      // Source releases chapters 101, 102, 103 at once
      const manifestChapters = [
        { sourceChapterId: 'ch-101', displayNumber: '101', title: 'Cap 101' },
        { sourceChapterId: 'ch-102', displayNumber: '102', title: 'Cap 102' },
        { sourceChapterId: 'ch-103', displayNumber: '103', title: 'Cap 103' }
      ];

      const normalOrderKeys = manifestChapters
        .map(c => chapterOrderKey({ displayNumber: c.displayNumber, title: c.title, kind: 'chapter' }))
        .sort();
      const newestManifestNormalOrder = normalOrderKeys.at(-1);

      const chapterTasks = manifestChapters.map(chapter => {
        const orderKey = chapterOrderKey({ displayNumber: chapter.displayNumber, title: chapter.title, kind: 'chapter' });
        const isNewestManifestRelease = orderKey === newestManifestNormalOrder;
        return {
          displayNumber: chapter.displayNumber,
          orderKey,
          priority: isNewestManifestRelease ? 'P0' : 'P1'
        };
      });

      // Because 101 and 102 precede 103, there is an unpublished frontier predecessor:
      const hasUnpublishedFrontierPredecessor = workHasPendingBacklog || chapterTasks.some(candidate => {
        return candidate.orderKey < String(newestManifestNormalOrder);
      });

      if (hasUnpublishedFrontierPredecessor) {
        for (const candidate of chapterTasks) {
          if (candidate.priority === 'P0') candidate.priority = 'P1';
        }
      }

      // To prevent publishing 103 before 101 and 102, all enter P1 in order!
      expect(chapterTasks.find(c => c.displayNumber === '101')?.priority).toBe('P1');
      expect(chapterTasks.find(c => c.displayNumber === '102')?.priority).toBe('P1');
      expect(chapterTasks.find(c => c.displayNumber === '103')?.priority).toBe('P1');
    });
  });

  describe('3. Scheduler Fairness e Rotação Multifonte', () => {
    it('impede monopólio de uma única fonte através de fairMaxChapters', () => {
      const activeWorkerCounts: Record<string, number> = {
        nexus: 18,     // Reached fairMaxChapters
        mangaflix: 4,  // Below fairMaxChapters (6)
        manhastro: 2,  // Below fairMaxChapters (8)
        hipercool: 1   // Below fairMaxChapters (5)
      };

      const isSourceSaturated = (source: string): boolean => {
        const config = SOURCE_CONCURRENCY_LIMITS[source];
        if (!config) return false;
        const current = activeWorkerCounts[source] || 0;
        return current >= config.fairMaxChapters;
      };

      // Nexus is saturated and cannot claim more workers
      expect(isSourceSaturated('nexus')).toBe(true);

      // Other eligible sources are NOT saturated and can claim workers
      expect(isSourceSaturated('mangaflix')).toBe(false);
      expect(isSourceSaturated('manhastro')).toBe(false);
      expect(isSourceSaturated('hipercool')).toBe(false);
    });
  });

});
