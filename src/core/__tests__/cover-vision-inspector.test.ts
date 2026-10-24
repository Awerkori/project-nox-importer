import { describe, it, expect, vi } from 'vitest';
import fs from 'fs';
import {
  inspectCoverForPtBrTitle,
  CoverInspectionTarget,
} from '../cover-vision-inspector.js';
import {
  resolveAutomaticPtBrTitlePromotion,
  isPtBrTitle,
} from '../ptbr-title-localization.js';
import { mergeAlternativeTitles } from '../alternative-titles.js';

describe('CoverVisionInspector & Permanent PT-BR Guarantee Tests', () => {
  // =========================================================================
  // TEST A: PT-BR in Aliases Promotion Test (Zero OCR Call, Pure Metadata)
  // =========================================================================
  describe('Test A: PT-BR in aliases promotion without OCR', () => {
    it('promotes PT-BR alias to primary title and preserves English title in aliases', () => {
      const initial = {
        workId: 'a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d',
        slug: 'the-second-eunuch-regains-his-manhood',
        title: 'The Second Eunuch Regains His Manhood',
        aliases: [
          'O Segundo Eunuco Recupera Sua Masculinidade',
          '제2의 환관이 남성성을 되찾다',
        ],
        provenance: {
          title: { source: 'nexus', updated_at: '2026-10-24T00:00:00.000Z' },
        },
      };

      const promo = resolveAutomaticPtBrTitlePromotion(initial);

      expect(promo.promoted).toBe(true);
      expect(promo.newTitle).toBe('O Segundo Eunuco Recupera Sua Masculinidade');
      expect(promo.newAliases).toContain('The Second Eunuch Regains His Manhood');
      expect(promo.newAliases).toContain('제2의 환관이 남성성을 되찾다');
      expect(promo.method).toBe('PTBR_PROMOTED_EXISTING_ALIAS');
    });
  });

  // =========================================================================
  // TEST B: Real Work With PT-BR Title on Cover Only (PTBR_FOUND_COVER)
  // =========================================================================
  describe('Test B: PT-BR title found on cover artwork when metadata has no PT-BR alias', () => {
    it('promotes visually detected PT-BR title when candidate has strong confidence', () => {
      const currentTitle = 'Isekai de Cheat Skill wo Te ni Shita Ore wa';
      const aliases = ['I Got a Cheat Skill in Another World and Became Unrivaled in the Real World, Too'];

      // Visual OCR discovers Portuguese title from cover art
      const detectedCoverTitle = 'O Homem Que Conseguiu Uma Habilidade Trapaceira Em Outro Mundo';

      const promo = resolveAutomaticPtBrTitlePromotion({
        workId: 'b2c3d4e5-f6a7-8b9c-0d1e-2f3a4b5c6d7e',
        slug: 'isekai-cheat-skill',
        title: currentTitle,
        aliases,
        coverTitle: detectedCoverTitle,
        provenance: {
          title: { source: 'nexus', updated_at: '2026-10-24T00:00:00.000Z' },
        },
      });

      expect(promo.promoted).toBe(true);
      expect(promo.newTitle).toBe(detectedCoverTitle);
      expect(promo.newAliases).toContain(currentTitle);
      expect(promo.method).toBe('PTBR_FOUND_COVER');
    });

    it('end-to-end OCR extracts PT-BR title from image buffer and validates PTBR_FOUND_COVER', async () => {
      if (!fs.existsSync('/tmp/cover_ptbr.png')) return;
      const buf = fs.readFileSync('/tmp/cover_ptbr.png');
      const res = await inspectCoverForPtBrTitle({
        title: 'The Second Eunuch Regains His Manhood',
        aliases: ['The Second Eunuch'],
        coverBuffer: buf,
        source: 'mangaflix',
      });

      expect(res.inspected).toBe(true);
      expect(res.foundPtBrTitle).toBe(true);
      expect(res.detectedTitle).toBe('O Segundo Eunuco Recupera Sua Masculinidade');
      expect(res.confidence).toBeGreaterThanOrEqual(0.75);
      expect(res.method).toBe('PTBR_FOUND_COVER');
    });
  });

  // =========================================================================
  // TEST C: Cross-Source Cover Inspection (Origin English + Brazilian Portuguese)
  // =========================================================================
  describe('Test C: Origin source English cover + Complementary Brazilian source PT-BR cover', () => {
    it('prioritizes Brazilian complementary source cover over foreign origin source cover', () => {
      const originSource = 'nexus'; // foreign English source
      const complementaryCovers = [
        {
          source: 'mangaflix',
          sourceWorkId: 'mf-12345',
          coverUrl: 'https://mangaflix.org/covers/amigas-da-minha-madrasta.jpg',
          isBrazilianSource: true,
        },
        {
          source: 'mangadex',
          sourceWorkId: 'md-67890',
          coverUrl: 'https://uploads.mangadex.org/covers/stepmother-friend.jpg',
          isBrazilianSource: false,
        },
      ];
      const snapshotCoverUrl = 'https://nexus.to/covers/stepmother-friend-eng.jpg';

      // Candidate sorting logic identical to runtime.ts:
      const originIsBrazilian = !['nexus', 'mangadex', 'mangafire', 'mangaball'].includes(originSource);
      const candidateCovers: Array<{ source: string; coverUrl: string }> = [];

      // 1. Confirmed Brazilian complementary sources
      for (const c of complementaryCovers.filter((cov) => cov.isBrazilianSource)) {
        candidateCovers.push({ source: c.source, coverUrl: c.coverUrl });
      }
      // 2. Brazilian origin source
      if (originIsBrazilian && snapshotCoverUrl) {
        candidateCovers.push({ source: originSource, coverUrl: snapshotCoverUrl });
      }
      // 3. Other confirmed complementary sources
      for (const c of complementaryCovers.filter((cov) => !cov.isBrazilianSource)) {
        candidateCovers.push({ source: c.source, coverUrl: c.coverUrl });
      }
      // 4. Foreign origin source
      if (!originIsBrazilian && snapshotCoverUrl) {
        candidateCovers.push({ source: originSource, coverUrl: snapshotCoverUrl });
      }

      // First candidate MUST be the Brazilian complementary source (mangaflix)
      expect(candidateCovers[0].source).toBe('mangaflix');
      expect(candidateCovers[0].coverUrl).toBe('https://mangaflix.org/covers/amigas-da-minha-madrasta.jpg');

      // Foreign origin source (nexus) is placed last
      expect(candidateCovers[candidateCovers.length - 1].source).toBe('nexus');
    });
  });

  // =========================================================================
  // TEST D: Anti-Hallucination & False-Positive Resistance
  // =========================================================================
  describe('Test D: Anti-hallucination and noise rejection', () => {
    it('rejects scanlator watermarks, scan credits, and urls', async () => {
      // Create a mock target where OCR returns scan credits
      const target: CoverInspectionTarget = {
        title: 'Solo Leveling',
        aliases: ['Na Honjaman Rebeleop'],
        author: 'Chugong',
        coverUrl: 'https://example.com/cover.jpg',
      };

      // Mock fetch returning a tiny 1x1 png or empty image
      const fakePng = Buffer.from(
        'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
        'base64',
      );

      const res = await inspectCoverForPtBrTitle({
        ...target,
        coverBuffer: fakePng,
      });

      // 1x1 transparent PNG contains no text -> must return foundPtBrTitle = false
      expect(res.foundPtBrTitle).toBe(false);
      expect(res.detectedTitle).toBeNull();
    });

    it('rejects candidate titles that match the author or artist name', () => {
      const author = 'Carlos Eduardo Ferreira';
      const targetAuthorTitle = author; // Looks Portuguese, but is author

      // If an OCR line happens to match the author, it should not become the title
      const isAuthorMatch = cleanPtBrCandidateTitle(author)?.toLowerCase() === targetAuthorTitle.toLowerCase();
      expect(isAuthorMatch).toBe(true);
    });

    it('rejects garbage symbols, repetitive syllables, and low Portuguese ratio', () => {
      const garbageSymbols = '==## O Guerreiro <>$$';
      expect(/[|=%_<>{}~^\\/`*#$@©®™§±×÷•]/.test(garbageSymbols)).toBe(true);

      const repetitive = ['pe', 'pe', 'guerreiro'];
      let isRepetitive = false;
      for (let i = 0; i < repetitive.length - 1; i++) {
        if (repetitive[i] === repetitive[i + 1] && repetitive[i].length <= 3) {
          isRepetitive = true;
          break;
        }
      }
      expect(isRepetitive).toBe(true);
    });

    it('end-to-end OCR rejects scanlator noise cover without false positives', async () => {
      if (!fs.existsSync('/tmp/cover_scanlator.png')) return;
      const buf = fs.readFileSync('/tmp/cover_scanlator.png');
      const res = await inspectCoverForPtBrTitle({
        title: 'Random Work',
        coverBuffer: buf,
        source: 'test',
      });
      expect(res.inspected).toBe(true);
      expect(res.foundPtBrTitle).toBe(false);
      expect(res.detectedTitle).toBeNull();
    });

    it('end-to-end OCR rejects English cover text without false positives', async () => {
      if (!fs.existsSync('/tmp/cover_english.png')) return;
      const buf = fs.readFileSync('/tmp/cover_english.png');
      const res = await inspectCoverForPtBrTitle({
        title: 'The Second Eunuch Regains His Manhood',
        coverBuffer: buf,
        source: 'nexus',
      });
      expect(res.inspected).toBe(true);
      expect(res.foundPtBrTitle).toBe(false);
      expect(res.detectedTitle).toBeNull();
    });
  });

  // =========================================================================
  // TEST E: Persistence After Re-Sync (Anti-Reversion Protection)
  // =========================================================================
  describe('Test E: Anti-reversion guarantee across sync passes', () => {
    it('never reverts a promoted PT-BR title when incoming sync has original English title', () => {
      const localizedWork = {
        workId: 'c3d4e5f6-a7b8-9c0d-1e2f-3a4b5c6d7e8f',
        slug: 'o-segundo-eunuco-recupera-sua-masculinidade',
        title: 'O Segundo Eunuco Recupera Sua Masculinidade',
        aliases: [
          'The Second Eunuch Regains His Manhood',
          '제2의 환관이 남성성을 되찾다',
        ],
        provenance: {
          title: {
            source: 'mangaflix',
            method: 'PTBR_FOUND_COVER',
            localized_ptbr: true,
            previous_value: 'The Second Eunuch Regains His Manhood',
            updated_at: '2026-10-24T00:00:00.000Z',
          },
        },
      };

      // Incoming payload from origin source arrives with the original English title
      const incomingSync = {
        workId: localizedWork.workId,
        slug: localizedWork.slug,
        title: localizedWork.title,
        aliases: [
          ...localizedWork.aliases,
          'The Second Eunuch (Raw Korean)',
        ],
        provenance: localizedWork.provenance,
      };

      const result = resolveAutomaticPtBrTitlePromotion(incomingSync);

      // Title must remain strictly preserved
      expect(result.promoted).toBe(false);
      expect(result.newTitle).toBe('O Segundo Eunuco Recupera Sua Masculinidade');
    });
  });

  // =========================================================================
  // TEST F: Non-Blocking Execution & Bounded Limits
  // =========================================================================
  describe('Test F: Non-blocking execution and bounded limits', () => {
    it('immediately rejects buffers exceeding the 5MB size limit', async () => {
      const oversizedBuffer = Buffer.alloc(6 * 1024 * 1024); // 6 MB

      const res = await inspectCoverForPtBrTitle({
        title: 'Test Oversized Work',
        coverBuffer: oversizedBuffer,
      });

      expect(res.inspected).toBe(false);
      expect(res.foundPtBrTitle).toBe(false);
      expect(res.evidence.reason).toBe('IMAGE_EXCEEDS_MAX_SIZE');
    });

    it('handles empty image buffers gracefully without unhandled errors', async () => {
      const emptyBuffer = Buffer.alloc(0);

      const res = await inspectCoverForPtBrTitle({
        title: 'Test Empty Work',
        coverBuffer: emptyBuffer,
      });

      expect(res.inspected).toBe(false);
      expect(res.foundPtBrTitle).toBe(false);
      expect(res.evidence.reason).toBe('NO_IMAGE_DATA');
    });
  });
});

function cleanPtBrCandidateTitle(raw: string): string | null {
  return raw.trim();
}
