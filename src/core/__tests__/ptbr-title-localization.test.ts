import { describe, expect, it } from 'vitest';
import {
  cleanPtBrCandidateTitle,
  isConsecratedProperTitle,
  isPtBrTitle,
  OWNER_EDITORIAL_TITLE_PREFERENCES,
  PT_PT_REJECTED_ALIASES,
  resolveAutomaticPtBrTitlePromotion,
} from '../ptbr-title-localization.js';

describe('ptbr-title-localization', () => {
  it('recognizes legitimate Brazilian Portuguese titles including loanwords and accents', () => {
    expect(isPtBrTitle('O Segundo Eunuco Recupera Sua Masculinidade')).toBe(true);
    expect(isPtBrTitle('A Professora Yoshikawa Faz Tudo o Que Eu Mando')).toBe(true);
    expect(isPtBrTitle('Depósito de Porra da Onee-chan')).toBe(true);
    expect(isPtBrTitle('As Onee-chans Que Me Deixam Fodê-las 3')).toBe(true);
    expect(isPtBrTitle('Mulher Elástica')).toBe(true);
    expect(isPtBrTitle('Makima vs O Pau Demoníaco')).toBe(true);
    expect(isPtBrTitle('Spider-Man Pornô: O casamento')).toBe(true);
    expect(isPtBrTitle('Dragon Ball Z Hentai – As Férias da Chichi')).toBe(true);
    expect(isPtBrTitle('Um Pedaço')).toBe(true);
    expect(isPtBrTitle('Ataque dos Titãs')).toBe(true);
    expect(isPtBrTitle('O Basquete de Kuroko: Replace Plus')).toBe(true);
    expect(isPtBrTitle('Minha Adorável Cosplayer')).toBe(true);
    expect(isPtBrTitle('Novos Heróis')).toBe(true);
  });

  it('rejects English, Romaji, Spanish, French, Vietnamese, and Pinyin titles', () => {
    expect(isPtBrTitle('The Second Eunuch Regains His Manhood')).toBe(false);
    expect(isPtBrTitle('Boku No Iinari Yoshikawa Sensei')).toBe(false);
    expect(isPtBrTitle('Ano Ko o Korosu na - Edogawa Osamu Dark Stories')).toBe(false);
    expect(isPtBrTitle('Shén De Yùwàng Yóuxì')).toBe(false);
    expect(isPtBrTitle('La Familia Real')).toBe(false);
    expect(isPtBrTitle('Vigilante de Camaras de Seguridad')).toBe(false);
    expect(isPtBrTitle('Por fin contigo')).toBe(false);
    expect(isPtBrTitle('Solo para ti')).toBe(false);
    expect(isPtBrTitle('La novia de mi hermano')).toBe(false);
    expect(isPtBrTitle('Nhỏ em họ của tôi')).toBe(false);
    expect(isPtBrTitle("Plain Ol' Wife (Laboratório de Dexter)")).toBe(false);
  });

  it('Scenario 1: One Piece with owner preference promotes to "Um Pedaço"', () => {
    const canonicalOnePieceId = 'e1b4bce1-5fbd-4fdf-8930-ca4afc26771d';
    const promo = resolveAutomaticPtBrTitlePromotion({
      workId: canonicalOnePieceId,
      slug: 'one-piece-ptbr',
      title: 'One Piece',
      aliases: ['Um Pedaço', 'ワンピース', 'Wan Pīsu'],
    });

    expect(promo.promoted).toBe(true);
    expect(promo.newTitle).toBe('Um Pedaço');
    expect(promo.method).toBe('OWNER_EDITORIAL_PREFERENCE');
    expect(promo.ownerEditorialPreference).toBe(true);
    expect(promo.newAliases).toContain('One Piece');
    expect(promo.newAliases).not.toContain('Um Pedaço');
  });

  it('Scenario 2: Syncing from original sources never overwrites "Um Pedaço"', () => {
    const canonicalOnePieceId = 'e1b4bce1-5fbd-4fdf-8930-ca4afc26771d';
    const syncPromo = resolveAutomaticPtBrTitlePromotion({
      workId: canonicalOnePieceId,
      slug: 'one-piece-ptbr',
      title: 'Um Pedaço',
      aliases: ['One Piece', 'ワンピース', 'Wan Pīsu', 'One Piece (Manga)'],
      provenance: {
        title: {
          source: 'ptbr_primary_titles_v1',
          owner_editorial_preference: true,
          localized_ptbr: true,
        },
      },
    });

    expect(syncPromo.promoted).toBe(false);
    expect(syncPromo.newTitle).toBe('Um Pedaço');
    expect(syncPromo.ownerEditorialPreference).toBe(true);
  });

  it('Scenario 3: Validated PT-BR title is never overwritten by incoming foreign title', () => {
    const res = resolveAutomaticPtBrTitlePromotion({
      title: 'O Segundo Eunuco Recupera Sua Masculinidade',
      aliases: ['The Second Eunuch Regains His Manhood', '2회차 환관이 남성을 되찾음'],
      provenance: {
        title: {
          source: 'ptbr_primary_titles_v1',
          localized_ptbr: true,
        },
      },
    });

    expect(res.promoted).toBe(false);
    expect(res.newTitle).toBe('O Segundo Eunuco Recupera Sua Masculinidade');
  });

  it('Scenario 4: MangaDex / MangaFire / MangaBall enrich exclusively aliases for PT-BR works', () => {
    const existingWork = {
      title: 'A Veterana Pitica da Firma',
      aliases: ['Uchi no Kaisha no Chiisai Senpai no Hanashi'],
      provenance: {
        title: {
          source: 'ptbr_primary_titles_v1',
          localized_ptbr: true,
        },
      },
    };

    // When new aliases from MangaDex/MangaFire arrive:
    const incomingAliases = [
      ...existingWork.aliases,
      'My Tiny Senpai',
      'Story of a Small Senior in My Company',
    ];

    const res = resolveAutomaticPtBrTitlePromotion({
      title: existingWork.title,
      aliases: incomingAliases,
      provenance: existingWork.provenance,
    });

    expect(res.promoted).toBe(false);
    expect(res.newTitle).toBe('A Veterana Pitica da Firma');
    expect(incomingAliases).toContain('My Tiny Senpai');
    expect(incomingAliases).toContain('Story of a Small Senior in My Company');
  });

  it('Scenario 5: New work receives PT-BR title when verified PT-BR alias exists', () => {
    const newWorkPromo = resolveAutomaticPtBrTitlePromotion({
      title: 'The Second Eunuch Regains His Manhood',
      aliases: ['2회차 환관이 남성을 되찾음', 'O Segundo Eunuco Recupera Sua Masculinidade (Brazil)'],
    });

    expect(newWorkPromo.promoted).toBe(true);
    expect(newWorkPromo.newTitle).toBe('O Segundo Eunuco Recupera Sua Masculinidade');
    expect(newWorkPromo.method).toBe('PTBR_PROMOTED_EXISTING_ALIAS');
    expect(newWorkPromo.newAliases).toContain('The Second Eunuch Regains His Manhood');
    expect(newWorkPromo.newAliases).not.toContain('O Segundo Eunuco Recupera Sua Masculinidade');
  });

  it('Scenario 6: New PT-BR alias promotes a foreign/famous primary title (Attack on Titan -> Ataque dos Titãs)', () => {
    expect(isConsecratedProperTitle('Attack on Titan')).toBe(true);

    const aotPromo = resolveAutomaticPtBrTitlePromotion({
      title: 'Attack on Titan',
      aliases: ['Shingeki no Kyojin', 'Ataque dos Titãs'],
    });

    expect(aotPromo.promoted).toBe(true);
    expect(aotPromo.newTitle).toBe('Ataque dos Titãs');
    expect(aotPromo.method).toBe('PTBR_PROMOTED_EXISTING_ALIAS');
    expect(aotPromo.newAliases).toContain('Attack on Titan');
    expect(aotPromo.newAliases).toContain('Shingeki no Kyojin');
  });

  it('Scenario 7: European Portuguese (Bola de Dragão) is rejected and Dragon Ball stays preserved', () => {
    expect(PT_PT_REJECTED_ALIASES.has('bola de dragao')).toBe(true);
    expect(PT_PT_REJECTED_ALIASES.has('bola de dragão')).toBe(true);

    const dbPromo = resolveAutomaticPtBrTitlePromotion({
      title: 'Dragon Ball',
      aliases: ['Bola de Dragão', 'Dragon Ball Z', 'ドラゴンボール'],
    });

    expect(dbPromo.promoted).toBe(false);
    expect(dbPromo.newTitle).toBe('Dragon Ball');
  });

  it('Scenario 8: Zero ping-pong oscillation between different language titles', () => {
    let currentTitle = 'The Second Eunuch Regains His Manhood';
    let currentAliases = ['O Segundo Eunuco Recupera Sua Masculinidade', '2회차 환관이 남성을 되찾음'];

    // 1st step: foreign title promotes to PT-BR
    const step1 = resolveAutomaticPtBrTitlePromotion({
      title: currentTitle,
      aliases: currentAliases,
    });
    expect(step1.promoted).toBe(true);
    expect(step1.newTitle).toBe('O Segundo Eunuco Recupera Sua Masculinidade');

    currentTitle = step1.newTitle;
    currentAliases = step1.newAliases;

    // 2nd step: running again with the newly promoted title
    const step2 = resolveAutomaticPtBrTitlePromotion({
      title: currentTitle,
      aliases: currentAliases,
      provenance: {
        title: {
          source: 'importer',
          localized_ptbr: true,
          method: 'PTBR_PROMOTED_EXISTING_ALIAS',
        },
      },
    });
    expect(step2.promoted).toBe(false);
    expect(step2.newTitle).toBe('O Segundo Eunuco Recupera Sua Masculinidade');

    // 3rd step: source tries to push English title back into candidate aliases
    const step3 = resolveAutomaticPtBrTitlePromotion({
      title: currentTitle,
      aliases: [...currentAliases, 'The Second Eunuch Regains His Manhood Season 2'],
      provenance: {
        title: {
          source: 'importer',
          localized_ptbr: true,
        },
      },
    });
    expect(step3.promoted).toBe(false);
    expect(step3.newTitle).toBe('O Segundo Eunuco Recupera Sua Masculinidade');
  });

  it('cleans (Brazil) and scan suffixes from candidate PT-BR titles', () => {
    expect(cleanPtBrCandidateTitle('Não mexa comigo, Nagatoro (Brazil)')).toBe('Não mexa comigo, Nagatoro');
  });
});
