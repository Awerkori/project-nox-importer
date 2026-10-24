import { alternativeTitleKey, cleanAlternativeTitle, mergeAlternativeTitles } from '../core/alternative-titles.js';
import { extractSeason, hasSequelOrEditionConflict, isNovel, matchWorkCandidate, normalizeTitle, significantTokenKey, } from '../core/matching.js';
/**
 * Scripts excluded from Project Nox `works.aliases` persistence:
 * Cyrillic, Hebrew, Arabic, Devanagari/Indic, Thai, Myanmar, Georgian, Armenian,
 * Multi-letter Greek words, and Vietnamese horn/breve/đ diacritics.
 * Allowed scripts: Latin (PT-BR, EN, Romaji, Pinyin, international) + Japanese + Korean + Chinese.
 */
const EXCLUDED_PERSIST_SCRIPT_REGEX = /[\u0400-\u052f\u0530-\u058f\u0590-\u05ff\u0600-\u06ff\u0750-\u077f\u0900-\u0dff\u0e00-\u0e7f\u1000-\u10ff\u{20000}-\u{2ceaf}]|[α-ωάέήίόύώ]{4,}|[ăắằẳẵặấầẩẫậđếềểễệốồổỗộơớờởỡợưứừửữự]|\(\s*Nom\s+Script\s*\)/iu;
const CJK_CHAR_REGEX = /[\u3040-\u30ff\u3400-\u9fff\uf900-\ufaff\uac00-\ud7af]/gu;
const HAS_CJK_REGEX = /[\u3040-\u30ff\u3400-\u9fff\uf900-\ufaff\uac00-\ud7af]/u;
const HAS_HANGUL_REGEX = /[\uac00-\ud7af]/u;
const HAS_KANA_REGEX = /[\u3040-\u30ff]/u;
const SPANISH_OR_ITALIAN_EXCLUSIVE_REGEX = /[ñ¿¡]|^la\s+|(?<![\p{L}\p{N}\-])(?:el|los|las|del|una|unos|unas|hacia|también|después|pero|joven|jóvenes|corazón|dragón|prisión|traición|reencarnación|maldición|canción|pasión|habitación|educación|tentación|obsesión|redención|salvación|en\s+el|en\s+la|de\s+la|con\s+el|con\s+la|a\s+solas|mi\s+hermana|mi\s+esposa|nella|della|degli|delle|nello|ragazza|dans|avec|pour|une|château)(?![\p{L}\p{N}])/iu;
const PT_STRONG_DIACRITIC_OR_SUFFIX_REGEX = /[ãõç]|\b\p{L}+(?:ção|ções|nho|nha|lh[ao]s?|íssim[ao])\b/iu;
const PT_LEXICON = new Set([
    'o', 'os', 'um', 'uma', 'do', 'da', 'dos', 'das', 'no', 'na', 'nos', 'nas',
    'pelo', 'pela', 'pelos', 'pelas', 'meu', 'minha', 'meus', 'minhas', 'seu', 'sua',
    'seus', 'suas', 'nao', 'não', 'sem', 'mais', 'como', 'quando', 'apos', 'após',
    'ate', 'até', 'entre', 'contra', 'sobre', 'irma', 'irmã', 'irmao', 'irmão',
    'marido', 'esposa', 'noiva', 'noivo', 'namorada', 'namorado', 'vila', 'vilã',
    'vilao', 'vilão', 'heroi', 'herói', 'heroina', 'heroína', 'principe', 'príncipe',
    'princesa', 'duque', 'duquesa', 'imperador', 'imperatriz', 'rei', 'rainha',
    'deus', 'deusa', 'demonio', 'demônio', 'dragao', 'dragão', 'torre', 'fazendeiro',
    'unico', 'único', 'unica', 'única', 'cacador', 'caçador', 'cacadora', 'caçadora',
    'espada', 'feiticeiro', 'feiticeira', 'mago', 'academia', 'escola', 'garota',
    'garoto', 'menino', 'menina', 'mulher', 'homem', 'segredo', 'vinganca', 'vingança',
    'mundo', 'vida', 'morte', 'noite', 'retorno', 'regresso', 'reencarnacao',
    'reencarnação', 'nivel', 'nível', 'mestre', 'discipulo', 'discípulo', 'sombrio',
    'sangrento', 'divino', 'supremo', 'lendario', 'lendário', 'maldito', 'doce',
    'amor', 'coracao', 'coração', 'familia', 'família', 'filha', 'filho', 'mae',
    'mãe', 'pai', 'crônicas', 'cronicas', 'lenda', 'sombras', 'luz', 'guerreiro',
]);
const PT_CONTENT_LEXICON = new Set([
    'não', 'irma', 'irmã', 'irmao', 'irmão', 'marido', 'esposa', 'noiva', 'noivo',
    'namorada', 'namorado', 'vilã', 'vilão', 'herói', 'heroína', 'príncipe', 'princesa',
    'duque', 'duquesa', 'imperador', 'imperatriz', 'rainha', 'demônio', 'dragão',
    'torre', 'fazendeiro', 'único', 'única', 'caçador', 'caçadora', 'espada',
    'feiticeiro', 'feiticeira', 'garota', 'garoto', 'menino', 'menina', 'mulher',
    'homem', 'segredo', 'vingança', 'morte', 'noite', 'retorno', 'regresso',
    'reencarnação', 'nível', 'discípulo', 'sombrio', 'sangrento', 'lendário',
    'maldito', 'doce', 'coração', 'família', 'filha', 'filho', 'mãe', 'guerreiro',
]);
const COMMON_GENERIC_WORDS = new Set([
    'secret', 'secrets', 'segredo', 'segredos', 'love', 'amor', 'lover', 'lovers',
    'young', 'first', 'last', 'one', 'night', 'day', 'girl', 'boy', 'woman', 'man',
    'sister', 'brother', 'mother', 'mom', 'wife', 'husband', 'maid', 'teacher',
    'sensei', 'senpai', 'kouhai', 'neighbor', 'neighbors', 'room', 'house', 'home',
    'sweet', 'bad', 'good', 'pure', 'dark', 'red', 'blue', 'white', 'black', 'summer',
    'winter', 'spring', 'rain', 'snow', 'dream', 'dreams', 'promise', 'confession',
    'desire', 'temptation', 'trap', 'game', 'play', 'lesson', 'club', 'after', 'school',
    'vacation', 'holiday', 'touch', 'kiss', 'real', 'cross', 'zero', 'monster', 'monsters',
    'world', 'life', 'story', 'tale', 'way', 'out', 'no', 'my', 'your', 'our', 'her', 'his',
    'anything', 'everything', 'something', 'nothing', 'someone', 'anyone', 'everyone',
    'cleaning', 'service', 'services', 'chicken', 'you', 'for', 'the', 'and', 'with',
]);
const PLACEHOLDER_CREATORS = new Set([
    '', 'unknown', 'desconhecido', 'n a', 'na', 'none', 'null', 'updating', 'autor', 'artista',
]);
const SPINOFF_STRICT_REGEX = /\b(?:side\s*story|gaiden|spin[\s-]*off|extra\s*story|hist[oó]ria\s+paralela|hist[oó]ria\s+extra|vigilantes|team[\s-]*up\s+missions|anthology|antologia|4[\s-]*koma|yonkoma|reboot)\b/i;
const ONESHOT_SUFFIX_REGEX = /^(?:\[(?:H|A|MOTION|18\+|R-?18|NSFW|DJ|Doujin(?:shi)?)\]\s*)+|\s*[\[(]\s*(?:one[\s-]*shot|oneshot|completo|pt[\s-]*br|decensored|uncensored|official|raw)\s*[\])]\s*$|\s+\b(?:decensored|uncensored|raw)\b\s*$/gi;
export function isPersistableAliasScript(title) {
    const cleaned = cleanAlternativeTitle(title);
    if (!cleaned)
        return false;
    if (EXCLUDED_PERSIST_SCRIPT_REGEX.test(cleaned))
        return false;
    return true;
}
export function isPortugueseAlias(title, explicitLang) {
    if (explicitLang) {
        const lang = explicitLang.toLowerCase();
        if (lang === 'pt-br' || lang === 'pt')
            return true;
        if (lang !== 'und' && lang !== 'unknown')
            return false;
    }
    const cleaned = cleanAlternativeTitle(title);
    if (!cleaned || HAS_CJK_REGEX.test(cleaned) || EXCLUDED_PERSIST_SCRIPT_REGEX.test(cleaned)) {
        return false;
    }
    if (SPANISH_OR_ITALIAN_EXCLUSIVE_REGEX.test(cleaned)) {
        return false;
    }
    if (PT_STRONG_DIACRITIC_OR_SUFFIX_REGEX.test(cleaned)) {
        return true;
    }
    const words = cleaned
        .toLowerCase()
        .replace(/[^\p{L}\p{N}\s]/gu, ' ')
        .split(/\s+/)
        .filter(Boolean);
    if (words.length < 2)
        return false;
    let ptHits = 0;
    let contentHits = 0;
    for (const w of words) {
        if (PT_LEXICON.has(w))
            ptHits++;
        if (PT_CONTENT_LEXICON.has(w))
            contentHits++;
    }
    return (ptHits >= 3 && contentHits >= 1) || (ptHits >= 2 && contentHits >= 2);
}
function stripSearchDecorations(title) {
    return title.replace(ONESHOT_SUFFIX_REGEX, '').trim();
}
function normalizeCreatorList(raw) {
    if (!raw)
        return [];
    const items = Array.isArray(raw) ? raw : String(raw).split(/[,;/|&]+/);
    const out = [];
    for (const item of items) {
        const norm = normalizeTitle(item).trim();
        if (norm.length >= 3 && !PLACEHOLDER_CREATORS.has(norm) && !out.includes(norm)) {
            out.push(norm);
        }
    }
    return out;
}
function creatorsMatch(noxCreators, candCreators) {
    if (!noxCreators.length || !candCreators.length)
        return false;
    for (const nc of noxCreators) {
        const ncCompact = nc.replace(/\s+/g, '');
        const ncTokens = nc.split(/\s+/).filter(Boolean).sort().join(' ');
        for (const cc of candCreators) {
            if (nc === cc)
                return true;
            if (ncCompact.length >= 5 && ncCompact === cc.replace(/\s+/g, ''))
                return true;
            const ccTokens = cc.split(/\s+/).filter(Boolean).sort().join(' ');
            if (ncTokens.length >= 5 && ncTokens === ccTokens)
                return true;
        }
    }
    return false;
}
function isHighSpecificityTitle(rawTitle) {
    const cjkMatches = rawTitle.match(CJK_CHAR_REGEX);
    if (cjkMatches && cjkMatches.length >= 4) {
        return true;
    }
    const norm = normalizeTitle(stripSearchDecorations(rawTitle));
    if (!norm)
        return false;
    const tokens = norm.split(/\s+/).filter(Boolean);
    if (tokens.length >= 3 && norm.length >= 14) {
        const nonGeneric = tokens.filter((t) => !COMMON_GENERIC_WORDS.has(t));
        return nonGeneric.length >= 1 || tokens.length >= 4;
    }
    if (tokens.length === 2 && norm.length >= 12) {
        const allGeneric = tokens.every((t) => COMMON_GENERIC_WORDS.has(t));
        if (!allGeneric && tokens.some((t) => t.length >= 6 && !COMMON_GENERIC_WORDS.has(t))) {
            return true;
        }
    }
    return false;
}
function hasHardIdentityConflict(nox, cand, matchedCandString) {
    const noxClean = stripSearchDecorations(nox.title);
    const candClean = stripSearchDecorations(cand.primaryTitle);
    // 1. Novel vs Comic
    const noxNovel = isNovel(nox.title, nox.kind || undefined);
    const candNovel = cand.kind === 'novel' ||
        cand.tags?.some((t) => /^(?:light\s*novel|web\s*novel|novel)$/i.test(t)) ||
        isNovel(cand.primaryTitle, cand.kind);
    if (noxNovel !== Boolean(candNovel)) {
        return 'NOVEL_VS_COMIC_CONFLICT';
    }
    // 2. Spin-off / Anthology / Gaiden / 4-koma conflict
    const noxSpin = SPINOFF_STRICT_REGEX.test(noxClean);
    const candSpin = SPINOFF_STRICT_REGEX.test(candClean) ||
        Boolean(cand.tags?.some((t) => /^(?:anthology|spin[\s-]*off|doujinshi->skip)$/i.test(t) && /^anthology$/i.test(t)));
    if (noxSpin !== candSpin) {
        // Allow if the matched candidate alias explicitly matches Nox's exact title without conflict
        if (!matchedCandString || normalizeTitle(matchedCandString) !== normalizeTitle(noxClean)) {
            return 'SPINOFF_OR_ANTHOLOGY_CONFLICT';
        }
    }
    // 3. Season conflict
    const noxSeason = extractSeason(noxClean);
    const candPrimarySeason = extractSeason(candClean);
    const candMatchSeason = matchedCandString ? extractSeason(stripSearchDecorations(matchedCandString)) : null;
    if (noxSeason !== candPrimarySeason && (noxSeason || candPrimarySeason)) {
        if (!matchedCandString || noxSeason !== candMatchSeason) {
            return `SEASON_CONFLICT:${noxSeason || 's1'}_VS_${candPrimarySeason || 's1'}`;
        }
    }
    // 4. Sequel / Edition conflict
    const compareTarget = matchedCandString ? stripSearchDecorations(matchedCandString) : candClean;
    const seqConflict = hasSequelOrEditionConflict(noxClean, compareTarget);
    if (seqConflict) {
        return `SEQUEL_CONFLICT:${seqConflict}`;
    }
    // Even if an alias matched, ensure candidate's primary title isn't a sequel ("Title 2", "Title:re", "Title Super")
    const primarySeqConflict = hasSequelOrEditionConflict(noxClean, candClean);
    if (primarySeqConflict) {
        return `PRIMARY_SEQUEL_CONFLICT:${primarySeqConflict}`;
    }
    // 5. Content rating / Adult parody guard
    const noxIsAdult = String(nox.contentRating || '').toUpperCase() === 'ADULT_18' ||
        Number(nox.ageRating || 0) >= 18 ||
        /\b(?:hentai|porno|pornô|doujin(?:shi)?)\b/i.test(nox.title);
    const candExplicitPorn = cand.contentRating === 'pornographic' ||
        Boolean(cand.tags?.some((t) => /^(?:hentai|pornographic)$/i.test(t)));
    const candTitleParody = /\b(?:hentai|porno|pornô|doujin(?:shi)?)\b/i.test(cand.primaryTitle);
    const noxTitleParody = /\b(?:hentai|porno|pornô|doujin(?:shi)?)\b/i.test(nox.title);
    // Always block adult parody titles when Nox's title has no parody marker ("Chainsaw Man" vs "Chainsaw Man Hentai")
    if (!noxTitleParody && candTitleParody) {
        return 'SAFE_WORK_VS_EXPLICIT_PORN_CONFLICT';
    }
    // Many Nox works imported from 18+ providers retain the column default `age_rating = 12` / `GENERAL`.
    // Only hard-block `pornographic` rating when Nox is not marked adult AND the match is NOT a high-specificity exact title.
    if (!noxIsAdult && candExplicitPorn) {
        const exactHighSpec = Boolean(matchedCandString && isHighSpecificityTitle(matchedCandString));
        if (!exactHighSpec) {
            return 'SAFE_WORK_VS_EXPLICIT_PORN_CONFLICT';
        }
    }
    if (noxTitleParody && !candTitleParody && !cand.isAdult && cand.contentRating === 'safe') {
        return 'ADULT_PARODY_VS_SAFE_MAINSTREAM_CONFLICT';
    }
    // 6. Original CJK script conflict (Korean Hangul manhwa vs Japanese Kana manga sharing an English homonym)
    if (!matchedCandString || !HAS_CJK_REGEX.test(matchedCandString)) {
        const noxTexts = [nox.title, ...(nox.aliases || [])];
        const noxHasHangul = noxTexts.some((t) => HAS_HANGUL_REGEX.test(t));
        const noxHasKana = noxTexts.some((t) => HAS_KANA_REGEX.test(t));
        const candTexts = [cand.primaryTitle, ...(cand.rawMatchAliases || cand.aliases.map((a) => a.title))];
        const candHasHangul = candTexts.some((t) => HAS_HANGUL_REGEX.test(t));
        const candHasKana = candTexts.some((t) => HAS_KANA_REGEX.test(t));
        if (noxHasHangul &&
            !noxHasKana &&
            !candHasHangul &&
            (cand.originalLanguage === 'ja' || (cand.kind === 'manga' && candHasKana))) {
            return 'ORIGINAL_SCRIPT_LANGUAGE_CONFLICT:KO_VS_JA';
        }
        if (noxHasKana &&
            !noxHasHangul &&
            !candHasKana &&
            (cand.originalLanguage === 'ko' || (cand.kind === 'manhwa' && candHasHangul))) {
            return 'ORIGINAL_SCRIPT_LANGUAGE_CONFLICT:JA_VS_KO';
        }
    }
    return null;
}
/**
 * Protects adult/tankoubon manga from chapter-list story dumps mistakenly listed in `altTitles`.
 */
export function sanitizeCandidateAliases(nox, cand) {
    const persistable = [];
    for (const item of cand.aliases) {
        const stripped = item.title
            .replace(/^(?:\[(?:H|A|MOTION|18\+|R-?18|NSFW|DJ|Doujin(?:shi)?)\]\s*)+/gi, '')
            .replace(/\s*(?:[\[(]?\b(?:decensored|uncensored|official)\b[\])]?|\(\s*raw\s*\)|\braw)\s*$/gi, '')
            .trim();
        const cleaned = cleanAlternativeTitle(stripped);
        if (!cleaned || !isPersistableAliasScript(cleaned))
            continue;
        persistable.push({ title: cleaned, language: item.language });
    }
    // Tankoubon story-list heuristic: explicit pornographic Japanese tankoubon manga with >= 6 aliases
    // where >= 4 EN/JA-RO Latin aliases share zero content words with nox.title / nox.aliases
    const isJapanesePornTankoubon = cand.contentRating === 'pornographic' &&
        (cand.originalLanguage === 'ja' || (!cand.originalLanguage && cand.kind === 'manga'));
    if (isJapanesePornTankoubon && persistable.length >= 6) {
        const noxWords = new Set([nox.title, ...nox.aliases]
            .flatMap((t) => normalizeTitle(t).split(/\s+/))
            .filter((w) => w.length >= 4 && !COMMON_GENERIC_WORDS.has(w)));
        const primaryWords = normalizeTitle(cand.primaryTitle)
            .split(/\s+/)
            .filter((w) => w.length >= 4 && !COMMON_GENERIC_WORDS.has(w));
        const primarySharesNoxWord = noxWords.size === 0 || primaryWords.some((w) => noxWords.has(w));
        const anchorWords = new Set([
            ...noxWords,
            ...(primarySharesNoxWord ? primaryWords : []),
        ]);
        const noxCjkKeys = new Set([nox.title, ...nox.aliases]
            .filter((t) => HAS_CJK_REGEX.test(t))
            .map((t) => alternativeTitleKey(t)));
        const disjointLatin = persistable.filter((p) => {
            if (HAS_CJK_REGEX.test(p.title))
                return false;
            if (p.language && !/^(?:en|ja|ja-ro|und|unknown)$/i.test(p.language))
                return false;
            const words = normalizeTitle(p.title).split(/\s+/).filter((w) => w.length >= 4);
            return words.length > 0 && !words.some((w) => anchorWords.has(w));
        });
        if (disjointLatin.length >= 4) {
            return persistable.filter((p) => {
                if (HAS_CJK_REGEX.test(p.title)) {
                    return primarySharesNoxWord || noxCjkKeys.size === 0 || noxCjkKeys.has(alternativeTitleKey(p.title));
                }
                if (primarySharesNoxWord && alternativeTitleKey(p.title) === alternativeTitleKey(cand.primaryTitle)) {
                    return true;
                }
                const words = normalizeTitle(p.title).split(/\s+/).filter((w) => w.length >= 4);
                return words.some((w) => anchorWords.has(w));
            });
        }
    }
    return persistable;
}
function hasSharedCrossLink(known, cand) {
    if (!known || !cand)
        return null;
    if (known.mangadexId && cand.mangadexId && known.mangadexId.toLowerCase() === cand.mangadexId.toLowerCase()) {
        return `mangadex:${known.mangadexId}`;
    }
    if (known.anilistId && cand.anilistId && known.anilistId === cand.anilistId) {
        return `anilist:${known.anilistId}`;
    }
    if (known.malId && cand.malId && known.malId === cand.malId) {
        return `mal:${known.malId}`;
    }
    if (known.mangaUpdatesId && cand.mangaUpdatesId && known.mangaUpdatesId.toLowerCase() === cand.mangaUpdatesId.toLowerCase()) {
        return `mangaupdates:${known.mangaUpdatesId}`;
    }
    return null;
}
/**
 * Evaluates all candidates returned for a work on a single ALIAS_ONLY source
 * and classifies into CONFIRMED_MATCH, AMBIGUOUS_MATCH, or NOT_FOUND.
 */
export function evaluateSourceCandidates(nox, candidates, corroboratedContext) {
    if (!candidates.length) {
        return {
            status: 'NOT_FOUND',
            candidate: null,
            confidence: 0,
            matchMethod: null,
            matchReason: 'NO_SEARCH_RESULTS',
            evidence: {},
        };
    }
    // Deduplicate candidates by sourceWorkId
    const uniqueCandidates = [];
    const seenIds = new Set();
    for (const c of candidates) {
        if (!c.sourceWorkId || seenIds.has(c.sourceWorkId))
            continue;
        seenIds.add(c.sourceWorkId);
        uniqueCandidates.push(c);
    }
    const allKnownTitles = [
        nox.title,
        ...(nox.aliases || []),
        ...(corroboratedContext?.confirmedTitles || []),
    ]
        .map((t) => cleanAlternativeTitle(t))
        .filter((t) => Boolean(t));
    const knownByNorm = new Map();
    for (const raw of allKnownTitles) {
        const norm = normalizeTitle(stripSearchDecorations(raw));
        if (!norm)
            continue;
        const list = knownByNorm.get(norm) || [];
        if (!list.includes(raw))
            list.push(raw);
        knownByNorm.set(norm, list);
    }
    const noxCreators = normalizeCreatorList([nox.author || '', nox.artist || ''].filter(Boolean));
    const scored = [];
    for (const cand of uniqueCandidates) {
        const candStrings = [
            cand.primaryTitle,
            ...(cand.rawMatchAliases || cand.aliases.map((a) => a.title)),
        ].filter(Boolean);
        const exactMatchedPairs = [];
        const matchedNormSet = new Set();
        for (const cs of candStrings) {
            const cNorm = normalizeTitle(stripSearchDecorations(cs));
            if (!cNorm)
                continue;
            const matchingNoxList = knownByNorm.get(cNorm);
            if (matchingNoxList && matchingNoxList.length > 0) {
                if (!matchedNormSet.has(cNorm)) {
                    matchedNormSet.add(cNorm);
                    exactMatchedPairs.push({ noxStr: matchingNoxList[0], candStr: cs, norm: cNorm });
                }
            }
        }
        const bestExactCandStr = exactMatchedPairs[0]?.candStr;
        const hardConflict = hasHardIdentityConflict(nox, cand, bestExactCandStr);
        if (hardConflict)
            continue;
        const candCreators = normalizeCreatorList([...(cand.authors || []), ...(cand.artists || [])]);
        const creatorMatched = creatorsMatch(noxCreators, candCreators);
        const sharedLink = hasSharedCrossLink(corroboratedContext?.confirmedLinks, cand.links);
        const yearCompatible = Boolean(nox.year && cand.year && Math.abs(Number(nox.year) - Number(cand.year)) <= 1);
        const hasHighSpecificityExact = exactMatchedPairs.some((p) => isHighSpecificityTitle(p.noxStr) || isHighSpecificityTitle(p.candStr));
        // Check significant content-token equivalence (>= 3 tokens, >= 16 chars)
        let sigTokenMatch = false;
        if (exactMatchedPairs.length === 0) {
            const noxSigs = new Map();
            for (const kt of allKnownTitles) {
                const sk = significantTokenKey(stripSearchDecorations(kt));
                if (sk && sk.split(' ').length >= 3 && sk.length >= 16)
                    noxSigs.set(sk, kt);
            }
            for (const cs of candStrings) {
                const csk = significantTokenKey(stripSearchDecorations(cs));
                if (csk && noxSigs.has(csk)) {
                    sigTokenMatch = true;
                    break;
                }
            }
        }
        const fallbackMatch = matchWorkCandidate({
            title: stripSearchDecorations(nox.title),
            slug: nox.slug,
            aliases: allKnownTitles,
            author: nox.author || undefined,
            artist: nox.artist || undefined,
            kind: nox.kind || undefined,
            year: nox.year,
        }, {
            title: stripSearchDecorations(cand.primaryTitle),
            slug: cand.slug,
            aliases: candStrings,
            author: cand.authors?.[0],
            artist: cand.artists?.[0],
            kind: cand.kind,
            year: cand.year,
        });
        scored.push({
            cand,
            exactMatchedPairs,
            distinctExactNormCount: matchedNormSet.size,
            hasHighSpecificityExact,
            creatorMatched,
            sharedLink,
            yearCompatible,
            sigTokenMatch,
            fuzzyScore: fallbackMatch.matched ? fallbackMatch.confidenceScore : 0,
            fuzzyMethod: fallbackMatch.matchMethod,
        });
    }
    if (!scored.length) {
        return {
            status: 'NOT_FOUND',
            candidate: null,
            confidence: 0,
            matchMethod: null,
            matchReason: 'ALL_CANDIDATES_REJECTED_OR_UNMATCHED',
            evidence: { totalEvaluated: uniqueCandidates.length },
        };
    }
    // 1. Persistent Cross-ID match with title/creator consistency
    const linkMatches = scored.filter((s) => s.sharedLink && (s.exactMatchedPairs.length > 0 || s.creatorMatched || s.fuzzyScore >= 0.85));
    if (linkMatches.length === 1) {
        const win = linkMatches[0];
        return {
            status: 'CONFIRMED_MATCH',
            candidate: win.cand,
            confidence: 0.99,
            matchMethod: 'PERSISTENT_EXTERNAL_ID',
            matchReason: `Verified via persistent cross-link (${win.sharedLink})`,
            evidence: {
                sharedLink: win.sharedLink,
                matchedPairs: win.exactMatchedPairs.slice(0, 3),
                creatorMatched: win.creatorMatched,
            },
        };
    }
    // 2. Exact title/alias matches
    const exactHits = scored.filter((s) => s.exactMatchedPairs.length > 0);
    if (exactHits.length > 0) {
        // Sort by: creatorMatched desc -> distinctExactNormCount desc -> hasHighSpecificityExact desc
        exactHits.sort((a, b) => {
            if (a.creatorMatched !== b.creatorMatched)
                return a.creatorMatched ? -1 : 1;
            if (a.distinctExactNormCount !== b.distinctExactNormCount) {
                return b.distinctExactNormCount - a.distinctExactNormCount;
            }
            if (a.hasHighSpecificityExact !== b.hasHighSpecificityExact) {
                return a.hasHighSpecificityExact ? -1 : 1;
            }
            if (a.yearCompatible !== b.yearCompatible)
                return a.yearCompatible ? -1 : 1;
            return 0;
        });
        const best = exactHits[0];
        const competingEqualHomonyms = exactHits.filter((other) => other !== best &&
            other.creatorMatched === best.creatorMatched &&
            other.distinctExactNormCount >= best.distinctExactNormCount);
        // Unambiguously corroborated by creator or >= 2 distinct independent aliases
        if ((best.creatorMatched || best.distinctExactNormCount >= 2) &&
            competingEqualHomonyms.length === 0) {
            return {
                status: 'CONFIRMED_MATCH',
                candidate: best.cand,
                confidence: best.creatorMatched ? 0.99 : 0.98,
                matchMethod: best.creatorMatched ? 'EXACT_TITLE_WITH_CREATOR' : 'MULTI_ALIAS_EXACT',
                matchReason: best.creatorMatched
                    ? `Exact title/alias match corroborated by creator`
                    : `Corroborated by ${best.distinctExactNormCount} distinct matching titles/aliases`,
                evidence: {
                    matchedPairs: best.exactMatchedPairs.slice(0, 4),
                    creatorMatched: best.creatorMatched,
                    competingCount: competingEqualHomonyms.length,
                },
            };
        }
        // High-specificity single exact match with ZERO competing homonyms in search response
        if (best.hasHighSpecificityExact && competingEqualHomonyms.length === 0) {
            return {
                status: 'CONFIRMED_MATCH',
                candidate: best.cand,
                confidence: 0.96,
                matchMethod: 'EXACT_SPECIFIC_TITLE',
                matchReason: `Unique high-specificity exact match ("${best.exactMatchedPairs[0].candStr}")`,
                evidence: {
                    matchedPairs: best.exactMatchedPairs.slice(0, 3),
                    yearCompatible: best.yearCompatible,
                },
            };
        }
        // Low-specificity single-word/short title corroborated by BOTH exact year (+/-1) AND compatible non-empty kind + 0 homonyms
        const matchedNorm = best.exactMatchedPairs[0]?.norm || '';
        const isUltraGenericWord = COMMON_GENERIC_WORDS.has(matchedNorm);
        if (!isUltraGenericWord &&
            matchedNorm.length >= 4 &&
            best.yearCompatible &&
            nox.kind &&
            best.cand.kind &&
            nox.kind.toLowerCase() === best.cand.kind.toLowerCase() &&
            competingEqualHomonyms.length === 0) {
            return {
                status: 'CONFIRMED_MATCH',
                candidate: best.cand,
                confidence: 0.93,
                matchMethod: 'EXACT_TITLE_YEAR_KIND',
                matchReason: `Unique exact match corroborated by year (${best.cand.year}) and kind (${best.cand.kind})`,
                evidence: {
                    matchedPairs: best.exactMatchedPairs.slice(0, 2),
                    year: best.cand.year,
                    kind: best.cand.kind,
                },
            };
        }
        // Otherwise: homonymous or short generic title without sufficient disambiguation -> AMBIGUOUS_MATCH!
        return {
            status: 'AMBIGUOUS_MATCH',
            candidate: best.cand,
            confidence: 0.65,
            matchMethod: competingEqualHomonyms.length > 0 ? 'HOMONYM_CONFLICT' : 'SHORT_GENERIC_UNCORROBORATED',
            matchReason: competingEqualHomonyms.length > 0
                ? `${competingEqualHomonyms.length + 1} competing works matched "${best.exactMatchedPairs[0]?.candStr}" without creator/ID tie-breaker`
                : `Short/generic title "${best.exactMatchedPairs[0]?.candStr}" lacks creator, year, or secondary alias corroboration`,
            evidence: {
                matchedPairs: best.exactMatchedPairs.slice(0, 2),
                competingCandidates: competingEqualHomonyms.slice(0, 3).map((c) => ({
                    id: c.cand.sourceWorkId,
                    title: c.cand.primaryTitle,
                    authors: c.cand.authors,
                    year: c.cand.year,
                })),
            },
        };
    }
    // 3. Significant content-token match (>= 3 content words, >= 16 chars)
    const sigHits = scored.filter((s) => s.sigTokenMatch);
    if (sigHits.length === 1) {
        return {
            status: 'CONFIRMED_MATCH',
            candidate: sigHits[0].cand,
            confidence: 0.94,
            matchMethod: 'SIGNIFICANT_TOKENS_EXACT',
            matchReason: 'Unique 3+ content-word equivalence match',
            evidence: { primaryTitle: sigHits[0].cand.primaryTitle },
        };
    }
    // 4. High fuzzy similarity: only CONFIRMED if creator also matches, otherwise AMBIGUOUS
    const fuzzyHits = scored.filter((s) => s.fuzzyScore >= 0.88).sort((a, b) => b.fuzzyScore - a.fuzzyScore);
    if (fuzzyHits.length > 0) {
        const top = fuzzyHits[0];
        if (top.creatorMatched && fuzzyHits.filter((f) => f.creatorMatched).length === 1) {
            return {
                status: 'CONFIRMED_MATCH',
                candidate: top.cand,
                confidence: 0.92,
                matchMethod: 'FUZZY_WITH_CREATOR',
                matchReason: `High similarity (${top.fuzzyScore.toFixed(2)}) corroborated by creator`,
                evidence: { fuzzyScore: top.fuzzyScore, primaryTitle: top.cand.primaryTitle },
            };
        }
        return {
            status: 'AMBIGUOUS_MATCH',
            candidate: top.cand,
            confidence: Number(Math.min(0.75, top.fuzzyScore * 0.8).toFixed(4)),
            matchMethod: 'FUZZY_UNCORROBORATED',
            matchReason: `Fuzzy candidate "${top.cand.primaryTitle}" (${top.fuzzyScore.toFixed(2)}) requires manual identity verification`,
            evidence: { fuzzyScore: top.fuzzyScore, primaryTitle: top.cand.primaryTitle, authors: top.cand.authors },
        };
    }
    return {
        status: 'NOT_FOUND',
        candidate: null,
        confidence: 0,
        matchMethod: null,
        matchReason: 'NO_CONFIDENT_CANDIDATE',
        evidence: { topEvaluatedTitle: scored[0]?.cand.primaryTitle },
    };
}
/**
 * Computes the exact merged `works.aliases` array and delta metrics without ever removing existing valid aliases.
 */
export function computeMergedWorkAliases(nox, incomingLocalized) {
    const existing = Array.isArray(nox.aliases) ? nox.aliases : [];
    const existingKeys = new Set(existing.map((a) => alternativeTitleKey(a)).filter(Boolean));
    const primaryKey = alternativeTitleKey(nox.title);
    // Order incoming so PT-BR and English/Romaji/CJK variants never get truncated by max=50
    const langMap = new Map();
    const orderedStrings = [];
    const prioritized = [...incomingLocalized].sort((a, b) => {
        const aPt = isPortugueseAlias(a.title, a.language) ? 0 : 1;
        const bPt = isPortugueseAlias(b.title, b.language) ? 0 : 1;
        return aPt - bPt;
    });
    for (const item of prioritized) {
        const cleaned = cleanAlternativeTitle(item.title);
        if (!cleaned || !isPersistableAliasScript(cleaned))
            continue;
        const k = alternativeTitleKey(cleaned);
        if (!k)
            continue;
        if (!langMap.has(k) && item.language)
            langMap.set(k, item.language);
        orderedStrings.push(cleaned);
    }
    const merged = mergeAlternativeTitles(existing, orderedStrings, nox.title, 60);
    // Guarantee zero accidental loss of pre-existing aliases that were already persisted on `works.aliases`
    // unless `mergeAlternativeTitles` normalized whitespace/deduped identical keys.
    const addedAliases = [];
    const ptBrAliasesAdded = [];
    for (const alias of merged) {
        const k = alternativeTitleKey(alias);
        if (!k || k === primaryKey || existingKeys.has(k))
            continue;
        addedAliases.push(alias);
        if (isPortugueseAlias(alias, langMap.get(k))) {
            ptBrAliasesAdded.push(alias);
        }
    }
    const aliasesChanged = addedAliases.length > 0 &&
        (merged.length !== existing.length || merged.some((v, idx) => v !== existing[idx]));
    return {
        aliasesAfter: aliasesChanged ? merged : existing,
        addedAliases: aliasesChanged ? addedAliases : [],
        ptBrAliasesAdded: aliasesChanged ? ptBrAliasesAdded : [],
        aliasesChanged,
    };
}
