/**
 * Cross-Provider Matching and Normalization Engine
 * Provides deterministic and robust title matching across different manga sources.
 */
const NOISE_TOKENS = new Set([
    'manga',
    'manhwa',
    'manhua',
    'webtoon',
    'comic',
    'scan',
    'scans',
    'scanlator',
    'fansub',
    'pt-br',
    'ptbr',
    'pt',
    'br',
    'portugues',
    'portuguese',
    'online',
    'color',
    'raw',
    'oficial',
    'official',
]);
/**
 * Normalizes title string by removing accents, lowercasing, stripping punctuation
 * and filtering common scan/format noise words.
 */
export function normalizeTitle(text) {
    if (!text)
        return '';
    // 1. Remove diacritics / accents
    let normalized = text
        .normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase();
    // 2. Replace punctuation, brackets, special symbols with spaces
    normalized = normalized.replace(/['’`´"״]/g, ''); // keep words joined without apostrophes (e.g. Academy's -> Academys)
    normalized = normalized.replace(/[^\w\s]/g, ' ');
    // 3. Filter noise tokens
    const tokens = normalized
        .split(/\s+/)
        .map((t) => t.trim())
        .filter((t) => t.length > 0 && !NOISE_TOKENS.has(t));
    return tokens.join(' ');
}
/**
 * Converts a title to a standardized URL-friendly slug.
 */
export function slugifyTitle(text) {
    return text
        .normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
}
/**
 * Extracts season identifier if present (e.g., 's2', 'season 2', '2a temporada')
 */
export function extractSeason(text) {
    const norm = text.toLowerCase();
    const match = norm.match(/(?:season|temporada|temp|s)\s*(\d+)/i) || norm.match(/(\d+)[ªaºo]?\s*temporada/i);
    return match ? `s${match[1]}` : null;
}
/**
 * Checks whether text indicates a light novel / webnovel
 */
export function isNovel(text, kind) {
    const t = (text + ' ' + (kind || '')).toLowerCase();
    return /\b(novel|light novel|webnovel|ln|wn)\b/i.test(t);
}
/**
 * Checks whether text indicates a spin-off, side story, or gaiden
 */
export function extractSpinOff(text) {
    return /\b(side story|gaiden|spin-off|spinoff|extra story)\b/i.test(text.toLowerCase());
}
/**
 * Computes Sørensen-Dice coefficient between two strings based on bigrams.
 */
export function diceSimilarity(str1, str2) {
    const s1 = str1.trim();
    const s2 = str2.trim();
    if (s1 === s2)
        return 1.0;
    if (s1.length < 2 || s2.length < 2)
        return 0.0;
    const getBigrams = (str) => {
        const bigrams = new Map();
        for (let i = 0; i < str.length - 1; i++) {
            const bg = str.substring(i, i + 2);
            bigrams.set(bg, (bigrams.get(bg) || 0) + 1);
        }
        return bigrams;
    };
    const bg1 = getBigrams(s1);
    const bg2 = getBigrams(s2);
    let intersection = 0;
    for (const [bg, count1] of bg1.entries()) {
        if (bg2.has(bg)) {
            intersection += Math.min(count1, bg2.get(bg));
        }
    }
    const total = (s1.length - 1) + (s2.length - 1);
    return total > 0 ? (2.0 * intersection) / total : 0.0;
}
/**
 * Computes Levenshtein edit-distance similarity ratio [0, 1].
 */
export function levenshteinSimilarity(s1, s2) {
    const a = s1.trim();
    const b = s2.trim();
    if (a === b)
        return 1.0;
    if (a.length === 0 || b.length === 0)
        return 0.0;
    const matrix = [];
    for (let i = 0; i <= b.length; i++) {
        matrix[i] = [i];
    }
    for (let j = 0; j <= a.length; j++) {
        matrix[0][j] = j;
    }
    for (let i = 1; i <= b.length; i++) {
        for (let j = 1; j <= a.length; j++) {
            if (b.charAt(i - 1) === a.charAt(j - 1)) {
                matrix[i][j] = matrix[i - 1][j - 1];
            }
            else {
                matrix[i][j] = Math.min(matrix[i - 1][j - 1] + 1, // substitution
                matrix[i][j - 1] + 1, // insertion
                matrix[i - 1][j] + 1 // deletion
                );
            }
        }
    }
    const distance = matrix[b.length][a.length];
    const maxLen = Math.max(a.length, b.length);
    return maxLen > 0 ? (maxLen - distance) / maxLen : 0.0;
}
/**
 * Token Jaccard overlap ratio.
 */
export function tokenOverlapRatio(s1, s2) {
    const t1 = new Set(s1.split(/\s+/).filter(Boolean));
    const t2 = new Set(s2.split(/\s+/).filter(Boolean));
    if (t1.size === 0 || t2.size === 0)
        return 0.0;
    let intersection = 0;
    for (const token of t1) {
        if (t2.has(token))
            intersection++;
    }
    const union = new Set([...t1, ...t2]).size;
    return union > 0 ? intersection / union : 0.0;
}
/**
 * Evaluates whether candidate work matches target work with safety constraints.
 */
export function matchWorkCandidate(target, candidate) {
    // Safety Guard 1: Novel vs Comic rejection
    const targetIsNovel = isNovel(target.title, target.kind);
    const candIsNovel = isNovel(candidate.title, candidate.kind);
    if (targetIsNovel !== candIsNovel) {
        return {
            matched: false,
            confidenceScore: 0.0,
            matchMethod: 'NO_MATCH',
            reason: 'Format mismatch: Novel vs Comic',
        };
    }
    // Safety Guard 2: Season mismatch
    const targetSeason = extractSeason(target.title);
    const candSeason = extractSeason(candidate.title);
    if (targetSeason && candSeason && targetSeason !== candSeason) {
        return {
            matched: false,
            confidenceScore: 0.0,
            matchMethod: 'NO_MATCH',
            reason: `Season mismatch: ${targetSeason} vs ${candSeason}`,
        };
    }
    // Safety Guard 3: Spin-off / Side Story mismatch
    const targetSpin = extractSpinOff(target.title);
    const candSpin = extractSpinOff(candidate.title);
    if (targetSpin !== candSpin) {
        return {
            matched: false,
            confidenceScore: 0.0,
            matchMethod: 'NO_MATCH',
            reason: 'Spin-off / Side story mismatch',
        };
    }
    // Normalized strings
    const normTarget = normalizeTitle(target.title);
    const normCand = normalizeTitle(candidate.title);
    // Exact title match (after normalization)
    if (normTarget && normCand && normTarget === normCand) {
        return {
            matched: true,
            confidenceScore: 1.0,
            matchMethod: 'EXACT_TITLE',
        };
    }
    // Compact title match without spaces (e.g. "SandLand" vs "Sand Land", "OnePiece" vs "One Piece")
    if (normTarget.length >= 3 &&
        normCand.length >= 3 &&
        normTarget.replace(/\s+/g, '') === normCand.replace(/\s+/g, '')) {
        return {
            matched: true,
            confidenceScore: 0.98,
            matchMethod: 'EXACT_TITLE',
            reason: 'Compact whitespace-insensitive match',
        };
    }
    // Exact slug match
    const slugTarget = target.slug ? slugifyTitle(target.slug) : slugifyTitle(target.title);
    const slugCand = candidate.slug ? slugifyTitle(candidate.slug) : slugifyTitle(candidate.title);
    if (slugTarget && slugCand && slugTarget === slugCand) {
        return {
            matched: true,
            confidenceScore: 1.0,
            matchMethod: 'EXACT_SLUG',
        };
    }
    // Check known aliases / alternative titles
    const targetAliases = (target.aliases || []).map(normalizeTitle).filter(Boolean);
    const candAliases = (candidate.aliases || []).map(normalizeTitle).filter(Boolean);
    // Check cross-alias matches
    for (const alias of targetAliases) {
        if (alias === normCand ||
            candAliases.includes(alias) ||
            alias.replace(/\s+/g, '') === normCand.replace(/\s+/g, '')) {
            return {
                matched: true,
                confidenceScore: 0.95,
                matchMethod: 'ALIAS_EXACT',
                reason: `Matched alias "${alias}"`,
            };
        }
    }
    for (const alias of candAliases) {
        if (alias === normTarget ||
            alias.replace(/\s+/g, '') === normTarget.replace(/\s+/g, '')) {
            return {
                matched: true,
                confidenceScore: 0.95,
                matchMethod: 'ALIAS_EXACT',
                reason: `Matched candidate alias "${alias}"`,
            };
        }
    }
    // High-Confidence Fuzzy Matching
    if (normTarget.length >= 3 && normCand.length >= 3) {
        const dice = diceSimilarity(normTarget, normCand);
        const lev = levenshteinSimilarity(normTarget, normCand);
        const overlap = tokenOverlapRatio(normTarget, normCand);
        // Combined score favoring token overlap and bigram dice
        const combinedScore = Number((0.45 * dice + 0.35 * lev + 0.20 * overlap).toFixed(4));
        // Also check if one contains the other completely if word count >= 3
        const targetWords = normTarget.split(/\s+/);
        const candWords = normCand.split(/\s+/);
        const isSubstring = (normTarget.includes(normCand) && candWords.length >= 2) ||
            (normCand.includes(normTarget) && targetWords.length >= 2);
        const isMatch = combinedScore >= 0.85 || (isSubstring && overlap >= 0.75);
        if (isMatch) {
            return {
                matched: true,
                confidenceScore: Math.max(combinedScore, isSubstring ? 0.88 : combinedScore),
                matchMethod: 'FUZZY_HIGH',
                reason: `Similarity score: ${combinedScore} (dice: ${dice.toFixed(2)}, lev: ${lev.toFixed(2)}, overlap: ${overlap.toFixed(2)})`,
            };
        }
    }
    return {
        matched: false,
        confidenceScore: 0.0,
        matchMethod: 'NO_MATCH',
    };
}
