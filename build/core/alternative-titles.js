const GENRE_OR_TAXONOMY_WORDS = new Set([
    'adulto', 'drama', 'fantasia', 'historico', 'histórico', 'josei', 'manhwa', 'manga',
    'manhua', 'romance', 'smut', 'shounen', 'shoujo', 'seinen', 'yaoi', 'yuri', 'ecchi',
    'action', 'adventure', 'comedy', 'mystery', 'supernatural', 'tragedy', 'horror', 'sci-fi',
    'slice of life', 'unknown', 'n/a', 'none', 'null', 'undefined', 'untitled', 'sem titulo', 'sem título',
]);
/**
 * Display values are retained exactly enough for readers; comparison keys are
 * deliberately separate.  This avoids losing native scripts while still
 * coalescing cosmetic source variants such as whitespace and curly quotes.
 */
export function cleanAlternativeTitle(value) {
    if (typeof value !== 'string')
        return null;
    const title = value.normalize('NFKC').replace(/\s+/gu, ' ').trim();
    if (title.length < 2 || title.length > 250)
        return null;
    // Discard pure numeric IDs, chapters, volumes or age ratings like "+18", "18+", "10803", "873"
    if (/^\+?\d+\+?$/u.test(title))
        return null;
    // Discard isolated genres or metadata placeholders mistakenly ingested as aliases
    if (GENRE_OR_TAXONOMY_WORDS.has(title.toLocaleLowerCase()))
        return null;
    return title;
}
/**
 * Comparison-only key. It intentionally does not transliterate or translate
 * titles: Japanese, Korean, Chinese and romanized variants remain distinct
 * aliases unless a source supplies the same value again.
 */
export function alternativeTitleKey(value) {
    const title = cleanAlternativeTitle(value);
    if (!title)
        return '';
    return title
        .toLocaleLowerCase()
        .replace(/[‘’`´]/gu, "'")
        .replace(/[\-‐-―_.,;:!?()[\]{}"'・]/gu, ' ')
        .replace(/\s+/gu, ' ')
        .trim();
}
function extractPunctuatedClauses(title) {
    const clauses = new Set();
    if (!title || typeof title !== 'string')
        return clauses;
    const parts = title.split(/[,:;\-–—|]/).map((p) => p.trim()).filter(Boolean);
    if (parts.length > 1) {
        for (const part of parts) {
            const key = alternativeTitleKey(part);
            if (key)
                clauses.add(key);
        }
    }
    return clauses;
}
/** Merge aliases without changing the first valid display spelling. */
export function mergeAlternativeTitles(existing, incoming, primaryTitle, max = 50) {
    const merged = [];
    const seen = new Set();
    const primaryKey = alternativeTitleKey(primaryTitle);
    const rawPrimaryTitle = typeof primaryTitle === 'string' ? primaryTitle : '';
    const forbiddenSubClauses = extractPunctuatedClauses(rawPrimaryTitle);
    // First pass: identify full titles in existing and incoming to prevent fragments
    for (const raw of [...(existing || []), ...(incoming || [])]) {
        if (typeof raw === 'string' && (raw.includes(',') || raw.includes(';') || raw.includes(' - '))) {
            const clauses = extractPunctuatedClauses(raw);
            for (const c of clauses)
                forbiddenSubClauses.add(c);
        }
    }
    for (const raw of [...(existing || []), ...(incoming || [])]) {
        const title = cleanAlternativeTitle(raw);
        const key = alternativeTitleKey(title);
        if (!title || !key || key === primaryKey || seen.has(key))
            continue;
        // Discard fragments of primary title or compound titles (e.g. "Summon The Demon" from "Summon The Demon, For I Am The Abyss")
        if (forbiddenSubClauses.has(key))
            continue;
        seen.add(key);
        merged.push(title);
        if (merged.length >= max)
            break;
    }
    return merged;
}
