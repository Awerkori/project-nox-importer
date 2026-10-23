import { decodeHtmlEntities } from '../sources/common/html-utils.js';
const GENRE_OR_TAXONOMY_WORDS = new Set([
    'adulto', 'drama', 'fantasia', 'historico', 'histórico', 'josei', 'manhwa', 'manga',
    'manhua', 'romance', 'smut', 'shounen', 'shoujo', 'seinen', 'yaoi', 'yuri', 'ecchi',
    'action', 'adventure', 'comedy', 'mystery', 'supernatural', 'tragedy', 'horror', 'sci-fi',
    'aventura', 'mecha', 'updating', 'xem phim', 'galax scans', 'hen', 'san.',
    'slice of life', 'unknown', 'n/a', 'none', 'null', 'undefined', 'untitled', 'sem titulo', 'sem título',
    'san', 'kun', 'chan', 'sama', 'sensei', 'senpai', 'dono',
    'erotico', 'erótico', 'psicologico', 'psicológico', 'fim da temporada', 'vida cotidiana',
    'official colored', 'hq colorida digital', 'digital colored comics', 'team argo',
]);
/**
 * Display values are retained exactly enough for readers; comparison keys are
 * deliberately separate.  This avoids losing native scripts while still
 * coalescing cosmetic source variants such as whitespace and curly quotes.
 */
export function cleanAlternativeTitle(value) {
    if (typeof value !== 'string')
        return null;
    const decoded = decodeHtmlEntities(value)
        .normalize('NFKC')
        .replace(/\s+/gu, ' ')
        .replace(/^[,;:\s]+|[,;:\s]+$/gu, '')
        .trim();
    if (decoded.length < 1 || decoded.length > 250)
        return null;
    // Discard brackets, punctuation-only, or garbage fragments like "[]", "[ ]", "shot)", "-"
    if (/^[\s\[\]\(\)\{\}\-_.,:;!?/'"\\|~+*#@$%^&=>`<´]+$/u.test(decoded))
        return null;
    if (/^(shot|one-shot|oneshot|colorido|raw|novel|doujinshi)\)?$/i.test(decoded))
        return null;
    // Discard age ratings ("+18", "18+"), zero, or 3+ digit numeric machine IDs ("10803", "873"),
    // while preserving legitimate short numeric titles like "86".
    if (/^(?:\+\d+\+?|\d+\+|\d{3,}|0)$/u.test(decoded))
        return null;
    // Discard star rating widgets or vote summaries mistakenly ingested from themes
    if (/\b(?:m[eé]dia\s+\d|\d+(?:[.,]\d+)?\s+do\s+total\s+de\s+\d+\s+votos|\d+(?:[.,]\d+)?\s+de\s+5(?:\s+estrelas)?|\bvotos?\b|\brating\b|\bavalia[çc][aã]o\b)/i.test(decoded))
        return null;
    // Discard CSS rules, stylesheet custom properties, CSS units/colors, JS snippets, comments, and direct URLs
    if (/[{}@=]|\/\*|\*\/|^--|-->|var\(|calc\(|oklch\(|repeat\(auto-fill|!important|\b(?:rgb|rgba|hsl|hsla|border|background|color|padding|margin|font|line-height|overflow|grid|width|height|display|position|transition|transform|opacity|z-index|float|text-align|text-transform|min-width|max-height|flex-direction|aspect-ratio|place-content|scrollbar-width|object-fit|mask-image|src|unicode-range|fill|align-self|align-items|white-space|flex-grow|initial-value|inherits|animation)\s*[:(]|\b(?:top|left|right|bottom)\s*:\s*[-0-9]|\b\d+(?:\.\d+)?(?:px|rem|em|vh|vw|ms|fr|%)\b|^#[0-9a-f]{3,8}\b|^\d+(?:\.\d+)?\)$|^[.#][a-z_-]|\b(?:googleapis|googleusercontent|gstatic|blogblog|iconify|document\.getElementById|new\s+Array|new\s+URL|filterGenre|noThumbnail|resizeImage|_config\.arr)\b|^(?:img|b|-1)$/i.test(decoded) ||
        /^['"].*['"]\]?$/.test(decoded) ||
        /^san\s+(?:is|ga|wa)\b/i.test(decoded))
        return null;
    if (/^https?:\/\//i.test(decoded))
        return null;
    // Discard isolated genres or metadata placeholders mistakenly ingested as aliases
    if (GENRE_OR_TAXONOMY_WORDS.has(decoded.toLocaleLowerCase()))
        return null;
    return decoded;
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
const CLAUSE_CONTINUATION_REGEX = /^(?:for|and|or|so|because|but|yet|too|also|with|without|nor|mas|e|ou|pois|porque|com|sem|para|por)\b/i;
const VOCATIVE_HONORIFIC_REGEX = /(?:-(?:san|kun|chan|sama|sensei|senpai|dono)\b|\b(?:your\s+(?:grace|highness|majesty)|vossa\s+(?:gra[çc]a|alteza|majestade)|duque|duke|milady|my\s+lord|meu\s+senhor)[!?.]*$)/i;
const INTERJECTION_PREFIXES = new Set([
    'hello', 'oh', 'ah', 'hey', 'dear', 'bye', 'goodbye', 'sayonara', 'adeus', 'ola', 'olá',
    'mom', 'mae', 'mãe', 'dad', 'pai', 'wait', 'espere', 'sorry', 'desculpa', 'perdao', 'perdão',
    'please', 'onegai', 'ijiranaide', 'ijirimakutte', 'uzaki', 'komi',
]);
function extractPunctuatedClauses(title) {
    const clauses = new Set();
    if (!title || typeof title !== 'string' || !title.includes(','))
        return clauses;
    // Only forbid comma-split fragments of a single grammatical sentence title
    // (e.g. "Summon The Demon, For I Am The Abyss" or "Hello, Fluffy Griffin!" or "Ijiranaide, Nagatoro-san").
    // Never forbid legitimate subtitles or independent titles separated by ':', '-', ';', or '|'.
    const parts = title.split(/(?<!\d),(?!\d{3})\s*/).map((p) => p.trim()).filter(Boolean);
    if (parts.length > 1) {
        const firstWord = parts[0].toLowerCase().replace(/[^a-z\u00C0-\u017F]/g, '');
        const isGrammaticalTitle = INTERJECTION_PREFIXES.has(firstWord)
            || VOCATIVE_HONORIFIC_REGEX.test(title)
            || parts.some((p, i) => i > 0 && (CLAUSE_CONTINUATION_REGEX.test(p) || /^[a-z]/.test(p)));
        if (isGrammaticalTitle) {
            for (const part of parts) {
                const key = alternativeTitleKey(part);
                if (key)
                    clauses.add(key);
            }
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
    // First pass: identify grammatical comma-sentence titles in existing and incoming to prevent half-sentence fragments
    for (const raw of [...(existing || []), ...(incoming || [])]) {
        if (typeof raw === 'string' && raw.includes(',')) {
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
