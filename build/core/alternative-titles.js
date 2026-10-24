import { decodeHtmlEntities } from '../sources/common/html-utils.js';
const GENRE_OR_TAXONOMY_WORDS = new Set([
    'adulto', 'drama', 'fantasia', 'historico', 'histórico', 'josei', 'manhwa', 'manga',
    'manhua', 'romance', 'smut', 'shounen', 'shoujo', 'seinen', 'yaoi', 'yuri', 'ecchi',
    'action', 'adventure', 'comedy', 'mystery', 'supernatural', 'tragedy', 'horror', 'sci-fi',
    'aventura', 'mecha', 'updating', 'xem phim', 'galax scans', 'hen', 'san.',
    'slice of life', 'unknown', 'n/a', 'none', 'null', 'undefined', 'untitled', 'sem titulo', 'sem título',
    'san', 'kun', 'chan', 'sama', 'sensei', 'senpai', 'dono', 'tan', 'shi', 'tachi', 'sun',
    'erotico', 'erótico', 'psicologico', 'psicológico', 'fim da temporada', 'vida cotidiana',
    'official colored', 'hq colorida digital', 'digital colored comics', 'team argo',
    'ação', 'acao', 'artes marciais', 'isekai', 'reencarnação', 'reencarnacao',
    'comédia', 'comedia', 'escolar', 'ficção científica', 'ficcao cientifica',
    'magia', 'monstros', 'apocalipse', 'guerra', 'sobrenatural', 'mistério', 'misterio',
    'terror', 'tragédia', 'tragedia', 'esportes', 'harém', 'harem', 'cultivo', 'murim',
    'atualização', 'atualizacao', 'atualizando', 'one shot', 'oneshot', 'one-shot', 'hentai',
    'webtoon', 'shoujo manga', 'sister', 'desculpa',
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
        .replace(/\\(["'])/g, '$1')
        .replace(/\s+/gu, ' ')
        .replace(/^(?:\[(?:H|A|MOTION|18\+|R-?18|NSFW|DJ|Doujin(?:shi)?)\]\s*)+/iu, '')
        .replace(/\s*\(\s*(?:Official|Decensored|Uncensored)\s*\)\s*$/iu, '')
        .replace(/^[,;:\s]+|[,;:\s]+$/gu, '')
        .trim();
    if (decoded.length < 1 || decoded.length > 250)
        return null;
    // Discard lowercase ASCII hyphenated machine slugs (3+ segments, e.g. "sobrevivendo-como-a-esposa-de-um-assassino-em-serie")
    if (/^[a-z0-9]+(?:-[a-z0-9]+){2,}$/.test(decoded))
        return null;
    // Discard brackets, punctuation-only, or garbage fragments like "[]", "[ ]", "shot)", "-"
    if (/^[\s\[\]\(\)\{\}\-_.,:;!?/'"\\|~+*#@$%^&=>`<´“”‘’]+$/u.test(decoded))
        return null;
    if (/^(shot|one[\s-]*shot|colorido|raw|novel|doujinshi|pre-serialization|serialization)\s*\)?$/i.test(decoded))
        return null;
    // Discard age ratings ("+18", "18+"), zero, 3+ digit numeric machine IDs ("10803", "873"),
    // Google Fonts weight pairs ("0,700", "1,400"), 24-char MongoDB hex IDs, or UUIDs, while preserving legitimate short numeric titles like "86".
    if (/^(?:\+\d+\+?|\d+\+|\d{3,}|0|\d+,\d+|[0-9a-f]{24}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$/iu.test(decoded))
        return null;
    // Discard star rating widgets or vote summaries mistakenly ingested from themes
    if (/\b(?:m[eé]dia\s+\d|\d+(?:[.,]\d+)?\s+do\s+total\s+de\s+\d+\s+votos|\d+(?:[.,]\d+)?\s+de\s+5(?:\s+estrelas)?|\bvotos?\b|\brating\b|\bavalia[çc][aã]o\b)/i.test(decoded))
        return null;
    // Discard CSS rules, stylesheet custom properties, CSS units/colors, JS snippets, comments, and direct URLs
    if (/[{}@=]|\/\*|\*\/|^--|-->|var\(|calc\(|oklch\(|cubic-bezier\(|counter\(|repeat\(auto-fill|!important|\b(?:rgb|rgba|hsl|hsla|border|background|color|padding|margin|font|line-height|overflow|grid|width|height|display|position|transition|transform|opacity|z-index|float|text-align|text-transform|min-width|max-height|flex-direction|aspect-ratio|place-content|scrollbar-width|object-fit|mask-image|src|unicode-range|fill|align-self|align-items|white-space|flex-grow|initial-value|inherits|animation|content)(?:-[a-z]+)*\s*[:(]|\b(?:top|left|right|bottom)\s*:\s*[-0-9]|\b\d+(?:\.\d+)?(?:px|rem|em|vh|vw|ms|fr|deg)\b|\b\d+\.\d+s\b|\b(?:border-box|content-box|linear-gradient|radial-gradient|conic-gradient|ease-in-out|ease-in|ease-out)\b|^#[0-9a-f]{3,8}\b|^\d+(?:\.\d+)?\)$|^[.#][a-z_-]|_config\.|\b(?:googleapis|googleusercontent|gstatic|blogblog|iconify|document\.getElementById|Array\.prototype|this\.getFeed|disqusShortname|filterStatus|new\s+Array|new\s+URL|filterGenre|noThumbnail|resizeImage)\b|^(?:img|b|-1)$/i.test(decoded) ||
        /^['"].*['"]\]?$/.test(decoded) ||
        /^(?:san|kun|chan|sama|sensei|senpai|dono)(?:['’]s\b|[!?:.]|\s+(?:is|ga|wa|no|to|ni|wo|o|mo|de|ka|na|da|tte|tachi|wants|said|chi|sensei|\(one))\b/.test(decoded))
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
        .replace(/[“”«»„‟\\\-‐-―_.,;:!?()[\]{}"'・]/gu, ' ')
        .replace(/\s+/gu, ' ')
        .trim();
}
const CLAUSE_CONTINUATION_REGEX = /^(?:for|and|or|so|because|but|yet|too|also|with|without|nor|mas|e|ou|pois|porque|com|sem|para|por|why|how|when|where|who|what|don't|can't|won't|isn't|aren't|right|let's|i|i'm|i'll|i've|i'd|you|you're|you'll|we|we're|he|she|it|it's|my|his|her|their|eu|você|voce|nós|nos|ele|ela|meu|minha|seu|sua|então|entao|agora|only|even|still|already|never|always|once|again|novamente|alone|não\s+[eé]|nao\s+[eé])\b/i;
const SUBORDINATE_START_REGEX = /^(?:when|while|after|before|since|although|though|even\s+though|if|unless|until|despite|due\s+to|without|with|in|on|at|for|by|from|to|quando|enquanto|depois|ap[oó]s|antes|desde|embora|mesmo|se|sem|com|em|no|na|nos|nas|para|por|contanto)\b/i;
const VOCATIVE_HONORIFIC_REGEX = /(?:-(?:san|kun|chan|sama|sensei|senpai|dono)\b|\b(?:your\s+(?:grace|highness|majesty)|vossa\s+(?:gra[çc]a|alteza|majestade)|duque|duke|duquesa|duchess|margrave|milady|my\s+lord|meu\s+senhor|husband|marido|sweetheart|querid[ao]|miss\s+\w+|mr\.?\s+\w+|senhor(?:a)?\s+\w+|irm[aã]o?|senior\s+sister|immortal\s+fairy|fada)[!?.]*$)/i;
const INTERJECTION_PREFIXES = new Set([
    'hello', 'oh', 'ah', 'hey', 'dear', 'darling', 'honey', 'baby', 'babe', 'bye', 'goodbye', 'sayonara', 'adeus', 'ola', 'olá',
    'mom', 'mae', 'mãe', 'dad', 'pai', 'wait', 'espere', 'sorry', 'desculpa', 'perdao', 'perdão',
    'please', 'onegai', 'ganbare', 'ijiranaide', 'ijirimakutte', 'uzaki', 'komi', 'hyung', 'oppa', 'noona', 'unnie',
    'querido', 'querida', 'amor', 'mestre', 'master', 'like',
]);
function hasParallelTitleWordOverlap(parts) {
    const stopWords = new Set(['para', 'como', 'mais', 'pela', 'pelo', 'uma', 'with', 'from', 'that', 'this', 'into', 'over', 'your', 'mine']);
    const wordSets = parts.map((p) => {
        const words = p
            .toLowerCase()
            .replace(/[^a-z\u00C0-\u017F0-9\s]/g, ' ')
            .split(/\s+/)
            .filter(Boolean);
        return {
            totalWords: words.length,
            sigWords: new Set(words.filter((w) => w.length >= 4 && !stopWords.has(w))),
        };
    });
    for (let i = 0; i < wordSets.length; i++) {
        for (let j = i + 1; j < wordSets.length; j++) {
            if (wordSets[i].totalWords >= 2 && wordSets[j].totalWords >= 2) {
                for (const w of wordSets[i].sigWords) {
                    if (wordSets[j].sigWords.has(w))
                        return true;
                }
            }
        }
    }
    return false;
}
function extractPunctuatedClauses(title, isPrimaryTitle = false) {
    const clauses = new Set();
    if (!title || typeof title !== 'string' || !title.includes(','))
        return clauses;
    // Also forbid naive comma-split fragments of numbers (e.g. "10,000" -> "Logging 10" + "000 Years...")
    // or parentheses containing commas (e.g. "(No, It Can't)" -> "... (No" + "It Can't)")
    if (/\d,\d{3}\b/.test(title) || /\([^)]*,[^)]*\)/.test(title)) {
        for (const rawPart of title.split(/,\s*/)) {
            const k = alternativeTitleKey(rawPart);
            if (k)
                clauses.add(k);
        }
    }
    // Never treat unspaced-comma lists or quote-comma-quote lists as grammatical sentences
    if (/"\s*,\s*"|(?<!\d),(?![\s\d])/.test(title))
        return clauses;
    const parts = title.split(/,(?!\d{3}\b)\s*/).map((p) => p.trim()).filter(Boolean);
    if (parts.length > 1 && parts.every((p) => /[a-zA-Z\u00C0-\u017F]/.test(p) && !/[\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af\u0400-\u04ff]/.test(p))) {
        const firstWord = parts[0].toLowerCase().replace(/[^a-z\u00C0-\u017F]/g, '');
        const isGrammaticalTitle = isPrimaryTitle ||
            parts.some((p, i) => i > 0 && /^[a-z\u00E0-\u00FC]/.test(p)) ||
            (!hasParallelTitleWordOverlap(parts) && (INTERJECTION_PREFIXES.has(firstWord) ||
                /^por\s+favor\b/i.test(parts[0]) ||
                SUBORDINATE_START_REGEX.test(parts[0]) ||
                VOCATIVE_HONORIFIC_REGEX.test(title) ||
                parts.some((p, i) => i > 0 && (CLAUSE_CONTINUATION_REGEX.test(p) || /\b(?:and|or|e|ou)\b/i.test(p)))));
        if (isGrammaticalTitle) {
            // Forbid all strict contiguous sub-slices (single parts and multi-part sub-clauses)
            for (let i = 0; i < parts.length; i++) {
                for (let j = i; j < parts.length; j++) {
                    if (i === 0 && j === parts.length - 1)
                        continue;
                    const subSlice = parts.slice(i, j + 1).join(', ');
                    const key = alternativeTitleKey(subSlice);
                    if (key)
                        clauses.add(key);
                }
            }
        }
    }
    return clauses;
}
const NON_LATIN_SCRIPT_REGEX = /[\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af\u0400-\u04ff]/;
function expandMultiScriptOrSlashList(raw) {
    if (typeof raw !== 'string')
        return [raw];
    if (/(?<!:)\/\/|\|\|/.test(raw)) {
        const parts = raw.split(/\s*(?:(?<!:)\/\/|\|\|)\s*/).filter(Boolean);
        if (parts.length > 1)
            return parts;
    }
    // Handle space-concatenated "<Latin multi-word title> <CJK title>" (e.g. "Wo Yige Shen Hao, Dang Zhanan Hen Heli ba 我一个神豪,当渣男很合理吧")
    const latinThenCjk = raw
        .trim()
        .match(/^([A-Za-z\u00C0-\u024F0-9'’"“”,.!?:;\-() ]{8,}?)\s+([\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af][\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af0-9,，.!?:;~\-() ]{2,})$/u);
    if (latinThenCjk) {
        return [latinThenCjk[1].trim(), latinThenCjk[2].trim()];
    }
    if (raw.includes(',') && !/\([^)]*,[^)]*\)/.test(raw)) {
        const parts = raw.split(/,(?!\d{3}\b)\s*/).map((p) => p.trim()).filter(Boolean);
        if (parts.length >= 2) {
            const hasMixedLatinAndCjkPart = parts.some((p) => /[A-Za-z\u00C0-\u024F]{3,}/u.test(p) && NON_LATIN_SCRIPT_REGEX.test(p));
            if (!hasMixedLatinAndCjkPart) {
                const hasNonLatin = parts.some((p) => NON_LATIN_SCRIPT_REGEX.test(p));
                const hasPureLatin = parts.some((p) => !NON_LATIN_SCRIPT_REGEX.test(p) && /[a-zA-Z\u00C0-\u017F]/.test(p));
                const allSameKey = new Set(parts.map((p) => alternativeTitleKey(p)).filter(Boolean)).size === 1;
                if ((hasNonLatin && hasPureLatin) || allSameKey) {
                    return parts;
                }
            }
        }
    }
    return [raw];
}
/** Merge aliases without changing the first valid display spelling. */
export function mergeAlternativeTitles(existing, incoming, primaryTitle, max = 50) {
    const merged = [];
    const seen = new Set();
    const primaryKey = alternativeTitleKey(primaryTitle);
    const rawPrimaryTitle = typeof primaryTitle === 'string' ? primaryTitle : '';
    const forbiddenSubClauses = extractPunctuatedClauses(rawPrimaryTitle, true);
    const expandedInputs = [...(existing || []), ...(incoming || [])].flatMap(expandMultiScriptOrSlashList);
    const allRawStrings = [];
    if (rawPrimaryTitle)
        allRawStrings.push(rawPrimaryTitle);
    // First pass: identify grammatical comma-sentence titles in existing and incoming to prevent half-sentence fragments
    for (const raw of expandedInputs) {
        if (typeof raw === 'string') {
            const cleaned = cleanAlternativeTitle(raw);
            if (cleaned)
                allRawStrings.push(cleaned);
            if (raw.includes(',')) {
                const clauses = extractPunctuatedClauses(raw);
                for (const c of clauses)
                    forbiddenSubClauses.add(c);
            }
        }
    }
    // Also forbid unspaced hyphen-split suffixes (and paired prefixes) of any full title in the same work
    // e.g. "Rank Tank Won't Die!" from "The Returned C-Rank Tank Won't Die!", or "chan Wants to be Kept" from "Mii-chan Wants to be Kept"
    const lowerStrings = allRawStrings.map((s) => s.toLocaleLowerCase());
    for (const candidate of allRawStrings) {
        const cLow = candidate.toLocaleLowerCase();
        for (const full of lowerStrings) {
            if (full === cLow)
                continue;
            const suffixIdx = full.indexOf('-' + cLow);
            if (suffixIdx > 0 && suffixIdx + 1 + cLow.length === full.length && !/\s$/.test(full.slice(0, suffixIdx))) {
                const suffixKey = alternativeTitleKey(candidate);
                if (suffixKey)
                    forbiddenSubClauses.add(suffixKey);
                const prefixPart = full.slice(0, suffixIdx);
                // If the unspaced hyphen prefix is also present as a standalone fragment, forbid it too
                if (lowerStrings.includes(prefixPart)) {
                    const prefixKey = alternativeTitleKey(prefixPart);
                    if (prefixKey)
                        forbiddenSubClauses.add(prefixKey);
                }
            }
        }
    }
    for (const raw of expandedInputs) {
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
