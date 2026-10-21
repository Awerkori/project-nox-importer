import { alternativeTitleKey, cleanAlternativeTitle, mergeAlternativeTitles } from '../core/alternative-titles.js';
import { decodeHtmlEntities } from '../sources/common/html-utils.js';
const SOURCE_TITLE_PLACEHOLDERS = new Set(['unknown', 'n/a', 'none', 'null', 'undefined', 'untitled', 'sem titulo', 'sem título']);
/**
 * A mapping title is first-party evidence only after its mapping is attached
 * to the canonical work. Older adapters sometimes copied a machine slug (or
 * API id) into this field; reject just those fallbacks while accepting a
 * reader-facing title even when the provider uses it as the slug as well.
 */
export function isProviderDisplayTitleAlias(sourceTitle, sourceSlug, primaryTitle) {
    const title = cleanAlternativeTitle(sourceTitle);
    if (!title || alternativeTitleKey(title) === alternativeTitleKey(primaryTitle))
        return false;
    const folded = title.toLocaleLowerCase();
    if (SOURCE_TITLE_PLACEHOLDERS.has(folded) || /^(?:\+\d+\+?|\d+\+|\d{3,}|0)$/u.test(title))
        return false;
    const slug = cleanAlternativeTitle(sourceSlug);
    return alternativeTitleKey(title) !== alternativeTitleKey(slug)
        || !/^[a-z0-9]+(?:[-_][a-z0-9]+)+$/u.test(title);
}
export const STORED_MAPPING_ALIAS_FIELDS = [
    'alternativeTitles', 'alternative_titles', 'alternativeTitle', 'alternative_title',
    'aliases', 'alternativeNames', 'alternative_names', 'otherNames', 'other_names', 'alsoKnownAs', 'also_known_as',
    'originalTitle', 'original_title', 'nativeTitle', 'native_title', 'nativeName', 'native_name',
    'englishTitle', 'english_title', 'romanizedTitle', 'romanized_title', 'romajiTitle', 'romaji_title',
    'japaneseTitle', 'japanese_title', 'koreanTitle', 'korean_title', 'chineseTitle', 'chinese_title',
    'altName', 'alt_name', 'altNames', 'alt_names', 'altTitle', 'alt_title', 'altTitles', 'alt_titles', 'alt',
    'synonyms', 'sinonimos', 'sinônimos', 'sinonimo', 'sinônimo',
    // These are provider-declared fields from Portuguese APIs and scans:
    'titulo_alternative', 'titulo_alternativo', 'titulos_alternativos',
    'nome_alternativo', 'nomes_alternativos', 'outros_nomes', 'outro_nome',
    'obr_outros_nomes', 'obr_nome_alternativo', 'obr_titulo_alternativo',
    'titulo_original', 'nome_original', 'titulo_nativo', 'nome_nativo',
    'titulo_ingles', 'titulo_em_ingles', 'titulo_romanizado', 'romaji',
    'titulo_japones', 'titulo_japonês', 'titulo_coreano', 'titulo_chines', 'titulo_chinês',
];
export const STORED_MAPPING_TAG_FIELDS = [
    'genres', 'genre', 'tags', 'tag', 'categories',
    // The same APIs expose their source taxonomy under Portuguese names.  A
    // category such as Manhua is explicit provider metadata and may be merged
    // just like an English `category`; no genre is guessed from prose.
    'generos', 'genero', 'categorias', 'categoria',
];
function explicitJsonArray(value) {
    const text = value.trim();
    // Some legacy mappings retained an adapter's explicit JSON array as a JSON
    // string. Decode only a bounded, syntactically valid array; prose and JSON
    // objects remain ineligible for historical enrichment.
    if (text.length < 2 || text.length > 32_000 || !text.startsWith('[') || !text.endsWith(']'))
        return null;
    try {
        const parsed = JSON.parse(text);
        return Array.isArray(parsed) ? parsed : null;
    }
    catch {
        return null;
    }
}
const CLAUSE_CONTINUATION_REGEX = /^(?:for|and|or|so|because|but|yet|too|also|with|without|nor|mas|e|ou|pois|porque|com|sem|para|por)\b/i;
const INTERJECTION_PREFIXES = new Set(['hello', 'oh', 'ah', 'hey', 'dear', 'bye', 'ola', 'olá']);
function splitDelimitedTitles(text) {
    const trimmed = text.trim();
    if (trimmed.length < 1)
        return [];
    if (/[\r\n]/.test(trimmed)) {
        return trimmed.split(/[\r\n]+/).map((s) => s.trim()).filter(Boolean);
    }
    if (/;/.test(trimmed)) {
        return trimmed.split(/;+/).map((s) => s.trim()).filter(Boolean);
    }
    if (/\s+\/\s+/.test(trimmed)) {
        return trimmed.split(/\s+\/\s+/).map((s) => s.trim()).filter(Boolean);
    }
    if (/, /.test(trimmed)) {
        const parts = trimmed.split(/,\s+/).map((s) => s.trim()).filter(Boolean);
        if (parts.length > 1) {
            // If the first part is an interjection (e.g. "Hello, Fluffy Griffin!"), or any subsequent part
            // is a clause continuation (e.g. "For I Am The Abyss", "Yet Pure White", "so I ..."),
            // or starts with a lowercase letter, it is a single grammatical title.
            const firstWord = parts[0].toLowerCase().replace(/[^a-z\u00C0-\u017F]/g, '');
            const isGrammaticalTitle = INTERJECTION_PREFIXES.has(firstWord)
                || parts.some((p, i) => i > 0 && (CLAUSE_CONTINUATION_REGEX.test(p) || /^[a-z]/.test(p)));
            if (!isGrammaticalTitle) {
                return parts;
            }
        }
    }
    return [trimmed];
}
function asTextList(value, objectKeys = [], depth = 0) {
    const encodedArray = typeof value === 'string' && depth < 2 ? explicitJsonArray(value) : null;
    const values = Array.isArray(value) ? value : (encodedArray ?? (value == null ? [] : [value]));
    return values.flatMap((entry) => {
        if (typeof entry === 'string') {
            const nested = depth < 2 ? explicitJsonArray(entry) : null;
            if (nested)
                return asTextList(nested, objectKeys, depth + 1);
            return splitDelimitedTitles(entry);
        }
        if (!entry || typeof entry !== 'object')
            return [];
        const record = entry;
        for (const key of objectKeys) {
            const nested = record[key];
            if (nested !== undefined)
                return asTextList(nested, objectKeys, depth + 1);
        }
        return [];
    });
}
function hasExplicitValue(metadata, fieldPattern) {
    return Object.entries(metadata).some(([key, value]) => {
        if (!fieldPattern.test(key))
            return false;
        if (Array.isArray(value))
            return value.length > 0;
        if (typeof value === 'string')
            return value.trim().length > 0 && !['[]', '{}', 'null'].includes(value.trim().toLowerCase());
        if (value && typeof value === 'object')
            return Object.keys(value).length > 0;
        return value !== null && value !== undefined;
    });
}
/**
 * Only explicitly named upstream metadata fields are eligible for the
 * no-network historical enrichment pass. It must never mine prose, URLs, or
 * a page title into a fabricated alias or tag.
 */
export function extractStoredMappingMetadata(raw, primaryTitle) {
    const metadata = raw && typeof raw === 'object' ? raw : {};
    const aliases = STORED_MAPPING_ALIAS_FIELDS.flatMap((field) => asTextList(metadata[field], [
        'title', 'name', 'value', 'nome', 'titulo', 'valor',
        'alternativeTitle', 'alternative_title', 'nativeTitle', 'native_title',
        'englishTitle', 'english_title', 'romanizedTitle', 'romanized_title',
        'titulo_alternative', 'titulo_alternativo', 'titulo_original',
        'nome_original', 'titulo_nativo', 'nome_nativo', 'titulo_ingles',
        'titulo_em_ingles', 'titulo_romanizado', 'romaji',
        'nome_alternativo', 'sinonimo', 'sinonimos',
    ]));
    // Legacy Portuguese adapters persisted tag records with `nome`; accept it
    // alongside the common English keys so the no-network replay can recover
    // metadata already stored in YSQL.
    const genres = STORED_MAPPING_TAG_FIELDS.flatMap((field) => asTextList(metadata[field], ['name', 'nome', 'title', 'label']));
    return {
        aliases: mergeAlternativeTitles([], aliases, primaryTitle),
        genres: [...new Set(genres.map((value) => value.normalize('NFKC').replace(/\s+/g, ' ').trim()).filter(Boolean))],
    };
}
/**
 * Classify metadata that was explicitly returned by a provider.  This is
 * shared by a normal catalog pass and a retroactive source refetch so a new
 * mapping cannot be silently left PENDING after its first successful fetch.
 */
export function classifyExplicitSourceMetadata(raw, primaryTitle) {
    const metadata = raw && typeof raw === 'object' ? raw : {};
    const extracted = extractStoredMappingMetadata(metadata, primaryTitle);
    // An upstream alias field containing only the primary title is an explicit
    // and valid NO_ALIASES result, not evidence of a parser defect.
    const parsedAliasEvidence = extracted.aliases.length > 0
        ? extracted.aliases
        : extractStoredMappingMetadata(metadata, '').aliases;
    const rawAliasEvidence = hasExplicitValue(metadata, /^(alternative.?titles?|aliases|alternative.?names|other.?names|also.?known.?as|original.?title|native.?(title|name)|english.?title|romanized.?title|romaji.?title|titulo.?alternative|titulos?.?alternativos?|titulo.?original|nome.?original|titulo.?nativo|nome.?nativo|titulo.?(em.)?ingles|titulo.?romanizado|romaji|nome.?alternativo|nomes?.?alternativos?|outros?.?nomes?|outro.?nome|obr.?outros?.?nomes?|obr.?nome.?alternativo|obr.?titulo.?alternativo|alt|alt.?(names?|titles?)|synonyms?|sin[oô]nimos?|japanese.?title|korean.?title|chinese.?title|titulo.?(japones|japonês|coreano|chines|chinês))$/i);
    const rawTagEvidence = hasExplicitValue(metadata, /^(genres?|tags?|categories|generos?|categorias?)$/i);
    const aliasesParserError = rawAliasEvidence && parsedAliasEvidence.length === 0;
    const tagsParserError = rawTagEvidence && extracted.genres.length === 0;
    return {
        ...extracted,
        aliasesState: aliasesParserError ? 'PARSER_ERROR' : (extracted.aliases.length ? 'ENRICHED' : 'NO_ALIASES'),
        tagsState: tagsParserError ? 'PARSER_ERROR' : (extracted.genres.length ? 'ENRICHED' : 'NO_TAGS'),
        parserError: aliasesParserError || tagsParserError,
    };
}
/**
 * Generate a bounded list of search query candidates for cross-source discovery.
 * Includes the primary title, the main title clause if punctuated (e.g. "Title, Subtitle" -> "Title"),
 * and up to 4 distinct aliases, deduplicated and capped at 5 total queries.
 */
export function buildDiscoveryQueryCandidates(title, aliases = []) {
    const candidateList = [];
    const cleanTitle = decodeHtmlEntities(title || '').trim();
    if (cleanTitle.length >= 1)
        candidateList.push(cleanTitle);
    // Extract primary title clause if punctuation indicates a subtitle
    const titleParts = cleanTitle.split(/(?:,\s*|[:;|/()]|\s+[-–—]\s+)/).map((p) => p.trim()).filter(Boolean);
    if (titleParts.length > 1 && titleParts[0].length >= 2) {
        candidateList.push(titleParts[0]);
    }
    for (const alias of aliases || []) {
        if (alias && typeof alias === 'string') {
            const decoded = decodeHtmlEntities(alias).trim();
            if (decoded.length >= 1) {
                candidateList.push(decoded);
            }
        }
    }
    const seen = new Set();
    const candidates = [];
    for (const cand of candidateList) {
        const norm = cand.toLowerCase();
        if (!seen.has(norm) && cand.length >= 1) {
            seen.add(norm);
            candidates.push(cand);
            if (candidates.length >= 5)
                break;
        }
    }
    return candidates;
}
