import { mergeAlternativeTitles } from '../core/alternative-titles.js';
const aliasesFields = [
    'alternativeTitles', 'alternative_titles', 'alternativeTitle', 'alternative_title',
    'aliases', 'alternativeNames', 'alternative_names', 'otherNames', 'other_names', 'alsoKnownAs', 'also_known_as',
    'originalTitle', 'original_title', 'nativeTitle', 'native_title', 'nativeName', 'native_name',
    'englishTitle', 'english_title', 'romanizedTitle', 'romanized_title', 'romajiTitle', 'romaji_title',
];
const tagFields = ['genres', 'genre', 'tags', 'tag', 'categories'];
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
function asTextList(value, objectKeys = [], depth = 0) {
    const encodedArray = typeof value === 'string' && depth < 2 ? explicitJsonArray(value) : null;
    const values = Array.isArray(value) ? value : (encodedArray ?? (value == null ? [] : [value]));
    return values.flatMap((entry) => {
        if (typeof entry === 'string') {
            const nested = depth < 2 ? explicitJsonArray(entry) : null;
            return nested ? asTextList(nested, objectKeys, depth + 1) : [entry];
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
/**
 * Only explicitly named upstream metadata fields are eligible for the
 * no-network historical enrichment pass. It must never mine prose, URLs, or
 * a page title into a fabricated alias or tag.
 */
export function extractStoredMappingMetadata(raw, primaryTitle) {
    const metadata = raw && typeof raw === 'object' ? raw : {};
    const aliases = aliasesFields.flatMap((field) => asTextList(metadata[field], [
        'title', 'name', 'value', 'alternativeTitle', 'alternative_title', 'nativeTitle', 'native_title',
        'englishTitle', 'english_title', 'romanizedTitle', 'romanized_title',
    ]));
    // Legacy Portuguese adapters persisted tag records with `nome`; accept it
    // alongside the common English keys so the no-network replay can recover
    // metadata already stored in YSQL.
    const genres = tagFields.flatMap((field) => asTextList(metadata[field], ['name', 'nome', 'title', 'label']));
    return {
        aliases: mergeAlternativeTitles([], aliases, primaryTitle),
        genres: [...new Set(genres.map((value) => value.normalize('NFKC').replace(/\s+/g, ' ').trim()).filter(Boolean))],
    };
}
