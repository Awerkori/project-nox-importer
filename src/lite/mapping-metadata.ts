import { mergeAlternativeTitles } from '../core/alternative-titles.js';

type RawRecord = Record<string, unknown>;

const aliasesFields = [
  'alternativeTitles', 'alternative_titles', 'alternativeTitle', 'alternative_title',
  'aliases', 'alternativeNames', 'alternative_names', 'otherNames', 'other_names', 'alsoKnownAs', 'also_known_as',
  'originalTitle', 'original_title', 'nativeTitle', 'native_title', 'nativeName', 'native_name',
  'englishTitle', 'english_title', 'romanizedTitle', 'romanized_title', 'romajiTitle', 'romaji_title',
];
const tagFields = ['genres', 'genre', 'tags', 'tag', 'categories'];

function explicitJsonArray(value: string): unknown[] | null {
  const text = value.trim();
  // Some legacy mappings retained an adapter's explicit JSON array as a JSON
  // string. Decode only a bounded, syntactically valid array; prose and JSON
  // objects remain ineligible for historical enrichment.
  if (text.length < 2 || text.length > 32_000 || !text.startsWith('[') || !text.endsWith(']')) return null;
  try {
    const parsed = JSON.parse(text);
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function asTextList(value: unknown, objectKeys: string[] = [], depth = 0): string[] {
  const encodedArray = typeof value === 'string' && depth < 2 ? explicitJsonArray(value) : null;
  const values = Array.isArray(value) ? value : (encodedArray ?? (value == null ? [] : [value]));
  return values.flatMap((entry) => {
    if (typeof entry === 'string') {
      const nested = depth < 2 ? explicitJsonArray(entry) : null;
      return nested ? asTextList(nested, objectKeys, depth + 1) : [entry];
    }
    if (!entry || typeof entry !== 'object') return [];
    const record = entry as RawRecord;
    for (const key of objectKeys) {
      const nested = record[key];
      if (nested !== undefined) return asTextList(nested, objectKeys, depth + 1);
    }
    return [];
  });
}

function hasExplicitValue(metadata: RawRecord, fieldPattern: RegExp): boolean {
  return Object.entries(metadata).some(([key, value]) => {
    if (!fieldPattern.test(key)) return false;
    if (Array.isArray(value)) return value.length > 0;
    if (typeof value === 'string') return value.trim().length > 0 && !['[]', '{}', 'null'].includes(value.trim().toLowerCase());
    if (value && typeof value === 'object') return Object.keys(value as object).length > 0;
    return value !== null && value !== undefined;
  });
}

/**
 * Only explicitly named upstream metadata fields are eligible for the
 * no-network historical enrichment pass. It must never mine prose, URLs, or
 * a page title into a fabricated alias or tag.
 */
export function extractStoredMappingMetadata(raw: unknown, primaryTitle: string): {
  aliases: string[];
  genres: string[];
} {
  const metadata = raw && typeof raw === 'object' ? raw as RawRecord : {};
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

/**
 * Classify metadata that was explicitly returned by a provider.  This is
 * shared by a normal catalog pass and a retroactive source refetch so a new
 * mapping cannot be silently left PENDING after its first successful fetch.
 */
export function classifyExplicitSourceMetadata(raw: unknown, primaryTitle: string): {
  aliases: string[];
  genres: string[];
  aliasesState: 'ENRICHED' | 'NO_ALIASES' | 'PARSER_ERROR';
  tagsState: 'ENRICHED' | 'NO_TAGS' | 'PARSER_ERROR';
  parserError: boolean;
} {
  const metadata = raw && typeof raw === 'object' ? raw as RawRecord : {};
  const extracted = extractStoredMappingMetadata(metadata, primaryTitle);
  // An upstream alias field containing only the primary title is an explicit
  // and valid NO_ALIASES result, not evidence of a parser defect.
  const parsedAliasEvidence = extracted.aliases.length > 0
    ? extracted.aliases
    : extractStoredMappingMetadata(metadata, '').aliases;
  const rawAliasEvidence = hasExplicitValue(metadata,
    /^(alternative.?titles?|aliases|alternative.?names|other.?names|also.?known.?as|original.?title|native.?(title|name)|english.?title|romanized.?title|romaji.?title)$/i);
  const rawTagEvidence = hasExplicitValue(metadata, /^(genres?|tags?|categories)$/i);
  const aliasesParserError = rawAliasEvidence && parsedAliasEvidence.length === 0;
  const tagsParserError = rawTagEvidence && extracted.genres.length === 0;
  return {
    ...extracted,
    aliasesState: aliasesParserError ? 'PARSER_ERROR' : (extracted.aliases.length ? 'ENRICHED' : 'NO_ALIASES'),
    tagsState: tagsParserError ? 'PARSER_ERROR' : (extracted.genres.length ? 'ENRICHED' : 'NO_TAGS'),
    parserError: aliasesParserError || tagsParserError,
  };
}
