import { mergeAlternativeTitles } from '../core/alternative-titles.js';

type RawRecord = Record<string, unknown>;

const aliasesFields = [
  'alternativeTitles', 'alternative_titles', 'aliases', 'alternativeNames', 'alternative_names',
  'originalTitle', 'original_title', 'nativeTitle', 'native_title', 'englishTitle', 'english_title',
  'romanizedTitle', 'romanized_title',
];
const tagFields = ['genres', 'tags', 'categories'];

function asTextList(value: unknown, objectKeys: string[] = []): string[] {
  const values = Array.isArray(value) ? value : value == null ? [] : [value];
  return values.flatMap((entry) => {
    if (typeof entry === 'string') return [entry];
    if (!entry || typeof entry !== 'object') return [];
    const record = entry as RawRecord;
    for (const key of objectKeys) {
      const nested = record[key];
      if (typeof nested === 'string') return [nested];
    }
    return [];
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
  const aliases = aliasesFields.flatMap((field) => asTextList(metadata[field], ['title', 'name', 'value']));
  const genres = tagFields.flatMap((field) => asTextList(metadata[field], ['name', 'title', 'label']));
  return {
    aliases: mergeAlternativeTitles([], aliases, primaryTitle),
    genres: [...new Set(genres.map((value) => value.normalize('NFKC').replace(/\s+/g, ' ').trim()).filter(Boolean))],
  };
}
