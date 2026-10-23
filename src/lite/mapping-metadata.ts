import { alternativeTitleKey, cleanAlternativeTitle, mergeAlternativeTitles } from '../core/alternative-titles.js';
import { getCanonicalSynonyms } from '../core/matching.js';
import { decodeHtmlEntities } from '../sources/common/html-utils.js';

type RawRecord = Record<string, unknown>;

const SOURCE_TITLE_PLACEHOLDERS = new Set(['unknown', 'n/a', 'none', 'null', 'undefined', 'untitled', 'sem titulo', 'sem título']);

/**
 * A mapping title is first-party evidence only after its mapping is attached
 * to the canonical work. Older adapters sometimes copied a machine slug (or
 * API id) into this field; reject just those fallbacks while accepting a
 * reader-facing title even when the provider uses it as the slug as well.
 */
export function isProviderDisplayTitleAlias(
  sourceTitle: unknown,
  sourceSlug: unknown,
  primaryTitle: unknown,
): boolean {
  const title = cleanAlternativeTitle(sourceTitle);
  if (!title || alternativeTitleKey(title) === alternativeTitleKey(primaryTitle)) return false;
  const folded = title.toLocaleLowerCase();
  if (SOURCE_TITLE_PLACEHOLDERS.has(folded) || /^(?:\+\d+\+?|\d+\+|\d{3,}|0)$/u.test(title)) return false;

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

function explicitJsonArray(value: string): unknown[] | null {
  const text = value.trim();
  // Some legacy mappings retained an adapter's explicit JSON array as a JSON
  // string or Python single-quoted list literal. Decode only a bounded,
  // syntactically valid array; prose and JSON objects remain ineligible.
  if (text.length < 2 || text.length > 32_000 || !text.startsWith('[') || !text.endsWith(']')) return null;
  try {
    const parsed = JSON.parse(text);
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    // Handle Python-style list literals such as "['Title A', \"Title B's\"]"
    if (text.startsWith("['") || text.startsWith('["')) {
      const items: string[] = [];
      const itemRegex = /'((?:\\.|[^'\\])*)'|"((?:\\.|[^"\\])*)"/g;
      let match: RegExpExecArray | null;
      while ((match = itemRegex.exec(text)) !== null) {
        const val = (match[1] ?? match[2] ?? '').replace(/\\(['"\\])/g, '$1').trim();
        if (val) items.push(val);
      }
      if (items.length > 0) return items;
    }
    return null;
  }
}

const CLAUSE_CONTINUATION_REGEX = /^(?:for|and|or|so|because|but|yet|too|also|with|without|nor|mas|e|ou|pois|porque|com|sem|para|por)\b/i;
const VOCATIVE_NAME_REGEX = /(?:^[a-zA-Z\u00C0-\u017F\s]{1,25}-(?:san|kun|chan|sama|sensei|senpai|dono)[!?.]*$|\b(?:your\s+(?:grace|highness|majesty)|vossa\s+(?:gra[çc]a|alteza|majestade)|duque|duke|milady|my\s+lord|meu\s+senhor)[!?.]*$)/i;
const INTERJECTION_PREFIXES = new Set([
  'hello', 'oh', 'ah', 'hey', 'dear', 'bye', 'goodbye', 'sayonara', 'adeus', 'ola', 'olá',
  'mom', 'mae', 'mãe', 'dad', 'pai', 'wait', 'espere', 'sorry', 'desculpa', 'perdao', 'perdão',
  'please', 'onegai', 'ijiranaide', 'ijirimakutte', 'uzaki', 'komi',
]);

function splitDelimitedTitles(text: string): string[] {
  const trimmed = text.trim();
  if (trimmed.length < 1) return [];
  if (/[\r\n]/.test(trimmed)) {
    return trimmed.split(/[\r\n]+/).map((s) => s.trim()).filter(Boolean);
  }
  if (/;/.test(trimmed)) {
    return trimmed.split(/;+/).map((s) => s.trim()).filter(Boolean);
  }
  if (/\s+\/\s+|\s*\/\/\s*|\|/.test(trimmed)) {
    return trimmed.split(/\s*(?:\/|\/\/|\|)\s*/).map((s) => s.trim()).filter(Boolean);
  }
  if (/(?<!\d),(?!\d{3})/.test(trimmed)) {
    const parts = trimmed.split(/(?<!\d),(?!\d{3})\s*/).map((s) => s.trim()).filter(Boolean);
    if (parts.length > 1) {
      const mergedParts: string[] = [];
      for (let i = 0; i < parts.length; i++) {
        const current = parts[i];
        const next = parts[i + 1];
        if (next !== undefined) {
          const firstWord = current.toLowerCase().replace(/[^a-z\u00C0-\u017F]/g, '');
          const isPairGrammatical =
            INTERJECTION_PREFIXES.has(firstWord) ||
            VOCATIVE_NAME_REGEX.test(current) ||
            VOCATIVE_NAME_REGEX.test(next) ||
            CLAUSE_CONTINUATION_REGEX.test(next) ||
            /^[a-z]/.test(next);
          if (isPairGrammatical) {
            mergedParts.push(`${current}, ${next}`);
            i++;
            continue;
          }
        }
        mergedParts.push(current);
      }
      return mergedParts;
    }
  }
  return [trimmed];
}

function asTextList(value: unknown, objectKeys: string[] = [], depth = 0): string[] {
  const encodedArray = typeof value === 'string' && depth < 2 ? explicitJsonArray(value) : null;
  const values = Array.isArray(value) ? value : (encodedArray ?? (value == null ? [] : [value]));
  return values.flatMap((entry) => {
    if (typeof entry === 'string') {
      const nested = depth < 2 ? explicitJsonArray(entry) : null;
      if (nested) return asTextList(nested, objectKeys, depth + 1);
      return splitDelimitedTitles(entry);
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
    /^(alternative.?titles?|aliases|alternative.?names|other.?names|also.?known.?as|original.?title|native.?(title|name)|english.?title|romanized.?title|romaji.?title|titulo.?alternative|titulos?.?alternativos?|titulo.?original|nome.?original|titulo.?nativo|nome.?nativo|titulo.?(em.)?ingles|titulo.?romanizado|romaji|nome.?alternativo|nomes?.?alternativos?|outros?.?nomes?|outro.?nome|obr.?outros?.?nomes?|obr.?nome.?alternativo|obr.?titulo.?alternativo|alt|alt.?(names?|titles?)|synonyms?|sin[oô]nimos?|japanese.?title|korean.?title|chinese.?title|titulo.?(japones|japonês|coreano|chines|chinês))$/i);
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
 * canonical synonym variations, and up to 4 distinct aliases, deduplicated and capped at 5 total queries.
 */
export function buildDiscoveryQueryCandidates(title: string, aliases: string[] = []): string[] {
  const candidateList: string[] = [];
  const cleanTitle = decodeHtmlEntities(title || '').trim();
  if (cleanTitle.length >= 1) candidateList.push(cleanTitle);

  // Extract primary title clause if punctuation indicates a subtitle (excluding interjection prefixes)
  const titleParts = cleanTitle.split(/(?:,\s*|[:;|/()]|\s+[-–—]\s+)/).map((p) => p.trim()).filter(Boolean);
  if (titleParts.length > 1 && titleParts[0].length >= 2) {
    const firstWord = titleParts[0].toLowerCase().replace(/[^a-z\u00C0-\u017F]/g, '');
    if (!INTERJECTION_PREFIXES.has(firstWord)) {
      candidateList.push(titleParts[0]);
    }
  }

  // Include canonical synonym variations (e.g. "Step-mother" <-> "Madrasta", Roman numerals)
  for (const syn of getCanonicalSynonyms(cleanTitle)) {
    if (syn && syn.trim().length >= 2) {
      candidateList.push(syn.trim());
    }
  }

  for (const alias of aliases || []) {
    if (alias && typeof alias === 'string') {
      const decoded = decodeHtmlEntities(alias).trim();
      const isGeneric = /^(?:sorry|desculpa|perdao|por\s*favor|please|onegai|obrigad[ao]|thanks?|thank\s*you|na\s*verdade|actually|in\s*fact|turns\s*out|love|hello|alone|baby|oneshot|manga|comic|primal|scan|raw)$/i.test(decoded);
      if (decoded.length >= 1 && (!isGeneric || cleanTitle.toLowerCase().includes(decoded.toLowerCase()))) {
        candidateList.push(decoded);
      }
    }
  }

  const seen = new Set<string>();
  const candidates: string[] = [];
  for (const cand of candidateList) {
    const norm = cand.toLowerCase();
    if (!seen.has(norm) && cand.length >= 1) {
      seen.add(norm);
      candidates.push(cand);
      if (candidates.length >= 5) break;
    }
  }
  return candidates;
}
