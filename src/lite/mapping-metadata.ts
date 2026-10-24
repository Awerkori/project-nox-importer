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

const SINGLE_VALUE_PROVIDER_TITLE_FIELDS = ['titulo', 'titulo_brasil', 't', 'tb'] as const;
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

const CLAUSE_CONTINUATION_REGEX = /^(?:for|and|or|so|because|but|yet|too|also|with|without|nor|mas|e|ou|pois|porque|com|sem|para|por|why|how|when|where|who|what|don't|can't|won't|isn't|aren't|right|let's|i|i'm|i'll|i've|i'd|you|you're|you'll|we|we're|he|she|it|it's|my|his|her|their|eu|você|voce|nós|nos|ele|ela|meu|minha|seu|sua|então|entao|agora|only|even|still|already|never|always|once|again|novamente|alone|não\s+[eé]|nao\s+[eé])\b/i;
const SUBORDINATE_START_REGEX = /^(?:when|while|after|before|since|although|though|even\s+though|if|unless|until|despite|due\s+to|without|with|in|on|at|for|by|from|to|quando|enquanto|depois|ap[oó]s|antes|desde|embora|mesmo|se|sem|com|em|no|na|nos|nas|para|por|contanto)\b/i;
const VOCATIVE_NAME_REGEX = /(?:^[a-zA-Z\u00C0-\u017F\s]{1,25}-(?:san|kun|chan|sama|sensei|senpai|dono)[!?.]*$|\b(?:your\s+(?:grace|highness|majesty)|vossa\s+(?:gra[çc]a|alteza|majestade)|duque|duke|duquesa|duchess|margrave|milady|my\s+lord|meu\s+senhor|husband|marido|sweetheart|querid[ao]|miss\s+\w+|mr\.?\s+\w+|senhor(?:a)?\s+\w+|irm[aã]o?|senior\s+sister|immortal\s+fairy|fada)[!?.]*$)/i;
const INTERJECTION_PREFIXES = new Set([
  'hello', 'oh', 'ah', 'hey', 'dear', 'darling', 'honey', 'baby', 'babe', 'bye', 'goodbye', 'sayonara', 'adeus', 'ola', 'olá',
  'mom', 'mae', 'mãe', 'dad', 'pai', 'wait', 'espere', 'sorry', 'desculpa', 'perdao', 'perdão',
  'please', 'onegai', 'ganbare', 'ijiranaide', 'ijirimakutte', 'uzaki', 'komi', 'hyung', 'oppa', 'noona', 'unnie',
  'querido', 'querida', 'amor', 'mestre', 'master', 'like',
]);

function hasPairParallelTitleWordOverlap(a: string, b: string): boolean {
  const stopWords = new Set(['para', 'como', 'mais', 'pela', 'pelo', 'uma', 'with', 'from', 'that', 'this', 'into', 'over', 'your', 'mine']);
  const wordsA = a.toLowerCase().replace(/[^a-z\u00C0-\u017F0-9\s]/g, ' ').split(/\s+/).filter(Boolean);
  const wordsB = b.toLowerCase().replace(/[^a-z\u00C0-\u017F0-9\s]/g, ' ').split(/\s+/).filter(Boolean);
  if (wordsA.length < 2 || wordsB.length < 2) return false;
  const sigB = new Set(wordsB.filter((w) => w.length >= 4 && !stopWords.has(w)));
  return sigB.size > 0 && wordsA.some((w) => w.length >= 4 && !stopWords.has(w) && sigB.has(w));
}

function scriptSignature(s: string): string {
  const sig = [];
  if (/[a-zA-Z\u00C0-\u017F]/.test(s)) sig.push('L');
  if (/[\u3040-\u30ff]/.test(s)) sig.push('J');
  if (/[\u3400-\u4dbf\u4e00-\u9fff]/.test(s)) sig.push('C');
  if (/[\uac00-\ud7af]/.test(s)) sig.push('K');
  if (/[\u0400-\u04ff]/.test(s)) sig.push('R');
  if (/[\u0e00-\u0e7f]/.test(s)) sig.push('T');
  return sig.join('') || 'O';
}

function splitDelimitedTitles(text: string): string[] {
  const trimmed = text.trim();
  if (trimmed.length < 1) return [];
  // Reject CSS/JS/HTML stylesheet dumps whole before splitting on `;` or `,`
  if (
    /[{}@]|cubic-bezier|border-box|linear-gradient|Array\.prototype|this\.getFeed|max-results|disqusShortname|filterStatus|font-family|font-weight|background-repeat|counter\(section\)|\[x-cloak\]|\b(?:overflow-[xy]|margin-(?:left|right|top|bottom)|padding-(?:left|right|top|bottom)|font-size|line-height|background-size|border-radius|z-index|transition|transform)\s*:/i.test(trimmed)
  ) {
    return [];
  }
  if (/[\r\n]/.test(trimmed)) {
    return trimmed.split(/[\r\n]+/).map((s) => s.trim()).filter(Boolean);
  }
  if (/;/.test(trimmed)) {
    return trimmed.split(/;+/).map((s) => s.trim()).filter(Boolean);
  }
  if (/\s+\/\s+|\s+\/\/\s+|\|/.test(trimmed)) {
    return trimmed.split(/\s+(?:\/|\/\/)\s+|\s*\|\s*/).map((s) => s.trim()).filter(Boolean);
  }
  // Step 1: Always split on unspaced commas (not inside numbers like 10,000) or malformed quote-comma-quote `","`
  const unspacedSplit = trimmed
    .split(/(?:"\s*,\s*"|(?<!\d),(?![\s\d]))/)
    .map((s) => s.trim())
    .filter(Boolean);

  // Step 2: For each segment, split on spaced commas `, ` unless the adjacent parts form a single grammatical sentence.
  return unspacedSplit.flatMap((segment) => {
    if (!/,(?!\d{3}\b)\s+/.test(segment)) return [segment];
    const parts = segment.split(/,(?!\d{3}\b)\s+/).map((s) => s.trim()).filter(Boolean);
    if (parts.length <= 1) return [segment];

    const mergedParts: string[] = [];
    for (let i = 0; i < parts.length; i++) {
      let current = parts[i];
      while (i + 1 < parts.length) {
        const next = parts[i + 1];
        const sigCurr = scriptSignature(current);
        const sigNext = scriptSignature(next);
        const bothSameScript = sigCurr === sigNext && sigCurr.includes('L');
        const firstWord = current.toLowerCase().replace(/[^a-z\u00C0-\u017F]/g, '');
        const isSameStemSpelling =
          alternativeTitleKey(current) === alternativeTitleKey(next) ||
          current.toLowerCase().replace(/-(?:san|kun|chan|sama|sensei|senpai|dono)\b/i, '').trim() ===
            next.toLowerCase().replace(/\s+(?:san|kun|chan|sama|sensei|senpai|dono)\b/i, '').trim();

        const isPairGrammatical =
          bothSameScript &&
          !isSameStemSpelling &&
          !hasPairParallelTitleWordOverlap(current, next) &&
          Boolean(cleanAlternativeTitle(current)) &&
          Boolean(cleanAlternativeTitle(next)) &&
          (
            INTERJECTION_PREFIXES.has(firstWord) ||
            /^por\s+favor\b/i.test(current) ||
            SUBORDINATE_START_REGEX.test(current) ||
            VOCATIVE_NAME_REGEX.test(next) ||
            (VOCATIVE_NAME_REGEX.test(current) && (CLAUSE_CONTINUATION_REGEX.test(next) || /[!?]$/.test(next))) ||
            CLAUSE_CONTINUATION_REGEX.test(next) ||
            /^[a-z\u00E0-\u00FC]/.test(next) ||
            (i + 2 < parts.length && CLAUSE_CONTINUATION_REGEX.test(parts[i + 2]))
          );

        if (!isPairGrammatical) {
          break;
        }
        current = `${current}, ${next}`;
        i++;
      }
      mergedParts.push(current);
    }
    return mergedParts;
  });
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
  const aliases = [
    ...STORED_MAPPING_ALIAS_FIELDS.flatMap((field) => asTextList(metadata[field], [
      'title', 'name', 'value', 'nome', 'titulo', 'valor',
      'alternativeTitle', 'alternative_title', 'nativeTitle', 'native_title',
      'englishTitle', 'english_title', 'romanizedTitle', 'romanized_title',
      'titulo_alternative', 'titulo_alternativo', 'titulo_original',
      'nome_original', 'titulo_nativo', 'nome_nativo', 'titulo_ingles',
      'titulo_em_ingles', 'titulo_romanizado', 'romaji',
      'nome_alternativo', 'sinonimo', 'sinonimos',
    ])),
    ...SINGLE_VALUE_PROVIDER_TITLE_FIELDS.flatMap((field) => {
      const val = metadata[field];
      return typeof val === 'string' && val.trim() ? [val.trim()] : [];
    }),
  ];
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
