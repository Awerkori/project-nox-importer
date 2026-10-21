import { decodeHtmlEntities } from '../sources/common/html-utils.js';

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
export function cleanAlternativeTitle(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const decoded = decodeHtmlEntities(value)
    .normalize('NFKC')
    .replace(/\s+/gu, ' ')
    .replace(/^[,;:\s]+|[,;:\s]+$/gu, '')
    .trim();
  if (decoded.length < 1 || decoded.length > 250) return null;
  // Discard brackets, punctuation-only, or garbage fragments like "[]", "[ ]", "shot)", "-"
  if (/^[\s\[\]\(\)\{\}\-_.,:;!?/'"\\|~+*#@$%^&=>`<´]+$/u.test(decoded)) return null;
  if (/^(shot|one-shot|oneshot|colorido|raw|novel|doujinshi)\)?$/i.test(decoded)) return null;
  // Discard age ratings ("+18", "18+"), zero, or 3+ digit numeric machine IDs ("10803", "873"),
  // while preserving legitimate short numeric titles like "86".
  if (/^(?:\+\d+\+?|\d+\+|\d{3,}|0)$/u.test(decoded)) return null;
  // Discard star rating widgets or vote summaries mistakenly ingested from themes
  if (/\b(?:m[eé]dia\s+\d|\d+(?:[.,]\d+)?\s+do\s+total\s+de\s+\d+\s+votos|\bvotos?\b|\brating\b|\bavalia[çc][aã]o\b)/i.test(decoded)) return null;
  // Discard CSS rules, stylesheet code fragments, and direct URLs
  if (/[{}@]|var\(|calc\(|!important|\b(?:rgb|rgba|border|background|color|padding|margin|font)-/i.test(decoded)) return null;
  if (/^https?:\/\//i.test(decoded)) return null;
  // Discard isolated genres or metadata placeholders mistakenly ingested as aliases
  if (GENRE_OR_TAXONOMY_WORDS.has(decoded.toLocaleLowerCase())) return null;
  return decoded;
}

/**
 * Comparison-only key. It intentionally does not transliterate or translate
 * titles: Japanese, Korean, Chinese and romanized variants remain distinct
 * aliases unless a source supplies the same value again.
 */
export function alternativeTitleKey(value: unknown): string {
  const title = cleanAlternativeTitle(value);
  if (!title) return '';
  return title
    .toLocaleLowerCase()
    .replace(/[‘’`´]/gu, "'")
    .replace(/[\-‐-―_.,;:!?()[\]{}"'・]/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim();
}

const CLAUSE_CONTINUATION_REGEX = /^(?:for|and|or|so|because|but|yet|too|also|with|without|nor|mas|e|ou|pois|porque|com|sem|para|por)\b/i;
const INTERJECTION_PREFIXES = new Set(['hello', 'oh', 'ah', 'hey', 'dear', 'bye', 'ola', 'olá']);

function extractPunctuatedClauses(title: string | null | undefined): Set<string> {
  const clauses = new Set<string>();
  if (!title || typeof title !== 'string' || !title.includes(',')) return clauses;
  // Only forbid comma-split fragments of a single grammatical sentence title
  // (e.g. "Summon The Demon, For I Am The Abyss" or "Hello, Fluffy Griffin!").
  // Never forbid legitimate subtitles or independent titles separated by ':', '-', ';', or '|'.
  const parts = title.split(/,\s*/).map((p) => p.trim()).filter(Boolean);
  if (parts.length > 1) {
    const firstWord = parts[0].toLowerCase().replace(/[^a-z\u00C0-\u017F]/g, '');
    const isGrammaticalTitle = INTERJECTION_PREFIXES.has(firstWord)
      || parts.some((p, i) => i > 0 && (CLAUSE_CONTINUATION_REGEX.test(p) || /^[a-z]/.test(p)));
    if (isGrammaticalTitle) {
      for (const part of parts) {
        const key = alternativeTitleKey(part);
        if (key) clauses.add(key);
      }
    }
  }
  return clauses;
}

/** Merge aliases without changing the first valid display spelling. */
export function mergeAlternativeTitles(
  existing: readonly unknown[] | null | undefined,
  incoming: readonly unknown[] | null | undefined,
  primaryTitle?: unknown,
  max = 50
): string[] {
  const merged: string[] = [];
  const seen = new Set<string>();
  const primaryKey = alternativeTitleKey(primaryTitle);
  const rawPrimaryTitle = typeof primaryTitle === 'string' ? primaryTitle : '';
  const forbiddenSubClauses = extractPunctuatedClauses(rawPrimaryTitle);

  // First pass: identify grammatical comma-sentence titles in existing and incoming to prevent half-sentence fragments
  for (const raw of [...(existing || []), ...(incoming || [])]) {
    if (typeof raw === 'string' && raw.includes(',')) {
      const clauses = extractPunctuatedClauses(raw);
      for (const c of clauses) forbiddenSubClauses.add(c);
    }
  }

  for (const raw of [...(existing || []), ...(incoming || [])]) {
    const title = cleanAlternativeTitle(raw);
    const key = alternativeTitleKey(title);
    if (!title || !key || key === primaryKey || seen.has(key)) continue;
    // Discard fragments of primary title or compound titles (e.g. "Summon The Demon" from "Summon The Demon, For I Am The Abyss")
    if (forbiddenSubClauses.has(key)) continue;
    seen.add(key);
    merged.push(title);
    if (merged.length >= max) break;
  }
  return merged;
}
