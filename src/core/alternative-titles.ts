/**
 * Display values are retained exactly enough for readers; comparison keys are
 * deliberately separate.  This avoids losing native scripts while still
 * coalescing cosmetic source variants such as whitespace and curly quotes.
 */
export function cleanAlternativeTitle(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const title = value.normalize('NFKC').replace(/\s+/gu, ' ').trim();
  if (title.length < 2 || title.length > 250) return null;
  return title;
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

  for (const raw of [...(existing || []), ...(incoming || [])]) {
    const title = cleanAlternativeTitle(raw);
    const key = alternativeTitleKey(title);
    if (!title || !key || key === primaryKey || seen.has(key)) continue;
    seen.add(key);
    merged.push(title);
    if (merged.length >= max) break;
  }
  return merged;
}
