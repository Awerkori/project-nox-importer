const clean = (value) => String(value ?? '').normalize('NFKC').replace(/\s+/g, ' ').trim();
const folded = (value) => clean(value).toLocaleLowerCase('und');
/**
 * Only a decimal suffix made entirely of zeroes is a cosmetic rendering of a
 * whole numbered chapter.  Do not parse this as a number: `12.10` can be a
 * distinct provider identifier from `12.1`, whereas `12.00` is unambiguously
 * the same numeric chapter as `12`.
 */
function canonicalRegularNumber(value) {
    return value.replace(/^(\d+)\.0+$/, '$1');
}
export function chapterKind(input) {
    const explicit = folded(input.kind);
    if (explicit)
        return explicit;
    const title = folded(input.title);
    return /(?:extra|especial|special|omake|prologue|pr[oó]logo|side)/i.test(title) ? 'special' : 'chapter';
}
/** A stable opaque key; callers must never parse it back into a float. */
export function canonicalChapterKey(input) {
    const title = folded(input.title);
    const kind = chapterKind(input);
    const rawNumber = folded(input.displayNumber);
    // A special/extra keeps its exact provider identifier.  The equivalence is
    // deliberately limited to regular numeric chapters, where `28`, `28.0`
    // and `28.00` are the same chapter rather than separate source variants.
    const number = kind === 'chapter' ? canonicalRegularNumber(rawNumber) : rawNumber;
    const language = folded(input.language || 'und');
    const edition = folded(input.edition || 'default');
    if (!number && !title)
        throw new Error('A chapter needs a source number or title');
    // Regular numbered chapters match across providers by number/edition/language
    // even when providers use different display titles. Title distinguishes only
    // non-numbered and special identities where it is part of the identity.
    const identityTitle = !number || kind !== 'chapter' ? (title || '-') : '-';
    return [language, edition, kind, number || 'unnumbered', identityTitle].map(encodeURIComponent).join('|');
}
export function parseCanonicalChapterKey(value) {
    const parts = String(value || '').split('|');
    if (parts.length !== 5)
        return null;
    try {
        const [language, edition, kind, encodedNumber, encodedTitle] = parts.map((part) => decodeURIComponent(part));
        if (!language || !edition || !kind || !encodedNumber)
            return null;
        return {
            language,
            edition,
            kind,
            displayNumber: encodedNumber === 'unnumbered' ? '' : encodedNumber,
            title: encodedTitle === '-' ? '' : encodedTitle,
        };
    }
    catch {
        return null;
    }
}
/**
 * A display-only sortable tuple. Decimal text is intentionally retained as a
 * tie breaker: 12.1 and 12.10 may sort together but can never become equal.
 */
export function chapterOrderKey(input) {
    const raw = folded(input.displayNumber);
    const match = raw.match(/^(\d+)(?:\.(\d+))?(.*)$/);
    if (!match)
        return `z|${encodeURIComponent(raw)}|${encodeURIComponent(folded(input.title))}`;
    const whole = match[1].padStart(12, '0');
    const fraction = (match[2] || '').padEnd(12, '0').slice(0, 12);
    const suffix = match[3].trim();
    return `n|${whole}|${fraction}|${encodeURIComponent(match[2] || '')}|${encodeURIComponent(suffix)}|${encodeURIComponent(folded(input.title))}`;
}
