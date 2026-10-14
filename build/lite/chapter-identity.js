const clean = (value) => String(value ?? '').normalize('NFKC').replace(/\s+/g, ' ').trim();
const folded = (value) => clean(value).toLocaleLowerCase('und');
export function chapterKind(input) {
    const explicit = folded(input.kind);
    if (explicit)
        return explicit;
    const title = folded(input.title);
    return /(?:extra|especial|special|omake|prologue|pr[oó]logo|side)/i.test(title) ? 'special' : 'chapter';
}
/** A stable opaque key; callers must never parse it back into a float. */
export function canonicalChapterKey(input) {
    const number = folded(input.displayNumber);
    const title = folded(input.title);
    const kind = chapterKind(input);
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
