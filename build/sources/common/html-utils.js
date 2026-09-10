export function slugify(text) {
    return text
        .normalize('NFD')
        .replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
}
export function decodeHtmlEntities(str) {
    return str
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#039;/g, "'")
        .replace(/&#8217;/g, "'")
        .replace(/&#8216;/g, "'")
        .replace(/&#8220;/g, '"')
        .replace(/&#8221;/g, '"')
        .replace(/&nbsp;/g, ' ')
        .trim();
}
export function stripHtml(str) {
    return decodeHtmlEntities(str.replace(/<[^>]*>/g, ' ')).replace(/\s+/g, ' ').trim();
}
export function extractChapterNumber(str) {
    const match = str.match(/(?:capitulo|cap[ií]tulo|cap\.?|ep\.?|ch\.?)[-_\s]*(\d+(?:[.,]\d+)?)/i) ||
        str.match(/(\d+(?:[.,]\d+)?)/);
    if (!match)
        return 0;
    const num = parseFloat(match[1].replace(',', '.'));
    return isNaN(num) ? 0 : Number(num.toFixed(4));
}
