export function slugify(text: string): string {
  return text
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

export function decodeHtmlEntities(str: string): string {
  if (!str) return '';
  return str
    .replace(/&#8211;/g, '–')
    .replace(/&#8212;/g, '—')
    .replace(/&#8216;/g, '‘')
    .replace(/&#8217;/g, '’')
    .replace(/&#8220;/g, '“')
    .replace(/&#8221;/g, '”')
    .replace(/&#8230;/g, '…')
    .replace(/&hellip;/g, '…')
    .replace(/\[&hellip;\]/g, '…')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/&ndash;/g, '–')
    .replace(/&mdash;/g, '—')
    .replace(/&#(\d+);/g, (_, dec) => {
      const code = Number(dec);
      return !isNaN(code) && code > 0 && code < 65536 ? String.fromCharCode(code) : '';
    })
    .replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => {
      const code = parseInt(hex, 16);
      return !isNaN(code) && code > 0 && code < 65536 ? String.fromCharCode(code) : '';
    })
    .trim();
}

export function stripHtml(str: string): string {
  return decodeHtmlEntities(str.replace(/<[^>]*>/g, ' ')).replace(/\s+/g, ' ').trim();
}

export function extractChapterNumber(str: string): number {
  const match = str.match(/(?:capitulo|cap[ií]tulo|cap\.?|ep\.?|ch\.?)[-_\s]*(\d+(?:[.,]\d+|-\d+)?)/i) ||
                str.match(/(\d+(?:[.,]\d+|-\d+)?)/);
  if (!match) return 0;
  const raw = match[1].replace('-', '.').replace(',', '.');
  const num = parseFloat(raw);
  return isNaN(num) ? 0 : Number(num.toFixed(4));
}

