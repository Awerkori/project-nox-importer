import { canonicalAdultTagName } from '../core/tag-normalization.js';
import { mergeAlternativeTitles } from '../core/alternative-titles.js';
const html = (value) => value
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&').replace(/&quot;/gi, '"').replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, '<').replace(/&gt;/gi, '>');
export function sanitizeSynopsis(value) {
    if (!value)
        return null;
    const normalized = html(value).replace(/<\/?(?:p|br)\s*\/?>/gi, '\n').replace(/<[^>]*>/g, '')
        .replace(/\r\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim();
    if (normalized.length < 12 || /cloudflare|just a moment|sign\s*in|access denied|enable javascript/i.test(normalized))
        return null;
    return normalized.slice(0, 20_000);
}
export function normalizeSourceTag(raw) {
    const value = html(raw).normalize('NFKC').replace(/\s+/g, ' ').trim();
    if (!value || value.length > 80 || /^(status|ongoing|completed|latest|home|menu)$/i.test(value))
        return null;
    // Adult is a rating alias only. Specific categories remain separate.
    return canonicalAdultTagName(value) || value;
}
export function mergeMetadata(existing, incoming, sourceRank = 100) {
    const result = { ...existing };
    const strings = ['title', 'coverUrl', 'author', 'artist', 'status', 'kind'];
    for (const field of strings) {
        const value = incoming[field];
        if (typeof value !== 'string' || !value.trim())
            continue;
        const old = result[field];
        if (!old?.protected && (!old?.validated || sourceRank < (old.sourceRank ?? Infinity))) {
            result[field] = { value: value.trim(), validated: true, sourceRank };
        }
    }
    const synopsis = sanitizeSynopsis(incoming.synopsis);
    const oldSynopsis = result.synopsis;
    if (synopsis && !oldSynopsis?.protected && (!oldSynopsis?.value || synopsis.length >= oldSynopsis.value.length || !oldSynopsis.validated)) {
        result.synopsis = { value: synopsis, validated: true, sourceRank };
    }
    const aliases = mergeAlternativeTitles(result.aliases?.value, (incoming.aliases || []).map(html), (result.title?.value || incoming.title));
    if (aliases.length)
        result.aliases = { value: aliases, validated: true, sourceRank: Math.min(result.aliases?.sourceRank ?? Infinity, sourceRank) };
    const tags = [...new Set([...(result.tags?.value || []), ...(incoming.tags || []).map(normalizeSourceTag).filter((x) => Boolean(x))])];
    if (tags.length)
        result.tags = { value: tags, validated: true, sourceRank: Math.min(result.tags?.sourceRank ?? Infinity, sourceRank) };
    return result;
}
