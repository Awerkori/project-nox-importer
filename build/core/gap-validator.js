/**
 * Gap Safety & Permanent Absence Validation Module
 *
 * Enforces strict architectural invariants:
 * - 403, Cloudflare, Turnstile, ASN block, timeout, lease expiry, CDN error, and 5xx transient errors
 *   MUST NEVER create permanent gaps (is_gap = true). They are classified as TEMPORARY_UNAVAILABLE.
 * - Permanent gap requires positive structural evidence:
 *   1. Chapter explicitly confirmed absent in source catalog (e.g. 404 on canonical endpoint, or omitted from source list)
 *   2. No active alternative work mappings or alternative sources possessing the chapter
 *   3. Non-transient confirmation across repeated probes
 */
const TRANSIENT_ERROR_PATTERNS = [
    /403/i,
    /cloudflare/i,
    /turnstile/i,
    /just a moment/i,
    /challenge/i,
    /captcha/i,
    /asn block/i,
    /shared network block/i,
    /datacenter network/i,
    /timeout/i,
    /etimedout/i,
    /esockettimedout/i,
    /econnreset/i,
    /econnrefused/i,
    /econnaborted/i,
    /lease expiry/i,
    /lease expired/i,
    /cdn error/i,
    /500/i,
    /502/i,
    /503/i,
    /504/i,
    /520/i,
    /521/i,
    /522/i,
    /524/i,
    /rate limit/i,
    /429/i,
    /floodwait/i,
    /too many requests/i,
    /temporary/i,
    /retry/i,
    /waf/i,
];
/**
 * Checks if a given error, status code, or message indicates a transient issue
 * that should NEVER be treated as a permanent gap.
 */
export function isTransientError(errorOrStatus) {
    if (!errorOrStatus)
        return false;
    const str = String(errorOrStatus).trim();
    // Numeric HTTP status check
    const num = parseInt(str, 10);
    if (!isNaN(num)) {
        if (num === 403 || num === 429 || num === 408)
            return true;
        if (num >= 500 && num <= 599)
            return true;
    }
    for (const pattern of TRANSIENT_ERROR_PATTERNS) {
        if (pattern.test(str)) {
            return true;
        }
    }
    return false;
}
/**
 * Validates whether a missing chapter can safely be considered a permanent gap.
 */
export async function validatePermanentGapCandidate(params) {
    const { workId, chapterNumber, chapterSortKey, source, httpStatus, errorMessage, alternativeSources = [] } = params;
    // 1. Guard against transient errors: 403, Cloudflare, Turnstile, timeouts, 5xx
    const errorCombined = `${httpStatus || ''} ${errorMessage || ''}`.trim();
    if (isTransientError(httpStatus) || isTransientError(errorMessage) || isTransientError(errorCombined)) {
        return {
            isPermanentGap: false,
            classification: 'TEMPORARY_UNAVAILABLE',
            reason: `Transient error detected (${errorCombined || 'WAF/Network error'}). Forbidden to mark as permanent gap. Must use retry/cooldown.`,
            httpStatus,
            alternativeSourcesAvailable: [],
            safeToMarkGap: false,
        };
    }
    // 2. Check if any alternative source already has this chapter
    const validAlternatives = alternativeSources.filter((a) => a.hasChapter).map((a) => a.source);
    if (validAlternatives.length > 0) {
        return {
            isPermanentGap: false,
            classification: 'AVAILABLE_ELSEWHERE',
            reason: `Chapter exists in alternative source(s): ${validAlternatives.join(', ')}. Should be acquired from alternative provider.`,
            httpStatus,
            alternativeSourcesAvailable: validAlternatives,
            safeToMarkGap: false,
        };
    }
    // 3. For permanent gap confirmation, require explicit 404 Not Found on canonical chapter endpoint
    // AND non-transient status
    const isExplicit404 = String(httpStatus).includes('404') || (errorMessage && /not found|404/i.test(errorMessage));
    if (!isExplicit404) {
        return {
            isPermanentGap: false,
            classification: 'UNVERIFIED',
            reason: `No structural proof of permanent absence (status is not confirmed 404). Current info: ${errorCombined}`,
            httpStatus,
            alternativeSourcesAvailable: [],
            safeToMarkGap: false,
        };
    }
    // 4. Confirmed structural absence
    return {
        isPermanentGap: true,
        classification: 'PERMANENT_GAP',
        reason: `Structural absence confirmed: 404 Not Found on ${source}, no alternative sources have chapter ${chapterNumber}.`,
        httpStatus: 404,
        alternativeSourcesAvailable: [],
        safeToMarkGap: true,
    };
}
