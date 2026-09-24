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

export interface GapValidationResult {
  isPermanentGap: boolean;
  classification: 'PERMANENT_GAP' | 'TEMPORARY_UNAVAILABLE' | 'AVAILABLE_ELSEWHERE' | 'UNVERIFIED';
  reason: string;
  httpStatus?: number | string;
  alternativeSourcesAvailable: string[];
  safeToMarkGap: boolean;
}

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
  /lease[_\s-]expir/i,
  /lease/i,
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
export function isTransientError(errorOrStatus: string | number | undefined | null): boolean {
  if (!errorOrStatus) return false;
  const str = String(errorOrStatus).trim();

  // Numeric HTTP status check
  const num = parseInt(str, 10);
  if (!isNaN(num)) {
    if (num === 403 || num === 429 || num === 408) return true;
    if (num >= 500 && num <= 599) return true;
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
export interface ValidateGapCandidateParams {
  workId: string;
  chapterNumber: number | string;
  chapterSortKey: number;
  source: string;
  httpStatus?: number | string;
  errorMessage?: string;
  alternativeSources?: Array<{ source: string; hasChapter: boolean }>;
  /**
   * Structural proof A: Chapter is confirmed absent from upstream catalog/listing
   */
  chapterAbsentFromUpstreamCatalog?: boolean;
  /**
   * Structural proof B: 404 confirmed across repeated independent probes
   */
  repeatedNotFoundConfirmed?: boolean;
  consecutiveNotFoundCount?: number;
}

/**
 * Validates whether a missing chapter can safely be considered a permanent gap.
 */
export async function validatePermanentGapCandidate(
  params: ValidateGapCandidateParams
): Promise<GapValidationResult> {
  const {
    workId,
    chapterNumber,
    chapterSortKey,
    source,
    httpStatus,
    errorMessage,
    alternativeSources = [],
    chapterAbsentFromUpstreamCatalog = false,
    repeatedNotFoundConfirmed = false,
    consecutiveNotFoundCount = 1,
  } = params;

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

  // 3. Structural Evidence Requirement:
  // An automated permanent gap MUST prove structural absence via either:
  // Proof A: Chapter explicitly confirmed absent in upstream catalog/listing
  // Proof B: 404 confirmed repeatedly across independent probes (>= 2)
  // A single isolated 404 is UNVERIFIED.
  const isExplicit404 = String(httpStatus).includes('404') || (errorMessage && /not found|404/i.test(errorMessage));
  const hasProofA = chapterAbsentFromUpstreamCatalog === true;
  const hasProofB = repeatedNotFoundConfirmed === true || consecutiveNotFoundCount >= 2;

  if (!isExplicit404 && !hasProofA) {
    return {
      isPermanentGap: false,
      classification: 'UNVERIFIED',
      reason: `No structural proof of permanent absence (status is not confirmed 404 or absent). Current info: ${errorCombined}`,
      httpStatus,
      alternativeSourcesAvailable: [],
      safeToMarkGap: false,
    };
  }

  if (isExplicit404 && !hasProofA && !hasProofB) {
    return {
      isPermanentGap: false,
      classification: 'UNVERIFIED',
      reason: `Single isolated 404 is insufficient for permanent gap. Requires repeated probe confirmation (>= 2) or upstream catalog absence proof.`,
      httpStatus: 404,
      alternativeSourcesAvailable: [],
      safeToMarkGap: false,
    };
  }

  // 4. Confirmed structural absence (Proof A or Proof B met, no alternatives possess chapter)
  const proofDetail = hasProofA
    ? 'confirmed absent from upstream catalog listing'
    : `confirmed 404 across repeated probes (${consecutiveNotFoundCount} probes)`;

  return {
    isPermanentGap: true,
    classification: 'PERMANENT_GAP',
    reason: `Structural absence confirmed (${proofDetail}) on ${source}, no alternative sources have chapter ${chapterNumber}.`,
    httpStatus: 404,
    alternativeSourcesAvailable: [],
    safeToMarkGap: true,
  };
}

export interface MarkGapParams extends ValidateGapCandidateParams {}

export interface MarkGapResult {
  mutated: boolean;
  validation: GapValidationResult;
  error?: string;
}

/**
 * The ONLY safe, authorized pathway to mark a permanent gap (is_gap = true) in the database.
 * Enforces pre-validation via validatePermanentGapCandidate.
 * If safeToMarkGap !== true, mutation is strictly forbidden and rejected.
 * FAILS CLOSED if alternative source query fails.
 */
export async function markPermanentGapSafely(
  client: any,
  params: MarkGapParams
): Promise<MarkGapResult> {
  // If alternativeSources wasn't explicitly supplied, check the database for actual alternative chapter mappings
  let alternatives = params.alternativeSources;
  let alternativeLookupFailed = false;
  let alternativeLookupError = '';

  if (!alternatives && client?.query) {
    try {
      const altRes = await client.query(
        `SELECT m.source,
                EXISTS (
                  SELECT 1 FROM importer_chapter_mappings cm
                  WHERE cm.work_id = m.work_id
                    AND cm.source = m.source
                    AND cm.chapter_sort_key = $2
                    AND cm.status IN ('COMPLETED', 'STAGED', 'IMPORTING', 'PENDING')
                    AND cm.is_gap = false
                ) as has_chapter
         FROM importer_work_mappings m
         WHERE m.work_id = $1::uuid AND m.source != $3;`,
        [params.workId, params.chapterSortKey, params.source]
      );
      alternatives = altRes.rows.map((r: any) => ({
        source: r.source,
        hasChapter: Boolean(r.has_chapter),
      }));
    } catch (err: any) {
      alternativeLookupFailed = true;
      alternativeLookupError = err?.message || String(err);
    }
  }

  // STRICT FAIL-CLOSED: If alternative source lookup failed, we CANNOT assume 0 alternatives!
  if (alternativeLookupFailed) {
    const unverifiedValidation: GapValidationResult = {
      isPermanentGap: false,
      classification: 'UNVERIFIED',
      reason: `ALTERNATIVE_SOURCE_CHECK_FAILED: Query failed (${alternativeLookupError}). Cannot safely confirm permanent absence without reliable alternative verification.`,
      httpStatus: params.httpStatus,
      alternativeSourcesAvailable: [],
      safeToMarkGap: false,
    };
    return {
      mutated: false,
      validation: unverifiedValidation,
      error: `Mutation rejected by GapValidator: ${unverifiedValidation.reason}`,
    };
  }

  const validation = await validatePermanentGapCandidate({
    ...params,
    alternativeSources: alternatives || [],
  });

  if (!validation.safeToMarkGap) {
    return {
      mutated: false,
      validation,
      error: `Mutation rejected by GapValidator: ${validation.reason}`,
    };
  }

  // Permitted to mark permanent gap ONLY with confirmed structural absence
  try {
    await client.query(
      `UPDATE importer_chapter_mappings
       SET is_gap = true,
           status = 'COMPLETED',
           last_error = $1,
           updated_at = NOW()
       WHERE work_id = $2::uuid
         AND chapter_sort_key = $3
         AND source = $4;`,
      [
        `PERMANENT_GAP_VALIDATED: ${validation.reason}`,
        params.workId,
        params.chapterSortKey,
        params.source,
      ]
    );

    return {
      mutated: true,
      validation,
    };
  } catch (dbErr: any) {
    return {
      mutated: false,
      validation,
      error: `Database update failed: ${dbErr?.message}`,
    };
  }
}

