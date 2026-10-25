export interface CoverInspectionTarget {
    workId?: string;
    slug?: string;
    title: string;
    aliases?: readonly string[];
    coverUrl?: string;
    coverBuffer?: Buffer;
    headers?: Record<string, string>;
    source?: string;
    author?: string;
    artist?: string;
    synopsis?: string;
}
export interface CoverInspectionResult {
    inspected: boolean;
    foundPtBrTitle: boolean;
    detectedTitle: string | null;
    confidence: number;
    method?: 'PTBR_FOUND_COVER';
    evidence: {
        source?: string;
        coverUrl?: string;
        sha256?: string;
        rawLines: string[];
        filteredCandidates: string[];
        matchedTitle?: string;
        durationMs: number;
        reason?: string;
    };
}
export declare function downloadCoverWithLimit(url: string, headers?: Record<string, string>, maxBytes?: number, timeoutMs?: number): Promise<{
    ok: boolean;
    buffer?: Buffer;
    reason?: string;
}>;
/**
 * Visually inspects a manga/manhwa cover artwork using OCR + context validation
 * to detect Brazilian Portuguese titles rendered directly onto the cover art.
 */
export declare function inspectCoverForPtBrTitle(target: CoverInspectionTarget): Promise<CoverInspectionResult>;
