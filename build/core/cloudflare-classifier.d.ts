export type CloudflareClassification = 'HOTLINK_REFERER_REQUIRED' | 'RATE_LIMITED' | 'BURST_LIMITED' | 'TEMPORARY_WAF' | 'DATACENTER_ASN_BLOCK' | 'CLOUDFLARE_DATACENTER_BLOCK' | 'JS_CHALLENGE' | 'TURNSTILE' | 'IMAGE_CDN_BLOCK' | 'API_BLOCK' | 'UNKNOWN_CLOUDFLARE';
export interface CloudflareInspectionResult {
    isBlocked: boolean;
    isChallenge: boolean;
    classification: CloudflareClassification | null;
    cfRay: string | null;
    server: string | null;
    retryAfterSeconds: number | null;
    isFakeContent: boolean;
    isValidImage: boolean;
    reason: string;
}
export declare class CloudflareClassifier {
    /**
     * Magic bytes for image formats.
     */
    private static isBinaryImage;
    /**
     * Classify HTTP response from upstream.
     */
    static inspect(status: number, headers: Headers | Record<string, string | null | undefined> | undefined, bodyText: string, context?: {
        url?: string;
        expectedType?: 'json' | 'image' | 'html';
        buffer?: ArrayBuffer | Uint8Array;
        isIsolatedRequest?: boolean;
        localStatus?: number;
    }): CloudflareInspectionResult;
}
