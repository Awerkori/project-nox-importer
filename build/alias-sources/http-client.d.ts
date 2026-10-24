import type { AliasSourceHealthSnapshot, AliasSourceId } from './types.js';
export interface SourceHttpClientOptions {
    source: AliasSourceId;
    maxConcurrent: number;
    minIntervalMs: number;
    timeoutMs: number;
    maxRetries: number;
    circuitBreakerThreshold: number;
    circuitBreakerCooldownMs: number;
    /** HTTP status codes + body patterns that represent deterministic access restrictions (no retry spam) */
    accessRestrictionDetector?: (status: number, body: string) => string | null;
    cacheMaxEntries?: number;
    fetchImpl?: typeof fetch;
}
export interface HttpJsonResponse<T = unknown> {
    ok: boolean;
    status: number;
    data?: T;
    rawText?: string;
    errorCode?: string;
    errorMessage?: string;
    latencyMs: number;
    fromCache?: boolean;
}
/**
 * Per-source adaptive HTTP client with:
 * - Bounded concurrency semaphore
 * - Token-spacing rate limiter + automatic 429/Retry-After backoff
 * - In-flight request deduplication + LRU response cache
 * - Circuit breaker for sustained failures or explicit access restrictions
 */
export declare class SourceHttpClient {
    private readonly options;
    private active;
    private readonly waiters;
    private nextSlotAt;
    private adaptiveIntervalMs;
    private consecutiveFailures;
    private circuitOpenUntil;
    private circuitReason;
    private requestsTotal;
    private cacheHits;
    private successTotal;
    private errorTotal;
    private rateLimitedTotal;
    private latencySumMs;
    private latencySamples;
    private readonly cache;
    private readonly inflight;
    private readonly fetchFn;
    constructor(options: SourceHttpClientOptions);
    getHealthSnapshot(): AliasSourceHealthSnapshot;
    resetCircuit(): void;
    private acquireSlot;
    private releaseSlot;
    request<T = unknown>(url: string, init?: {
        headers?: Record<string, string>;
        parseAs?: 'json' | 'text';
        cacheKey?: string;
        bypassCircuit?: boolean;
    }): Promise<HttpJsonResponse<T>>;
    private executeWithRetries;
}
