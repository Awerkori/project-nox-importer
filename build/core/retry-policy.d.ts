export type ErrorTaxonomyCode = 'TRANSIENT_NETWORK' | 'SOURCE_TIMEOUT' | 'SOURCE_429' | 'SOURCE_403' | 'SOURCE_404' | 'SOURCE_5XX' | 'IMAGE_404' | 'IMAGE_TIMEOUT' | 'TELEGRAM_TRANSIENT' | 'TELEGRAM_RATE_LIMIT' | 'DB_TRANSIENT' | 'LEASE_EXPIRED' | 'WORKER_STALL' | 'PARSER_ERROR' | 'EMPTY_PAGES' | 'INVALID_MEDIA' | 'PERMANENT_NOT_FOUND' | 'SOURCE_PAUSED' | 'CIRCUIT_BREAKER_OPEN' | 'RETRY_BUDGET_EXHAUSTED' | 'UNKNOWN';
export type RetryErrorClass = 'LOCAL_RETRY' | 'QUEUE_RETRY_STORAGE_502' | 'QUEUE_RETRY_STORAGE_503' | 'QUEUE_RETRY_TIMEOUT' | 'QUEUE_RETRY_429' | 'QUEUE_RETRY_PROVIDER' | 'QUEUE_RETRY_PERMANENT' | 'FAILED';
export interface RetryClassification {
    taxonomyCode: ErrorTaxonomyCode;
    retryClass: RetryErrorClass;
    isTransient: boolean;
    isPermanent: boolean;
    retryBudgetMax: number;
    needsRevalidation: boolean;
    retryAfterSeconds?: number;
    message: string;
    structuredMessage: string;
    sourceStage: 'storage' | 'provider' | 'database' | 'system';
}
export interface RetryDecision {
    status: 'RETRY' | 'FAILED';
    delaySeconds: number;
    reason: string;
}
export declare const MAX_RETRY_BUDGET_GLOBAL = 7;
export declare const CATEGORY_RETRY_BUDGET: Record<ErrorTaxonomyCode, number>;
export declare class ProviderDownloadError extends Error {
    readonly status: number;
    readonly url: string;
    readonly source: string;
    constructor(status: number, url: string, source: string, message?: string);
}
export declare class InvalidMediaError extends Error {
    readonly url: string;
    readonly source: string;
    constructor(url: string, source: string, message?: string);
}
export declare function callProvider<T>(operation: () => Promise<T>): Promise<T>;
export declare class RetryPolicy {
    /**
     * Classifica rigorosamente o erro considerando a TAXONOMIA DE ERROS COMPLETA,
     * a ORIGEM (Storage vs Provider vs Database vs System) e o status HTTP.
     */
    static classify(err: any): RetryClassification;
    /**
     * Determina a decisão estrita de retry aplicando o RETRY BUDGET por categoria.
     * NENHUM JOB ULTRAPASSA O RETRY BUDGET.
     * Jobs que excedem o teto de tentativas são movidos terminantemente para FAILED.
     */
    static decide(classification: RetryClassification, attempts: number, maxAttempts?: number, _options?: {
        isStaffPriority?: boolean;
    }): RetryDecision;
}
