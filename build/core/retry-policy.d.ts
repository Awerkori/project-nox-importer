export type RetryErrorClass = 'LOCAL_RETRY' | 'QUEUE_RETRY_STORAGE_502' | 'QUEUE_RETRY_STORAGE_503' | 'QUEUE_RETRY_TIMEOUT' | 'QUEUE_RETRY_429' | 'QUEUE_RETRY_PROVIDER' | 'QUEUE_RETRY_PERMANENT' | 'FAILED';
export interface RetryClassification {
    retryClass: RetryErrorClass;
    isTransient: boolean;
    isPermanent: boolean;
    retryAfterSeconds?: number;
    message: string;
    sourceStage: 'storage' | 'provider' | 'system';
}
export interface RetryDecision {
    status: 'RETRY' | 'FAILED';
    delaySeconds: number;
    reason: string;
}
export declare class ProviderDownloadError extends Error {
    readonly status: number;
    readonly url: string;
    readonly source: string;
    constructor(status: number, url: string, source: string, message?: string);
}
export declare class RetryPolicy {
    /**
     * Classifica rigorosamente o erro considerando a ORIGEM (Storage vs Provider)
     * e o status HTTP, utilizando Typed Errors.
     */
    static classify(err: any): RetryClassification;
    /**
     * Determina o atraso exato em segundos sem penalidades duplas e com teto máximo sustentável.
     * REGRA DE OURO: Erros técnicos (502, 503, 429, timeout, network, provider, auth, lease)
     * NUNCA terminam em FAILED por atingir max_attempts. Eles permanecem em RETRY com backoff.
     */
    static decide(classification: RetryClassification, attempts: number, _maxAttempts: number, options?: {
        isStaffPriority?: boolean;
    }): RetryDecision;
}
