import { NoxWorkerStorageError } from '../storage/worker.js';
export const MAX_RETRY_BUDGET_GLOBAL = 7;
export const CATEGORY_RETRY_BUDGET = {
    PERMANENT_NOT_FOUND: 1,
    IMAGE_404: 2,
    EMPTY_PAGES: 2,
    PARSER_ERROR: 2,
    SOURCE_404: 2,
    SOURCE_403: 2,
    INVALID_MEDIA: 3,
    LEASE_EXPIRED: 3,
    WORKER_STALL: 3,
    DB_TRANSIENT: 5,
    SOURCE_429: 5,
    SOURCE_5XX: 5,
    SOURCE_TIMEOUT: 5,
    IMAGE_TIMEOUT: 5,
    CIRCUIT_BREAKER_OPEN: 5,
    TRANSIENT_NETWORK: 6,
    TELEGRAM_TRANSIENT: 6,
    TELEGRAM_RATE_LIMIT: 6,
    SOURCE_PAUSED: 10,
    RETRY_BUDGET_EXHAUSTED: 0,
    UNKNOWN: 4,
};
export class ProviderDownloadError extends Error {
    status;
    url;
    source;
    constructor(status, url, source, message) {
        super(message || `Provider download failed for source ${source}: HTTP ${status} from ${url}`);
        this.status = status;
        this.url = url;
        this.source = source;
        this.name = 'ProviderDownloadError';
    }
}
export class InvalidMediaError extends Error {
    url;
    source;
    constructor(url, source, message) {
        super(message || `Invalid media format returned by source ${source} for ${url}`);
        this.url = url;
        this.source = source;
        this.name = 'InvalidMediaError';
    }
}
export async function callProvider(operation) {
    try {
        return await operation();
    }
    catch (value) {
        const original = value instanceof Error ? value : new Error(String(value));
        const error = new Error(original.message, { cause: original });
        Object.assign(error, original, { sourceStage: 'provider' });
        throw error;
    }
}
export class RetryPolicy {
    /**
     * Classifica rigorosamente o erro considerando a TAXONOMIA DE ERROS COMPLETA,
     * a ORIGEM (Storage vs Provider vs Database vs System) e o status HTTP.
     */
    static classify(err) {
        const message = err?.message || String(err);
        const status = err?.status || err?.statusCode;
        const stage = err?.sourceStage || (err instanceof NoxWorkerStorageError ? 'storage' : undefined);
        // 1. Invalid Media Format (HTML page received instead of binary image, corrupt magic bytes)
        if (err instanceof InvalidMediaError ||
            err?.name === 'InvalidMediaError' ||
            /Formato n[ãa]o permitido/i.test(message) ||
            /invalid media|not a valid image|unsupported image format/i.test(message)) {
            return {
                taxonomyCode: 'INVALID_MEDIA',
                retryClass: 'QUEUE_RETRY_PROVIDER',
                isTransient: true,
                isPermanent: false,
                retryBudgetMax: CATEGORY_RETRY_BUDGET.INVALID_MEDIA,
                needsRevalidation: true,
                message,
                structuredMessage: `[INVALID_MEDIA] ${message}`,
                sourceStage: 'provider',
            };
        }
        // 2. Permanent 404 Unresolved (already marked as permanent gap)
        if (/PERMANENT_404_UNRESOLVED/i.test(message)) {
            return {
                taxonomyCode: 'IMAGE_404',
                retryClass: 'FAILED',
                isTransient: false,
                isPermanent: true,
                retryBudgetMax: 1,
                needsRevalidation: false,
                message,
                structuredMessage: `[IMAGE_404] ${message}`,
                sourceStage: 'provider',
            };
        }
        // 3. Empty Pages (Source returns 0 pages, or chapter contains 0 valid content pages)
        if (/failed to return pages|returned 0 pages|contains 0 valid content pages|0 valid content pages|incomplete chapter import/i.test(message)) {
            return {
                taxonomyCode: 'EMPTY_PAGES',
                retryClass: 'QUEUE_RETRY_PROVIDER',
                isTransient: false,
                isPermanent: true,
                retryBudgetMax: CATEGORY_RETRY_BUDGET.EMPTY_PAGES,
                needsRevalidation: true,
                message,
                structuredMessage: `[EMPTY_PAGES] ${message}`,
                sourceStage: 'provider',
            };
        }
        // 4. Lease Expired / Worker Unresponsive
        if (/Lease expired|worker unresponsive/i.test(message)) {
            return {
                taxonomyCode: 'LEASE_EXPIRED',
                retryClass: 'LOCAL_RETRY',
                isTransient: true,
                isPermanent: false,
                retryBudgetMax: CATEGORY_RETRY_BUDGET.LEASE_EXPIRED,
                needsRevalidation: false,
                message,
                structuredMessage: `[LEASE_EXPIRED] ${message}`,
                sourceStage: 'system',
            };
        }
        // 5. Worker Safety Timeout / Stall
        if (/JobExecutionTimeout|exceeded safety limit/i.test(message)) {
            return {
                taxonomyCode: 'WORKER_STALL',
                retryClass: 'LOCAL_RETRY',
                isTransient: true,
                isPermanent: false,
                retryBudgetMax: CATEGORY_RETRY_BUDGET.WORKER_STALL,
                needsRevalidation: false,
                message,
                structuredMessage: `[WORKER_STALL] ${message}`,
                sourceStage: 'system',
            };
        }
        // 6. Source Circuit Breaker OPEN
        if (/circuit breaker OPEN|UPSTREAM_BLOCKED.*No fallbacks/i.test(message)) {
            return {
                taxonomyCode: 'CIRCUIT_BREAKER_OPEN',
                retryClass: 'LOCAL_RETRY',
                isTransient: true,
                isPermanent: false,
                retryBudgetMax: CATEGORY_RETRY_BUDGET.CIRCUIT_BREAKER_OPEN,
                needsRevalidation: false,
                message,
                structuredMessage: `[CIRCUIT_BREAKER_OPEN] ${message}`,
                sourceStage: 'provider',
            };
        }
        // 7. Source is PAUSED or DISABLED
        if (/Source .* is (PAUSED|DISABLED)/i.test(message)) {
            return {
                taxonomyCode: 'SOURCE_PAUSED',
                retryClass: 'LOCAL_RETRY',
                isTransient: true,
                isPermanent: false,
                retryBudgetMax: CATEGORY_RETRY_BUDGET.SOURCE_PAUSED,
                needsRevalidation: false,
                message,
                structuredMessage: `[SOURCE_PAUSED] ${message}`,
                sourceStage: 'provider',
            };
        }
        // 8. Storage Bridge Errors (NoxWorkerStorageError)
        if (err instanceof NoxWorkerStorageError ||
            err?.name === 'NoxWorkerStorageError' ||
            err?.stage === 'http' ||
            err?.stage === 'auth' ||
            stage === 'storage') {
            if (status === 429 || /429|rate\s*limit|floodwait/i.test(message)) {
                let waitSec = 60;
                if (typeof err.retryAfter === 'number' && err.retryAfter > 0) {
                    waitSec = err.retryAfter;
                }
                else {
                    const match = /Retry-After:\s*(\d+)s/i.exec(message);
                    if (match)
                        waitSec = parseInt(match[1], 10);
                }
                return {
                    taxonomyCode: 'TELEGRAM_RATE_LIMIT',
                    retryClass: 'QUEUE_RETRY_429',
                    isTransient: true,
                    isPermanent: false,
                    retryBudgetMax: CATEGORY_RETRY_BUDGET.TELEGRAM_RATE_LIMIT,
                    needsRevalidation: false,
                    retryAfterSeconds: waitSec,
                    message,
                    structuredMessage: `[TELEGRAM_RATE_LIMIT] ${message}`,
                    sourceStage: 'storage',
                };
            }
            if (status === 502 || status === 503 || /502|503/i.test(message)) {
                return {
                    taxonomyCode: 'TELEGRAM_TRANSIENT',
                    retryClass: status === 503 ? 'QUEUE_RETRY_STORAGE_503' : 'QUEUE_RETRY_STORAGE_502',
                    isTransient: true,
                    isPermanent: false,
                    retryBudgetMax: CATEGORY_RETRY_BUDGET.TELEGRAM_TRANSIENT,
                    needsRevalidation: false,
                    message,
                    structuredMessage: `[TELEGRAM_TRANSIENT] ${message}`,
                    sourceStage: 'storage',
                };
            }
            if (status === 401 || status === 403 || /unauthorized|forbidden/i.test(message)) {
                return {
                    taxonomyCode: 'PERMANENT_NOT_FOUND',
                    retryClass: 'QUEUE_RETRY_PERMANENT',
                    isTransient: false,
                    isPermanent: true,
                    retryBudgetMax: 1,
                    needsRevalidation: false,
                    message,
                    structuredMessage: `[PERMANENT_NOT_FOUND] Storage auth error: ${message}`,
                    sourceStage: 'storage',
                };
            }
            return {
                taxonomyCode: 'TELEGRAM_TRANSIENT',
                retryClass: 'QUEUE_RETRY_STORAGE_502',
                isTransient: true,
                isPermanent: false,
                retryBudgetMax: CATEGORY_RETRY_BUDGET.TELEGRAM_TRANSIENT,
                needsRevalidation: false,
                message,
                structuredMessage: `[TELEGRAM_TRANSIENT] ${message}`,
                sourceStage: 'storage',
            };
        }
        // 9. Provider Download Errors (ProviderDownloadError)
        if (err instanceof ProviderDownloadError || err?.name === 'ProviderDownloadError') {
            const errStatus = err.status;
            if (errStatus === 404 || /404|not found/i.test(message)) {
                return {
                    taxonomyCode: 'IMAGE_404',
                    retryClass: 'QUEUE_RETRY_PROVIDER',
                    isTransient: false,
                    isPermanent: true,
                    retryBudgetMax: CATEGORY_RETRY_BUDGET.IMAGE_404,
                    needsRevalidation: true,
                    message,
                    structuredMessage: `[IMAGE_404] ${message}`,
                    sourceStage: 'provider',
                };
            }
            if (errStatus === 403 || /403|forbidden|cloudflare/i.test(message)) {
                return {
                    taxonomyCode: 'SOURCE_403',
                    retryClass: 'QUEUE_RETRY_PROVIDER',
                    isTransient: false,
                    isPermanent: true,
                    retryBudgetMax: CATEGORY_RETRY_BUDGET.SOURCE_403,
                    needsRevalidation: false,
                    message,
                    structuredMessage: `[SOURCE_403] ${message}`,
                    sourceStage: 'provider',
                };
            }
            if (errStatus === 429 || /429|rate\s*limit/i.test(message)) {
                return {
                    taxonomyCode: 'SOURCE_429',
                    retryClass: 'QUEUE_RETRY_429',
                    isTransient: true,
                    isPermanent: false,
                    retryBudgetMax: CATEGORY_RETRY_BUDGET.SOURCE_429,
                    needsRevalidation: false,
                    retryAfterSeconds: 60,
                    message,
                    structuredMessage: `[SOURCE_429] ${message}`,
                    sourceStage: 'provider',
                };
            }
            if (errStatus >= 500) {
                return {
                    taxonomyCode: 'SOURCE_5XX',
                    retryClass: 'QUEUE_RETRY_PROVIDER',
                    isTransient: true,
                    isPermanent: false,
                    retryBudgetMax: CATEGORY_RETRY_BUDGET.SOURCE_5XX,
                    needsRevalidation: false,
                    message,
                    structuredMessage: `[SOURCE_5XX] ${message}`,
                    sourceStage: 'provider',
                };
            }
            return {
                taxonomyCode: 'SOURCE_TIMEOUT',
                retryClass: 'QUEUE_RETRY_PROVIDER',
                isTransient: true,
                isPermanent: false,
                retryBudgetMax: CATEGORY_RETRY_BUDGET.SOURCE_TIMEOUT,
                needsRevalidation: false,
                message,
                structuredMessage: `[SOURCE_TIMEOUT] ${message}`,
                sourceStage: 'provider',
            };
        }
        // 10. Database Transient Errors (Yugabyte / YSQL / deadlocks)
        if (/yugabyte|ysql|deadlock|serialization failure|terminating connection|connection terminated/i.test(message)) {
            return {
                taxonomyCode: 'DB_TRANSIENT',
                retryClass: 'LOCAL_RETRY',
                isTransient: true,
                isPermanent: false,
                retryBudgetMax: CATEGORY_RETRY_BUDGET.DB_TRANSIENT,
                needsRevalidation: false,
                message,
                structuredMessage: `[DB_TRANSIENT] ${message}`,
                sourceStage: 'database',
            };
        }
        // 11. Rate limit (generic)
        if (status === 429 || /429|rate\s*limit|floodwait/i.test(message)) {
            let waitSec = 60;
            const retryAfter = err?.retryAfter || err?.headers?.get?.('retry-after');
            if (retryAfter) {
                const parsed = parseInt(retryAfter, 10);
                if (!isNaN(parsed) && parsed > 0)
                    waitSec = Math.min(3600, parsed);
            }
            const isStorage = /telegram|storage|worker|upload|media/i.test(message);
            const taxCode = isStorage ? 'TELEGRAM_RATE_LIMIT' : 'SOURCE_429';
            return {
                taxonomyCode: taxCode,
                retryClass: 'QUEUE_RETRY_429',
                isTransient: true,
                isPermanent: false,
                retryBudgetMax: CATEGORY_RETRY_BUDGET[taxCode],
                needsRevalidation: false,
                retryAfterSeconds: waitSec,
                message,
                structuredMessage: `[${taxCode}] ${message}`,
                sourceStage: isStorage ? 'storage' : 'provider',
            };
        }
        // 12. Timeouts & Connection Resets
        if (/timeout|aborted|ETIMEDOUT|ECONNRESET/i.test(message)) {
            const isImage = /image|page|story|cdn/i.test(message);
            const isProv = stage === 'provider' || isImage;
            const taxCode = isImage ? 'IMAGE_TIMEOUT' : isProv ? 'SOURCE_TIMEOUT' : 'TRANSIENT_NETWORK';
            return {
                taxonomyCode: taxCode,
                retryClass: 'QUEUE_RETRY_TIMEOUT',
                isTransient: true,
                isPermanent: false,
                retryBudgetMax: CATEGORY_RETRY_BUDGET[taxCode],
                needsRevalidation: false,
                message,
                structuredMessage: `[${taxCode}] ${message}`,
                sourceStage: isProv ? 'provider' : 'system',
            };
        }
        // 13. 404 Not Found (API / Provider / Chapter)
        if (status === 404 || /404 Not Found|status: 404|HTTP 404/i.test(message)) {
            const isImage = /image|page|narrative|cdn/i.test(message);
            const taxCode = isImage ? 'IMAGE_404' : 'SOURCE_404';
            return {
                taxonomyCode: taxCode,
                retryClass: 'QUEUE_RETRY_PROVIDER',
                isTransient: false,
                isPermanent: true,
                retryBudgetMax: CATEGORY_RETRY_BUDGET[taxCode],
                needsRevalidation: isImage,
                message,
                structuredMessage: `[${taxCode}] ${message}`,
                sourceStage: 'provider',
            };
        }
        // 14. 401 / 403 Forbidden
        if (status === 401 || status === 403 || /unauthorized|forbidden/i.test(message)) {
            const isStorage = /storage|telegram|worker/i.test(message);
            const taxCode = isStorage ? 'PERMANENT_NOT_FOUND' : 'SOURCE_403';
            return {
                taxonomyCode: taxCode,
                retryClass: 'QUEUE_RETRY_PERMANENT',
                isTransient: false,
                isPermanent: true,
                retryBudgetMax: CATEGORY_RETRY_BUDGET[taxCode],
                needsRevalidation: false,
                message,
                structuredMessage: `[${taxCode}] ${message}`,
                sourceStage: isStorage ? 'storage' : 'provider',
            };
        }
        // 15. Parser errors
        if (/SyntaxError|JSON\.parse|unexpected token/i.test(message)) {
            return {
                taxonomyCode: 'PARSER_ERROR',
                retryClass: 'QUEUE_RETRY_PROVIDER',
                isTransient: false,
                isPermanent: true,
                retryBudgetMax: CATEGORY_RETRY_BUDGET.PARSER_ERROR,
                needsRevalidation: false,
                message,
                structuredMessage: `[PARSER_ERROR] ${message}`,
                sourceStage: 'provider',
            };
        }
        // 16. Default fallback: Transient Network / System
        return {
            taxonomyCode: 'TRANSIENT_NETWORK',
            retryClass: 'LOCAL_RETRY',
            isTransient: true,
            isPermanent: false,
            retryBudgetMax: CATEGORY_RETRY_BUDGET.TRANSIENT_NETWORK,
            needsRevalidation: false,
            message,
            structuredMessage: `[TRANSIENT_NETWORK] ${message}`,
            sourceStage: stage === 'provider' ? 'provider' : 'system',
        };
    }
    /**
     * Determina a decisão estrita de retry aplicando o RETRY BUDGET por categoria.
     * NENHUM JOB ULTRAPASSA O RETRY BUDGET.
     * Jobs que excedem o teto de tentativas são movidos terminantemente para FAILED.
     */
    static decide(classification, attempts, maxAttempts = 7, _options) {
        // 1. Falha fatal irrecuperável imediata
        if (classification.retryClass === 'FAILED' || (classification.isPermanent && classification.retryBudgetMax <= 1)) {
            return {
                status: 'FAILED',
                delaySeconds: 0,
                reason: classification.structuredMessage || `[${classification.taxonomyCode}] Permanent unrecoverable failure: ${classification.message}`,
            };
        }
        // 2. Cálculo do Retry Budget efetivo
        const categoryBudget = CATEGORY_RETRY_BUDGET[classification.taxonomyCode] ?? MAX_RETRY_BUDGET_GLOBAL;
        const effectiveBudget = Math.min(Math.max(1, maxAttempts || MAX_RETRY_BUDGET_GLOBAL), categoryBudget, MAX_RETRY_BUDGET_GLOBAL);
        // 3. Esgotamento do Retry Budget: transição TERMINAL para FAILED
        if (attempts >= effectiveBudget) {
            return {
                status: 'FAILED',
                delaySeconds: 0,
                reason: `[RETRY_BUDGET_EXHAUSTED] Max attempts (${attempts}/${effectiveBudget}) reached for [${classification.taxonomyCode}]: ${classification.message}`,
            };
        }
        // 4. Backoff adaptativo proporcional às tentativas
        if (classification.taxonomyCode === 'TELEGRAM_RATE_LIMIT' || classification.taxonomyCode === 'SOURCE_429') {
            const wait = classification.retryAfterSeconds ?? 60;
            const jitter = Math.floor(Math.random() * 3) + 1;
            const delay = wait + jitter;
            return {
                status: 'RETRY',
                delaySeconds: delay,
                reason: `[${classification.taxonomyCode}] Rate limit authority backoff (${delay}s, attempt ${attempts}/${effectiveBudget})`,
            };
        }
        // Escala de atrasos:
        // Attempt 1: 30s + jitter (30-35s)
        // Attempt 2: 60s + jitter (60-70s)
        // Attempt 3: 120s + jitter (120-135s)
        // Attempt 4+: 180s - 300s (teto 300s)
        const baseDelays = [30, 60, 120, 240];
        const base = baseDelays[attempts - 1] ?? 300;
        const jitter = Math.floor(Math.random() * (base * 0.15));
        const delay = Math.min(300, base + jitter);
        return {
            status: 'RETRY',
            delaySeconds: delay,
            reason: `[${classification.taxonomyCode}] Adaptive backoff (${delay}s, attempt ${attempts}/${effectiveBudget}): ${classification.message}`,
        };
    }
}
