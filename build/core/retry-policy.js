import { NoxWorkerStorageError } from '../storage/worker.js';
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
export class RetryPolicy {
    /**
     * Classifica rigorosamente o erro considerando a ORIGEM (Storage vs Provider)
     * e o status HTTP, utilizando Typed Errors.
     */
    static classify(err) {
        const message = err?.message || String(err);
        // 1. Erros do Storage Bridge (NoxWorkerStorageError)
        if (err instanceof NoxWorkerStorageError || err?.name === 'NoxWorkerStorageError' || err?.stage === 'http' || err?.stage === 'auth') {
            const status = err.status;
            if (status === 429) {
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
                    retryClass: 'QUEUE_RETRY_429',
                    isTransient: true,
                    isPermanent: false,
                    retryAfterSeconds: waitSec,
                    message,
                    sourceStage: 'storage',
                };
            }
            if (status === 502) {
                return {
                    retryClass: 'QUEUE_RETRY_STORAGE_502',
                    isTransient: true,
                    isPermanent: false,
                    message,
                    sourceStage: 'storage',
                };
            }
            if (status === 503) {
                return {
                    retryClass: 'QUEUE_RETRY_STORAGE_503',
                    isTransient: true,
                    isPermanent: false,
                    message,
                    sourceStage: 'storage',
                };
            }
            if (status === 401 || status === 403) {
                return {
                    retryClass: 'QUEUE_RETRY_PERMANENT',
                    isTransient: false,
                    isPermanent: true,
                    message,
                    sourceStage: 'storage',
                };
            }
            return {
                retryClass: 'QUEUE_RETRY_STORAGE_502',
                isTransient: true,
                isPermanent: false,
                message,
                sourceStage: 'storage',
            };
        }
        // 2. Erros de Download da Fonte Externa (ProviderDownloadError)
        if (err instanceof ProviderDownloadError || err?.name === 'ProviderDownloadError') {
            const is404 = err.status === 404;
            return {
                retryClass: 'QUEUE_RETRY_PROVIDER',
                isTransient: !is404,
                isPermanent: is404,
                message,
                sourceStage: 'provider',
            };
        }
        // 3. Status HTTP em erro genérico ou textual
        const status = err?.status || err?.statusCode;
        if (status === 429 || /429|rate\s*limit|floodwait/i.test(message)) {
            let waitSec = 60;
            const retryAfter = err?.retryAfter || err?.headers?.get?.('retry-after');
            if (retryAfter) {
                const parsed = parseInt(retryAfter, 10);
                if (!isNaN(parsed) && parsed > 0)
                    waitSec = Math.min(3600, parsed);
            }
            const isStorage = /telegram|storage|worker|upload|media/i.test(message);
            return {
                retryClass: 'QUEUE_RETRY_429',
                isTransient: true,
                isPermanent: false,
                retryAfterSeconds: waitSec,
                message,
                sourceStage: isStorage ? 'storage' : 'provider',
            };
        }
        if (/timeout|aborted|ETIMEDOUT|ECONNRESET/i.test(message)) {
            return {
                retryClass: 'QUEUE_RETRY_TIMEOUT',
                isTransient: true,
                isPermanent: false,
                message,
                sourceStage: 'system',
            };
        }
        if (status === 401 || status === 403 || /unauthorized|forbidden/i.test(message)) {
            return {
                retryClass: 'QUEUE_RETRY_PERMANENT',
                isTransient: false,
                isPermanent: true,
                message,
                sourceStage: /storage|telegram|worker/i.test(message) ? 'storage' : 'provider',
            };
        }
        if (/storage.*502|502.*storage|Internal storage upload failed.*502|502/i.test(message) && /storage|telegram|worker/i.test(message)) {
            return {
                retryClass: 'QUEUE_RETRY_STORAGE_502',
                isTransient: true,
                isPermanent: false,
                message,
                sourceStage: 'storage',
            };
        }
        if (/storage.*503|503.*storage|Internal storage upload failed.*503|503/i.test(message) && /storage|telegram|worker/i.test(message)) {
            return {
                retryClass: 'QUEUE_RETRY_STORAGE_503',
                isTransient: true,
                isPermanent: false,
                message,
                sourceStage: 'storage',
            };
        }
        if (/PERMANENT_404_UNRESOLVED/i.test(message)) {
            return {
                retryClass: 'FAILED',
                isTransient: false,
                isPermanent: true,
                message,
                sourceStage: 'provider',
            };
        }
        if (/Failed to download image|404|Not Found/i.test(message)) {
            return {
                retryClass: 'QUEUE_RETRY_PROVIDER',
                isTransient: false,
                isPermanent: true,
                message,
                sourceStage: 'provider',
            };
        }
        // 4. Default: fallback para Provider / Sistema
        return {
            retryClass: 'QUEUE_RETRY_PROVIDER',
            isTransient: true,
            isPermanent: false,
            message,
            sourceStage: 'system',
        };
    }
    /**
     * Determina o atraso exato em segundos sem penalidades duplas e com teto máximo sustentável.
     * REGRA DE OURO: Erros técnicos (502, 503, 429, timeout, network, provider, auth, lease)
     * NUNCA terminam em FAILED por atingir max_attempts. Eles permanecem em RETRY com backoff.
     */
    static decide(classification, attempts, _maxAttempts, options) {
        // Falha fatal irrecuperável somente se classificada explicitamente como FAILED (dados corrompidos)
        if (classification.retryClass === 'FAILED') {
            return {
                status: 'FAILED',
                delaySeconds: 0,
                reason: classification.message || 'Fatal permanent failure',
            };
        }
        switch (classification.retryClass) {
            case 'QUEUE_RETRY_STORAGE_502':
            case 'QUEUE_RETRY_STORAGE_503':
            case 'QUEUE_RETRY_TIMEOUT': {
                // Nível 2 - Fila curta para erros transitórios de Storage / Rede:
                // Attempt 1: 30s + jitter (30-35s)
                // Attempt 2: 60s + jitter (60-70s)
                // Attempt 3: 120s + jitter (120-135s)
                // Attempt 4: 240s + jitter (240-270s)
                // Teto sustentável: 300 segundos (5 minutos), retry perpétuo até restabelecer
                const baseDelays = [30, 60, 120, 240];
                const base = baseDelays[attempts - 1] ?? 300;
                const jitter = Math.floor(Math.random() * (base * 0.15));
                const delay = Math.min(300, base + jitter);
                return {
                    status: 'RETRY',
                    delaySeconds: delay,
                    reason: `${classification.retryClass} persistent backoff (${delay}s, attempt ${attempts})`,
                };
            }
            case 'QUEUE_RETRY_429': {
                // Autoridade do Retry-After + micro-jitter de 1 a 3s para evitar thundering herd
                const wait = classification.retryAfterSeconds ?? 60;
                const jitter = Math.floor(Math.random() * 3) + 1;
                return {
                    status: 'RETRY',
                    delaySeconds: wait + jitter,
                    reason: `Storage/Provider 429 Retry-After authority (${wait + jitter}s)`,
                };
            }
            case 'QUEUE_RETRY_PROVIDER': {
                // Falha transitória de provider externo: 45s -> 90s -> 180s -> 300s (persistente)
                const baseDelays = [45, 90, 180, 300];
                const base = baseDelays[attempts - 1] ?? 300;
                const jitter = Math.floor(Math.random() * 10);
                return {
                    status: 'RETRY',
                    delaySeconds: Math.min(300, base + jitter),
                    reason: `Provider persistent retry backoff (${base + jitter}s, attempt ${attempts})`,
                };
            }
            case 'QUEUE_RETRY_PERMANENT': {
                // Problemas de credencial/auth expirada ou 404 em provider externo:
                // NÃO mata o job. Aguarda em intervalo maior (300s a 600s) para intervenção humana ou restabelecimento
                const delay = Math.min(600, Math.max(300, attempts * 60));
                return {
                    status: 'RETRY',
                    delaySeconds: delay,
                    reason: `Provider auth/not-found persistent retry (${delay}s, attempt ${attempts})`,
                };
            }
            default: {
                const base = Math.min(300, Math.pow(2, Math.min(attempts, 6)) * 15);
                return {
                    status: 'RETRY',
                    delaySeconds: base,
                    reason: `Generic persistent retry backoff (${base}s, attempt ${attempts})`,
                };
            }
        }
    }
}
