import { Logger } from '../core/logger.js';
import { GlobalStorageRateLimiter } from '../core/rate-limiter.js';
export class NoxWorkerStorageError extends Error {
    stage;
    status;
    constructor(stage = 'validation', status, message) {
        super(message || `NoxWorkerStorage operation failed at stage: ${stage}${status ? ` (status: ${status})` : ''}`);
        this.stage = stage;
        this.status = status;
        this.name = 'NoxWorkerStorageError';
    }
}
export class NoxWorkerStorageProvider {
    workerBaseUrl;
    bridgeToken;
    transport;
    logger = new Logger('NoxWorkerStorage');
    rateLimiter;
    constructor(workerBaseUrl, bridgeToken, transport = fetch, rateLimiter) {
        this.workerBaseUrl = workerBaseUrl;
        this.bridgeToken = bridgeToken;
        this.transport = transport;
        if (!bridgeToken) {
            throw new Error('NoxWorkerStorageProvider requires valid NOX_STORAGE_BRIDGE_TOKEN');
        }
        this.rateLimiter = rateLimiter || new GlobalStorageRateLimiter({ maxRequestsPerMinute: 105, minIntervalMs: 350 });
    }
    getRateLimiter() {
        return this.rateLimiter;
    }
    getProviderKey() {
        // Media provider key in database is 'telegram' so manga reader streams it correctly
        return 'telegram';
    }
    async healthCheck() {
        try {
            const url = `${this.workerBaseUrl.replace(/\/$/, '')}/api/internal/storage/upload`;
            const res = await this.transport(url, {
                method: 'GET',
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 ProjectNox-Importer/1.0',
                    Authorization: `Bearer ${this.bridgeToken}`,
                    Accept: 'application/json',
                },
                signal: AbortSignal.timeout(15_000),
            });
            if (!res.ok) {
                this.logger.warn('Storage healthCheck received non-OK response', { status: res.status });
                return false;
            }
            const data = (await res.json().catch(() => null));
            return data?.ok === true && data?.provider === 'telegram';
        }
        catch (err) {
            this.logger.error('Storage healthCheck network error', { error: err?.message });
            return false;
        }
    }
    async upload(bytes, mime, id) {
        const url = `${this.workerBaseUrl.replace(/\/$/, '')}/api/internal/storage/upload?id=${encodeURIComponent(id)}`;
        let lastError;
        const maxAttempts = 3;
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                // Enforce global rate limit across all sources (Token Bucket + Sliding Window cap at 105 req/min)
                await this.rateLimiter.acquire();
                const blobPart = bytes;
                const res = await this.transport(url, {
                    method: 'POST',
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 ProjectNox-Importer/1.0',
                        Authorization: `Bearer ${this.bridgeToken}`,
                        'Content-Type': mime || 'application/octet-stream',
                        'Content-Length': String(bytes.byteLength),
                    },
                    body: new Blob([blobPart], { type: mime || 'application/octet-stream' }),
                    signal: AbortSignal.timeout(60_000),
                });
                if (res.status === 401 || res.status === 403) {
                    throw new NoxWorkerStorageError('auth', res.status, 'Authentication failed on internal storage endpoint');
                }
                if (res.status === 429) {
                    let waitSec = 15;
                    const retryHeader = res.headers?.get ? res.headers.get('retry-after') : null;
                    if (retryHeader) {
                        const parsed = parseInt(retryHeader, 10);
                        if (!isNaN(parsed) && parsed > 0)
                            waitSec = parsed;
                    }
                    else {
                        try {
                            const bodyJson = await res.clone().json().catch(() => null);
                            if (bodyJson?.retryAfter && typeof bodyJson.retryAfter === 'number' && bodyJson.retryAfter > 0) {
                                waitSec = bodyJson.retryAfter;
                            }
                        }
                        catch { }
                    }
                    this.rateLimiter.recordRateLimit(waitSec);
                    this.logger.warn(`Storage Bridge returned HTTP 429 (Rate Limit)! Cooldown ${waitSec}s, retrying attempt ${attempt + 1}/${maxAttempts}...`, { id, waitSec });
                    if (attempt < maxAttempts) {
                        await new Promise((r) => setTimeout(r, waitSec * 1000));
                        continue;
                    }
                    throw new NoxWorkerStorageError('http', 429, `Storage Bridge rate limit exceeded (Retry-After: ${waitSec}s)`);
                }
                if (!res.ok) {
                    const errText = await res.text().catch(() => '');
                    const error = new NoxWorkerStorageError('http', res.status, `Internal storage upload failed: HTTP ${res.status} - ${errText.slice(0, 200)}`);
                    if (res.status >= 500 && attempt < maxAttempts) {
                        // Record transient error: only triggers global pacing if concentrated (>= 3 in 30s)
                        this.rateLimiter.recordTransientError();
                        // Progressive retry with jitter: 4s -> 10s -> 20s + jitter
                        const baseDelays = [4000, 10000, 20000];
                        const baseMs = baseDelays[attempt - 1] ?? 20000;
                        const jitterMs = Math.floor(Math.random() * (baseMs * 0.25));
                        const delayMs = baseMs + jitterMs;
                        this.logger.warn(`Storage upload transient error HTTP ${res.status}, local backoff ${Math.round(delayMs / 1000)}s with jitter before attempt ${attempt + 1}/${maxAttempts}...`, { id, status: res.status, delayMs });
                        await new Promise((r) => setTimeout(r, delayMs));
                        continue;
                    }
                    throw error;
                }
                const data = (await res.json().catch(() => null));
                if (!data || typeof data.providerKey !== 'string' || !data.providerKey) {
                    throw new NoxWorkerStorageError('payload', res.status, 'Invalid response payload from internal storage endpoint');
                }
                // Gradually restore rate if currently throttled
                this.rateLimiter.restoreRate();
                return data.providerKey;
            }
            catch (err) {
                lastError = err;
                if (err instanceof NoxWorkerStorageError &&
                    (err.stage === 'auth' || (err.status && err.status < 500 && err.status !== 429))) {
                    throw err;
                }
                if (attempt < maxAttempts) {
                    this.rateLimiter.recordTransientError();
                    const baseDelays = [4000, 10000, 20000];
                    const baseMs = baseDelays[attempt - 1] ?? 20000;
                    const jitterMs = Math.floor(Math.random() * (baseMs * 0.25));
                    const delayMs = baseMs + jitterMs;
                    this.logger.warn(`Storage upload attempt ${attempt}/${maxAttempts} network failure (${err?.message}), waiting ${Math.round(delayMs / 1000)}s before retrying...`, { id, delayMs });
                    await new Promise((r) => setTimeout(r, delayMs));
                    continue;
                }
            }
        }
        if (lastError instanceof NoxWorkerStorageError)
            throw lastError;
        this.logger.error('Network error during internal storage upload after retries', { id, error: lastError?.message });
        throw new NoxWorkerStorageError('network', undefined, lastError?.message);
    }
}
