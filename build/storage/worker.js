import https from 'node:https';
import http from 'node:http';
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
const httpsAgent = new https.Agent({
    keepAlive: true,
    maxSockets: 64,
    maxFreeSockets: 16,
    timeout: 60_000,
    keepAliveMsecs: 30_000,
});
const httpAgent = new http.Agent({
    keepAlive: true,
    maxSockets: 64,
    maxFreeSockets: 16,
    timeout: 60_000,
    keepAliveMsecs: 30_000,
});
function doHttp1Request(urlStr, options) {
    return new Promise((resolve, reject) => {
        const parsedUrl = new URL(urlStr);
        const isHttps = parsedUrl.protocol === 'https:';
        const client = isHttps ? https : http;
        const isConnectionClose = options.headers['Connection']?.toLowerCase() === 'close' ||
            options.headers['connection']?.toLowerCase() === 'close';
        const agent = isConnectionClose ? false : (isHttps ? httpsAgent : httpAgent);
        const req = client.request(parsedUrl, {
            method: options.method,
            agent,
            headers: options.headers,
            timeout: options.timeoutMs ?? 60_000,
        }, (res) => {
            const chunks = [];
            res.on('data', (chunk) => {
                chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
            });
            res.on('end', () => {
                const bodyBuffer = Buffer.concat(chunks);
                const statusCode = res.statusCode || 500;
                resolve({
                    status: statusCode,
                    ok: statusCode >= 200 && statusCode < 300,
                    headers: res.headers,
                    text: async () => bodyBuffer.toString('utf8'),
                    json: async () => JSON.parse(bodyBuffer.toString('utf8')),
                });
            });
        });
        req.on('timeout', () => {
            req.destroy(new Error(`Request timed out after ${options.timeoutMs ?? 60_000}ms`));
        });
        req.on('error', (err) => {
            reject(err);
        });
        if (options.body && options.body.length > 0) {
            req.write(options.body);
        }
        req.end();
    });
}
export class NoxWorkerStorageProvider {
    workerBaseUrl;
    bridgeToken;
    transport;
    logger = new Logger('NoxWorkerStorage');
    rateLimiter;
    lastBotReference = 'MANGA_STORAGE_01';
    lastShardId = null;
    constructor(workerBaseUrl, bridgeToken, transport, rateLimiter) {
        this.workerBaseUrl = workerBaseUrl;
        this.bridgeToken = bridgeToken;
        this.transport = transport;
        if (!bridgeToken) {
            throw new Error('NoxWorkerStorageProvider requires valid NOX_STORAGE_BRIDGE_TOKEN');
        }
        this.rateLimiter =
            rateLimiter ||
                new GlobalStorageRateLimiter({
                    maxRequestsPerMinute: 150,
                    minRequestsPerMinute: 60,
                    safetyCeilingRate: 260,
                    minIntervalMs: 120,
                });
    }
    executeRequest(urlStr, options) {
        if (this.transport) {
            return this.transport(urlStr, {
                method: options.method,
                headers: options.headers,
                body: options.body,
                signal: AbortSignal.timeout(options.timeoutMs ?? 60_000),
            }).then(async (res) => {
                let headersObj = {};
                if (res.headers) {
                    if (typeof res.headers.forEach === 'function') {
                        res.headers.forEach((val, key) => {
                            headersObj[key] = val;
                        });
                    }
                    else if (typeof res.headers.entries === 'function') {
                        headersObj = Object.fromEntries(res.headers.entries());
                    }
                    else {
                        headersObj = res.headers;
                    }
                }
                return {
                    status: res.status ?? (res.ok ? 200 : 500),
                    ok: Boolean(res.ok),
                    headers: headersObj,
                    text: () => typeof res.text === 'function' ? res.text() : Promise.resolve(''),
                    json: () => typeof res.json === 'function' ? res.json() : Promise.resolve({}),
                };
            });
        }
        return doHttp1Request(urlStr, options);
    }
    getRateLimiter() {
        return this.rateLimiter;
    }
    getLastBotReference() {
        return this.lastBotReference;
    }
    getLastShardId() {
        return this.lastShardId;
    }
    getProviderKey() {
        // Media provider key in database is 'telegram' so manga reader streams it correctly
        return 'telegram';
    }
    async healthCheck() {
        try {
            const url = `${this.workerBaseUrl.replace(/\/$/, '')}/api/internal/storage/upload`;
            const res = await this.executeRequest(url, {
                method: 'GET',
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 ProjectNox-Importer/1.0',
                    Authorization: `Bearer ${this.bridgeToken}`,
                    Accept: 'application/json',
                },
                timeoutMs: 15_000,
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
    async upload(bytes, mime, id, chapterId) {
        const queryParts = [`id=${encodeURIComponent(id)}`];
        if (chapterId) {
            queryParts.push(`chapter_id=${encodeURIComponent(chapterId)}`);
        }
        const url = `${this.workerBaseUrl.replace(/\/$/, '')}/api/internal/storage/upload?${queryParts.join('&')}`;
        let lastError;
        const maxAttempts = 3;
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                // Enforce global rate limit across all sources (Token Bucket + Sliding Window cap at 120-160 req/min)
                await this.rateLimiter.acquire();
                const bodyBuffer = Buffer.isBuffer(bytes)
                    ? bytes
                    : Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
                const startTime = Date.now();
                const res = await this.executeRequest(url, {
                    method: 'POST',
                    headers: {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 ProjectNox-Importer/1.0',
                        Authorization: `Bearer ${this.bridgeToken}`,
                        'Content-Type': mime || 'application/octet-stream',
                        'Content-Length': String(bodyBuffer.length),
                        Connection: 'close',
                    },
                    body: bodyBuffer,
                    timeoutMs: 60_000,
                });
                if (res.status === 401 || res.status === 403) {
                    throw new NoxWorkerStorageError('auth', res.status, 'Authentication failed on internal storage endpoint');
                }
                if (res.status === 429) {
                    let waitSec = 15;
                    const retryHeader = res.headers['retry-after'];
                    if (retryHeader) {
                        const parsed = parseInt(Array.isArray(retryHeader) ? retryHeader[0] : retryHeader, 10);
                        if (!isNaN(parsed) && parsed > 0)
                            waitSec = parsed;
                    }
                    else {
                        try {
                            const bodyJson = await res.json().catch(() => null);
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
                        // Record transient error metric
                        this.rateLimiter.recordTransientError();
                        // Progressive per-request retry with jitter: 1s -> 2s -> 4s + jitter
                        const baseDelays = [1000, 2000, 4000];
                        const baseMs = baseDelays[attempt - 1] ?? 4000;
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
                if (data.botReference) {
                    this.lastBotReference = data.botReference;
                }
                if (data.shardId) {
                    this.lastShardId = data.shardId;
                }
                // Record throughput metrics for AIMD upward probe
                const durationMs = Date.now() - startTime;
                this.rateLimiter.recordSuccess(bodyBuffer.length, durationMs);
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
                    const baseDelays = [1000, 2000, 4000];
                    const baseMs = baseDelays[attempt - 1] ?? 4000;
                    const jitterMs = Math.floor(Math.random() * (baseMs * 0.25));
                    const delayMs = baseMs + jitterMs;
                    this.logger.warn(`Storage upload attempt ${attempt}/${maxAttempts} network failure (${err?.message}), waiting ${Math.round(delayMs / 1000)}s before retrying...`, { id, delayMs, cause: err?.cause?.message || err?.cause });
                    await new Promise((r) => setTimeout(r, delayMs));
                    continue;
                }
            }
        }
        if (lastError instanceof NoxWorkerStorageError)
            throw lastError;
        this.logger.error('Network error during internal storage upload after retries', {
            id,
            error: lastError?.message,
            cause: lastError?.cause?.message || lastError?.cause,
        });
        throw new NoxWorkerStorageError('network', undefined, lastError?.message);
    }
}
