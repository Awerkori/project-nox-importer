import { performance } from 'node:perf_hooks';
class LruCache {
    maxEntries;
    ttlMs;
    map = new Map();
    constructor(maxEntries, ttlMs) {
        this.maxEntries = maxEntries;
        this.ttlMs = ttlMs;
    }
    get(key) {
        const entry = this.map.get(key);
        if (!entry)
            return undefined;
        if (Date.now() > entry.expiresAt) {
            this.map.delete(key);
            return undefined;
        }
        this.map.delete(key);
        this.map.set(key, entry);
        return entry.value;
    }
    set(key, value) {
        if (this.map.has(key))
            this.map.delete(key);
        this.map.set(key, { value, expiresAt: Date.now() + this.ttlMs });
        while (this.map.size > this.maxEntries) {
            const oldest = this.map.keys().next().value;
            if (oldest === undefined)
                break;
            this.map.delete(oldest);
        }
    }
}
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
function parseRetryAfterMs(headerValue) {
    if (!headerValue)
        return null;
    const trimmed = headerValue.trim();
    const seconds = Number(trimmed);
    if (Number.isFinite(seconds) && seconds >= 0) {
        return Math.min(Math.max(Math.ceil(seconds * 1000), 500), 60_000);
    }
    const parsedDate = Date.parse(trimmed);
    if (!Number.isNaN(parsedDate)) {
        return Math.min(Math.max(parsedDate - Date.now(), 500), 60_000);
    }
    return null;
}
/**
 * Per-source adaptive HTTP client with:
 * - Bounded concurrency semaphore
 * - Token-spacing rate limiter + automatic 429/Retry-After backoff
 * - In-flight request deduplication + LRU response cache
 * - Circuit breaker for sustained failures or explicit access restrictions
 */
export class SourceHttpClient {
    options;
    active = 0;
    waiters = [];
    nextSlotAt = 0;
    adaptiveIntervalMs;
    consecutiveFailures = 0;
    circuitOpenUntil = 0;
    circuitReason;
    requestsTotal = 0;
    cacheHits = 0;
    successTotal = 0;
    errorTotal = 0;
    rateLimitedTotal = 0;
    latencySumMs = 0;
    latencySamples = 0;
    cache;
    inflight = new Map();
    fetchFn;
    constructor(options) {
        this.options = options;
        this.adaptiveIntervalMs = options.minIntervalMs;
        this.cache = new LruCache(options.cacheMaxEntries ?? 6_000, 45 * 60_000);
        this.fetchFn = options.fetchImpl ?? globalThis.fetch;
    }
    getHealthSnapshot() {
        const now = Date.now();
        const isOpen = now < this.circuitOpenUntil;
        return {
            source: this.options.source,
            requestsTotal: this.requestsTotal,
            cacheHits: this.cacheHits,
            successTotal: this.successTotal,
            errorTotal: this.errorTotal,
            rateLimitedTotal: this.rateLimitedTotal,
            circuitOpen: isOpen,
            circuitReason: isOpen ? this.circuitReason : undefined,
            circuitOpenUntil: isOpen ? this.circuitOpenUntil : undefined,
            avgLatencyMs: this.latencySamples > 0 ? Math.round(this.latencySumMs / this.latencySamples) : 0,
        };
    }
    resetCircuit() {
        this.circuitOpenUntil = 0;
        this.circuitReason = undefined;
        this.consecutiveFailures = 0;
        this.adaptiveIntervalMs = this.options.minIntervalMs;
    }
    async acquireSlot() {
        if (this.active >= this.options.maxConcurrent) {
            await new Promise((resolve) => this.waiters.push(resolve));
        }
        this.active++;
        const now = Date.now();
        const scheduled = Math.max(now, this.nextSlotAt);
        this.nextSlotAt = scheduled + this.adaptiveIntervalMs;
        const waitMs = scheduled - now;
        if (waitMs > 0) {
            await sleep(waitMs);
        }
    }
    releaseSlot() {
        this.active = Math.max(0, this.active - 1);
        const next = this.waiters.shift();
        if (next)
            next();
    }
    async request(url, init = {}) {
        const key = init.cacheKey ?? `${init.parseAs || 'json'}:${url}`;
        const cached = this.cache.get(key);
        if (cached) {
            this.cacheHits++;
            return { ...cached, latencyMs: 0, fromCache: true };
        }
        const existingInflight = this.inflight.get(key);
        if (existingInflight) {
            const shared = await existingInflight;
            return { ...shared, fromCache: true };
        }
        if (!init.bypassCircuit && Date.now() < this.circuitOpenUntil) {
            return {
                ok: false,
                status: 503,
                errorCode: this.circuitReason || 'CIRCUIT_OPEN',
                errorMessage: `Circuit open for ${this.options.source}: ${this.circuitReason || 'temporary unavailability'}`,
                latencyMs: 0,
            };
        }
        const promise = this.executeWithRetries(url, init);
        this.inflight.set(key, promise);
        try {
            const response = await promise;
            if (response.ok || response.status === 404) {
                this.cache.set(key, response);
            }
            return response;
        }
        finally {
            this.inflight.delete(key);
        }
    }
    async executeWithRetries(url, init) {
        const parseMode = init.parseAs || 'json';
        const maxAttempts = Math.max(1, this.options.maxRetries + 1);
        let lastError = null;
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            if (Date.now() < this.circuitOpenUntil) {
                return {
                    ok: false,
                    status: 503,
                    errorCode: this.circuitReason || 'CIRCUIT_OPEN',
                    errorMessage: `Circuit open for ${this.options.source}: ${this.circuitReason}`,
                    latencyMs: 0,
                };
            }
            await this.acquireSlot();
            let slotReleased = false;
            const releaseOnce = () => {
                if (!slotReleased) {
                    slotReleased = true;
                    this.releaseSlot();
                }
            };
            const started = performance.now();
            this.requestsTotal++;
            try {
                const controller = new AbortController();
                const timer = setTimeout(() => controller.abort(), this.options.timeoutMs);
                let res;
                let rawText = '';
                try {
                    res = await this.fetchFn(url, {
                        method: 'GET',
                        headers: {
                            'User-Agent': 'ProjectNoxAliasEnricher/1.0 (+https://manga.project-nox-awerkori.workers.dev)',
                            ...init.headers,
                        },
                        signal: controller.signal,
                    });
                    rawText = await res.text();
                }
                finally {
                    clearTimeout(timer);
                }
                const latencyMs = Math.round(performance.now() - started);
                this.latencySumMs += latencyMs;
                this.latencySamples++;
                if (res.status === 429) {
                    this.rateLimitedTotal++;
                    const retryAfterMs = parseRetryAfterMs(res.headers.get('retry-after'))
                        ?? parseRetryAfterMs(res.headers.get('x-ratelimit-retry-after'))
                        ?? Math.min(1_500 * attempt, 10_000);
                    // Adaptively slow down future requests on this source
                    this.adaptiveIntervalMs = Math.min(Math.max(this.adaptiveIntervalMs * 1.5, 250), 2_500);
                    this.nextSlotAt = Math.max(this.nextSlotAt, Date.now() + retryAfterMs);
                    lastError = {
                        ok: false,
                        status: 429,
                        errorCode: 'HTTP_429_RATE_LIMITED',
                        errorMessage: `HTTP 429 rate limited (retry-after ${retryAfterMs}ms)`,
                        latencyMs,
                    };
                    if (attempt < maxAttempts) {
                        releaseOnce();
                        await sleep(retryAfterMs);
                        continue;
                    }
                    this.errorTotal++;
                    return lastError;
                }
                if (this.options.accessRestrictionDetector) {
                    const restriction = this.options.accessRestrictionDetector(res.status, rawText);
                    if (restriction) {
                        this.errorTotal++;
                        this.consecutiveFailures = this.options.circuitBreakerThreshold;
                        this.circuitReason = restriction;
                        this.circuitOpenUntil = Date.now() + this.options.circuitBreakerCooldownMs;
                        return {
                            ok: false,
                            status: res.status,
                            rawText,
                            errorCode: restriction,
                            errorMessage: `Access restriction on ${this.options.source} (HTTP ${res.status}): ${restriction}`,
                            latencyMs,
                        };
                    }
                }
                if (res.status === 404) {
                    this.consecutiveFailures = 0;
                    this.successTotal++;
                    return {
                        ok: false,
                        status: 404,
                        errorCode: 'HTTP_404_NOT_FOUND',
                        errorMessage: 'HTTP 404 Not Found',
                        latencyMs,
                    };
                }
                if (!res.ok) {
                    const retryable = res.status >= 500 || res.status === 408;
                    this.consecutiveFailures++;
                    if (this.consecutiveFailures >= this.options.circuitBreakerThreshold) {
                        this.circuitReason = `HTTP_${res.status}_CONSECUTIVE_FAILURES`;
                        this.circuitOpenUntil = Date.now() + this.options.circuitBreakerCooldownMs;
                    }
                    lastError = {
                        ok: false,
                        status: res.status,
                        rawText: rawText.slice(0, 400),
                        errorCode: `HTTP_${res.status}`,
                        errorMessage: `HTTP ${res.status}`,
                        latencyMs,
                    };
                    if (retryable && attempt < maxAttempts) {
                        releaseOnce();
                        await sleep(Math.min(600 * attempt, 3_000));
                        continue;
                    }
                    this.errorTotal++;
                    return lastError;
                }
                // Gradually relax adaptive interval back toward baseline on sustained 200 OK
                this.consecutiveFailures = 0;
                if (this.adaptiveIntervalMs > this.options.minIntervalMs) {
                    this.adaptiveIntervalMs = Math.max(this.options.minIntervalMs, Math.floor(this.adaptiveIntervalMs * 0.92));
                }
                if (parseMode === 'text') {
                    this.successTotal++;
                    return {
                        ok: true,
                        status: res.status,
                        data: rawText,
                        rawText,
                        latencyMs,
                    };
                }
                try {
                    const parsed = JSON.parse(rawText);
                    this.successTotal++;
                    return {
                        ok: true,
                        status: res.status,
                        data: parsed,
                        latencyMs,
                    };
                }
                catch (parseErr) {
                    this.errorTotal++;
                    this.consecutiveFailures++;
                    if (this.consecutiveFailures >= this.options.circuitBreakerThreshold) {
                        this.circuitReason = 'INVALID_JSON_RESPONSE';
                        this.circuitOpenUntil = Date.now() + this.options.circuitBreakerCooldownMs;
                    }
                    return {
                        ok: false,
                        status: res.status,
                        rawText: rawText.slice(0, 400),
                        errorCode: 'INVALID_JSON_RESPONSE',
                        errorMessage: `Invalid JSON from ${this.options.source}: ${parseErr?.message || 'parse error'}`,
                        latencyMs,
                    };
                }
            }
            catch (err) {
                const latencyMs = Math.round(performance.now() - started);
                this.latencySumMs += latencyMs;
                this.latencySamples++;
                const isAbort = err?.name === 'AbortError' || /abort|timeout/i.test(String(err?.message || ''));
                this.consecutiveFailures++;
                if (this.consecutiveFailures >= this.options.circuitBreakerThreshold) {
                    this.circuitReason = isAbort ? 'SOURCE_TIMEOUT_CIRCUIT' : 'NETWORK_ERROR_CIRCUIT';
                    this.circuitOpenUntil = Date.now() + this.options.circuitBreakerCooldownMs;
                }
                lastError = {
                    ok: false,
                    status: 0,
                    errorCode: isAbort ? 'REQUEST_TIMEOUT' : 'NETWORK_ERROR',
                    errorMessage: err?.message || (isAbort ? 'Request timed out' : 'Network error'),
                    latencyMs,
                };
                if (attempt < maxAttempts) {
                    releaseOnce();
                    await sleep(Math.min(500 * attempt, 2_500));
                    continue;
                }
                this.errorTotal++;
                return lastError;
            }
            finally {
                releaseOnce();
            }
        }
        return lastError || {
            ok: false,
            status: 0,
            errorCode: 'UNKNOWN_ERROR',
            errorMessage: 'Request failed',
            latencyMs: 0,
        };
    }
}
