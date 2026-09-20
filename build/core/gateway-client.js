import { Logger } from './logger.js';
export class ImporterGatewayClient {
    logger = new Logger('GatewayClient');
    baseUrl;
    bridgeToken;
    constructor(mangaUrl, bridgeToken) {
        if (!bridgeToken) {
            throw new Error('ImporterGatewayClient requires valid NOX_STORAGE_BRIDGE_TOKEN');
        }
        this.bridgeToken = bridgeToken;
        this.baseUrl = `${mangaUrl.replace(/\/$/, '')}/api/internal/importer`;
    }
    async request(endpoint, options = {}) {
        const url = `${this.baseUrl}${endpoint.startsWith('/') ? endpoint : `/${endpoint}`}`;
        const method = options.method || 'GET';
        const timeoutMs = options.timeoutMs || 20000;
        const headers = {
            Authorization: `Bearer ${this.bridgeToken}`,
            Accept: 'application/json',
            'User-Agent': 'ProjectNox-Importer-GatewayClient/1.0',
        };
        if (options.body) {
            headers['Content-Type'] = 'application/json';
        }
        const res = await fetch(url, {
            method,
            headers,
            body: options.body ? JSON.stringify(options.body) : undefined,
            signal: AbortSignal.timeout(timeoutMs),
        });
        if (res.status === 401 || res.status === 403) {
            throw new Error(`Gateway authentication failed: HTTP ${res.status}`);
        }
        const json = await res.json().catch(() => null);
        if (!res.ok || json?.success === false) {
            const errMsg = json?.error || `Gateway request to ${endpoint} failed with HTTP ${res.status}`;
            throw new Error(errMsg);
        }
        return json;
    }
    /* 1. Job Acquisition */
    async acquireJobs(options) {
        const res = await this.request('/acquire-jobs', {
            method: 'POST',
            body: options,
            timeoutMs: 15000,
        });
        return res.jobs || [];
    }
    /* 2. Heartbeat Batch */
    async heartbeat(workerId, jobs) {
        const res = await this.request('/heartbeat', {
            method: 'POST',
            body: { workerId, jobs },
            timeoutMs: 15000,
        });
        return res.updates || [];
    }
    /* 3. Fail / Retry Batch */
    async failBatch(workerId, jobs) {
        const res = await this.request('/fail-batch', {
            method: 'POST',
            body: { workerId, jobs },
            timeoutMs: 15000,
        });
        return res.updatedCount || 0;
    }
    /* 4. Atomic Publication Batch */
    async publishBatch(payload) {
        return await this.request('/publish-batch', {
            method: 'POST',
            body: payload,
            timeoutMs: 30000,
        });
    }
    /* 5. Enqueue Jobs */
    async enqueueJobs(jobs) {
        const res = await this.request('/enqueue-jobs', {
            method: 'POST',
            body: { jobs },
            timeoutMs: 15000,
        });
        return res.enqueuedCount || 0;
    }
    /* 6. Recover Stalled Leases */
    async recoverStalled() {
        const res = await this.request('/recover-stalled', {
            method: 'POST',
            timeoutMs: 15000,
        });
        return res.recoveredCount || 0;
    }
    /* 7. Sources */
    async getSources(enabledOnly = false) {
        const res = await this.request(`/sources?enabled=${enabledOnly}`, {
            method: 'GET',
            timeoutMs: 15000,
        });
        return res.sources || [];
    }
    async updateSource(sourceIdOrName, update) {
        const res = await this.request('/sources', {
            method: 'POST',
            body: { id: sourceIdOrName, ...update },
            timeoutMs: 15000,
        });
        return Boolean(res.updated);
    }
    /* 8. Checkpoints */
    async getCheckpoint(source) {
        const res = await this.request(`/checkpoints?source=${encodeURIComponent(source)}`, {
            method: 'GET',
            timeoutMs: 10000,
        });
        return res.checkpoint || null;
    }
    async saveCheckpoint(source, cursorValue, metadata = {}) {
        await this.request('/checkpoints', {
            method: 'POST',
            body: { source, cursorValue, metadata },
            timeoutMs: 10000,
        });
    }
    /* 9. Work Deduplication / Matching */
    async resolveWork(candidate) {
        return await this.request('/resolve-work', {
            method: 'POST',
            body: candidate,
            timeoutMs: 15000,
        });
    }
    /* 10. Safety Barrier */
    async getSafetyBarrier() {
        const res = await this.request('/safety-barrier', {
            method: 'GET',
            timeoutMs: 10000,
        });
        return res.state || 'CLOSED';
    }
    async setSafetyBarrier(state) {
        await this.request('/safety-barrier', {
            method: 'POST',
            body: { state },
            timeoutMs: 10000,
        });
    }
    /* 11. Reconcile */
    async getReconcileWork(workId) {
        return await this.request(`/reconcile?workId=${encodeURIComponent(workId)}`, {
            method: 'GET',
            timeoutMs: 15000,
        });
    }
    async saveWorkHealth(healthData) {
        await this.request('/reconcile', {
            method: 'POST',
            body: healthData,
            timeoutMs: 15000,
        });
    }
    /* 12. Stats */
    async getStats() {
        const res = await this.request('/stats', {
            method: 'GET',
            timeoutMs: 15000,
        });
        return res.stats;
    }
    /* 13. Raw Parameterized SQL Gateway */
    async sql(query, params = []) {
        const res = await this.request('/sql', {
            method: 'POST',
            body: { query, params },
            timeoutMs: 30000,
        });
        return { rows: res.rows || [], rowCount: res.rowCount ?? (res.rows ? res.rows.length : 0) };
    }
    /* 14. Batch Transaction SQL Gateway */
    async batchSql(queries) {
        const res = await this.request('/sql', {
            method: 'POST',
            body: { queries: queries.map(q => ({ query: q.text, params: q.params })) },
            timeoutMs: 45000,
        });
        return res.results || [];
    }
}
