import { Logger } from './logger.js';
export class SourceCircuitBreaker {
    failureThreshold;
    initialCooldownMs;
    maxCooldownMs;
    logger = new Logger('CircuitBreaker');
    circuits = new Map();
    constructor(failureThreshold = 3, initialCooldownMs = 5 * 60_000, maxCooldownMs = 60 * 60_000) {
        this.failureThreshold = failureThreshold;
        this.initialCooldownMs = initialCooldownMs;
        this.maxCooldownMs = maxCooldownMs;
    }
    getCircuit(key) {
        let c = this.circuits.get(key);
        if (!c) {
            c = {
                state: 'CLOSED',
                consecutiveFailures: 0,
                lastFailureAt: null,
                lastSuccessAt: null,
                cooldownUntil: null,
                lastClassification: null,
                probeAttempts: 0,
                safeRate: 2.0,
            };
            this.circuits.set(key, c);
        }
        return c;
    }
    canExecute(sourceId, host) {
        const key = host ? `${sourceId}:${host}` : sourceId;
        const c = this.getCircuit(key);
        const now = Date.now();
        if (c.state === 'OPEN') {
            if (c.cooldownUntil && now >= c.cooldownUntil) {
                c.state = 'HALF_OPEN';
                this.logger.info(`Circuit for ${key} transitioned from OPEN to HALF_OPEN (cooldown expired)`);
                return true; // Allow single test probe
            }
            return false;
        }
        return true;
    }
    recordSuccess(sourceId, host) {
        const key = host ? `${sourceId}:${host}` : sourceId;
        const c = this.getCircuit(key);
        c.consecutiveFailures = 0;
        c.lastSuccessAt = Date.now();
        c.cooldownUntil = null;
        c.probeAttempts = 0;
        if (c.state === 'HALF_OPEN' || c.state === 'DEGRADED') {
            this.logger.info(`Circuit for ${key} fully recovered to CLOSED`);
            c.state = 'CLOSED';
        }
    }
    recordFailure(sourceId, classification, host) {
        const key = host ? `${sourceId}:${host}` : sourceId;
        const c = this.getCircuit(key);
        c.consecutiveFailures++;
        c.lastFailureAt = Date.now();
        c.lastClassification = classification;
        // Calculate exponential backoff cooldown with jitter
        const exponent = Math.min(c.consecutiveFailures - 1, 5);
        const baseCooldown = Math.min(this.initialCooldownMs * Math.pow(2, exponent), this.maxCooldownMs);
        const jitter = Math.floor(Math.random() * 30_000); // 0-30s jitter
        const cooldownMs = baseCooldown + jitter;
        if (c.state === 'HALF_OPEN') {
            // Re-trip immediately
            c.state = 'OPEN';
            c.cooldownUntil = Date.now() + cooldownMs;
            this.logger.warn(`Circuit for ${key} probe failed. Re-tripping to OPEN for ${Math.round(cooldownMs / 1000)}s`);
            return { tripped: true, cooldownMs };
        }
        if (c.consecutiveFailures >= this.failureThreshold || classification === 'DATACENTER_ASN_BLOCK') {
            c.state = 'OPEN';
            c.cooldownUntil = Date.now() + cooldownMs;
            this.logger.warn(`Circuit for ${key} tripped to OPEN (${c.consecutiveFailures} failures, cause: ${classification}). Cooldown: ${Math.round(cooldownMs / 1000)}s`);
            return { tripped: true, cooldownMs };
        }
        c.state = 'DEGRADED';
        return { tripped: false, cooldownMs: 0 };
    }
    getState(sourceId, host) {
        const key = host ? `${sourceId}:${host}` : sourceId;
        const c = this.getCircuit(key);
        if (c.state === 'OPEN' && c.cooldownUntil && Date.now() >= c.cooldownUntil) {
            return 'HALF_OPEN';
        }
        return c.state;
    }
    getInfo(sourceId, host) {
        const key = host ? `${sourceId}:${host}` : sourceId;
        return this.getCircuit(key);
    }
    reset(sourceId, host) {
        const key = host ? `${sourceId}:${host}` : sourceId;
        const c = this.getCircuit(key);
        c.state = 'CLOSED';
        c.consecutiveFailures = 0;
        c.cooldownUntil = null;
        c.probeAttempts = 0;
    }
}
