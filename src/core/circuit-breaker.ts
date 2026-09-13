import { Logger } from './logger.js';
import { CloudflareClassification } from './cloudflare-classifier.js';

export type CircuitState = 'CLOSED' | 'DEGRADED' | 'OPEN' | 'HALF_OPEN';

export interface CircuitInfo {
  state: CircuitState;
  consecutiveFailures: number;
  lastFailureAt: number | null;
  lastSuccessAt: number | null;
  cooldownUntil: number | null;
  lastClassification: CloudflareClassification | null;
  probeAttempts: number;
  safeRate: number;
}

export class SourceCircuitBreaker {
  private logger = new Logger('CircuitBreaker');
  private circuits = new Map<string, CircuitInfo>();

  constructor(
    private failureThreshold: number = 3,
    private initialCooldownMs: number = 5 * 60_000,
    private maxCooldownMs: number = 60 * 60_000
  ) {}

  private getCircuit(key: string): CircuitInfo {
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

  public canExecute(sourceId: string, host?: string): boolean {
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

  public recordSuccess(sourceId: string, host?: string): void {
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

  public recordFailure(
    sourceId: string,
    classification: CloudflareClassification,
    host?: string
  ): { tripped: boolean; cooldownMs: number } {
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

  public getState(sourceId: string, host?: string): CircuitState {
    const key = host ? `${sourceId}:${host}` : sourceId;
    const c = this.getCircuit(key);
    if (c.state === 'OPEN' && c.cooldownUntil && Date.now() >= c.cooldownUntil) {
      return 'HALF_OPEN';
    }
    return c.state;
  }

  public getInfo(sourceId: string, host?: string): CircuitInfo {
    const key = host ? `${sourceId}:${host}` : sourceId;
    return this.getCircuit(key);
  }

  public reset(sourceId: string, host?: string): void {
    const key = host ? `${sourceId}:${host}` : sourceId;
    const c = this.getCircuit(key);
    c.state = 'CLOSED';
    c.consecutiveFailures = 0;
    c.cooldownUntil = null;
    c.probeAttempts = 0;
  }
}
