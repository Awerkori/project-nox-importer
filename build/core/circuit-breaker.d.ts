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
export declare class SourceCircuitBreaker {
    private failureThreshold;
    private initialCooldownMs;
    private maxCooldownMs;
    private logger;
    private circuits;
    constructor(failureThreshold?: number, initialCooldownMs?: number, maxCooldownMs?: number);
    private getCircuit;
    canExecute(sourceId: string, host?: string): boolean;
    recordSuccess(sourceId: string, host?: string): void;
    recordFailure(sourceId: string, classification: CloudflareClassification, host?: string): {
        tripped: boolean;
        cooldownMs: number;
    };
    getState(sourceId: string, host?: string): CircuitState;
    getInfo(sourceId: string, host?: string): CircuitInfo;
    reset(sourceId: string, host?: string): void;
}
