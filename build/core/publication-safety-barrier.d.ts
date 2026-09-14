import type { SupabaseClient } from '@supabase/supabase-js';
export type BarrierState = 'OPEN' | 'CAUTION' | 'CLOSED' | 'RECOVERING';
export interface BarrierStatus {
    state: BarrierState;
    updatedAt: string;
    reason?: string;
}
export interface StallMetrics {
    publicationThroughput: number;
    readyBacklog: number;
    producerActive: boolean;
}
export interface StallEvaluation {
    isStalled: boolean;
    publicationThroughput: number;
    readyBacklog: number;
    producerActive: boolean;
    currentState: BarrierState;
    nextState: BarrierState;
    action: 'NONE' | 'AUTO_CLOSE' | 'AUTO_RECOVER' | 'AUTO_OPEN';
    reason?: string;
}
export declare class PublicationSafetyBarrier {
    private supabase;
    private logger;
    private cachedState;
    private lastFetchMs;
    private cacheTtlMs;
    constructor(supabase: SupabaseClient);
    /**
     * Returns current safety barrier state from database with short caching.
     */
    getState(forceFresh?: boolean): Promise<BarrierState>;
    /**
     * Sets safety barrier state in public.settings.
     */
    setState(newState: BarrierState, reason?: string): Promise<void>;
    /**
     * Checks whether worker slots should acquire IMPORT_CHAPTER jobs.
     * If CLOSED or RECOVERING, returns false so 0 worker slots and 0 semaphores are held.
     */
    canAcquireChapters(): Promise<boolean>;
    canProcessChapter(workId?: string, sortKey?: number): Promise<boolean>;
    /**
     * Checks whether historical backfill can enqueue/process bulk chapters.
     * Only allowed when state is fully OPEN.
     */
    isBackfillAllowed(): Promise<boolean>;
    /**
     * Evaluates stall condition deterministically from metrics and current barrier state.
     * Enforces transition rules:
     * 1. OPEN / CAUTION -> CLOSED when publication throughput = 0, ready backlog > 0, and producer active.
     * 2. CLOSED -> RECOVERING when publisher is restored (throughput > 0 or drain active).
     * 3. RECOVERING -> OPEN when ready backlog is drained and sequence restored.
     * Backlog is NEVER dumped or deleted in any transition.
     */
    evaluatePublicationStall(metrics: StallMetrics, currentState: BarrierState): StallEvaluation;
    /**
     * Queries live database metrics and executes the stall evaluation.
     * Applies state changes automatically if needed.
     */
    checkAndEnforceStallDetector(injectedMetrics?: StallMetrics): Promise<StallEvaluation>;
}
