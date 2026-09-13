import type { SupabaseClient } from '@supabase/supabase-js';
export type BarrierState = 'OPEN' | 'CAUTION' | 'CLOSED' | 'RECOVERING';
export interface BarrierStatus {
    state: BarrierState;
    updatedAt: string;
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
    /**
     * Checks whether historical backfill can enqueue/process bulk chapters.
     * Only allowed when state is fully OPEN.
     */
    isBackfillAllowed(): Promise<boolean>;
}
