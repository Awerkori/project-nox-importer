import type pg from 'pg';
import type { AliasSourceId, NoxWorkAliasTarget, WorkAliasEnrichmentResult } from './types.js';
export declare function classifyWorkPriorityBucket(work: {
    title: string;
    aliases: string[];
}): number;
export declare class AliasOnlyRepository {
    private readonly pool;
    private schemaReady;
    constructor(pool: pg.Pool);
    ensureSchema(): Promise<void>;
    /**
     * Loads all published works pending for a given campaign, sorted strictly by Section 4 priority buckets.
     */
    loadPendingPublishedWorksForCampaign(campaignId: string): Promise<{
        totalPublished: number;
        alreadyCompleted: number;
        pending: Array<NoxWorkAliasTarget & {
            priorityBucket: number;
        }>;
    }>;
    /**
     * Persists a small batch of enrichment results safely:
     * - Re-reads current `works.aliases` for modified works so concurrent chapter onboarding aliases are never lost.
     * - Updates EXCLUSIVELY `works.aliases` (and `updated_at`) when new legitimate aliases exist.
     * - Saves rollback snapshots, per-source provenance, and durable checkpoints.
     */
    persistBatchResults(campaignId: string, items: Array<{
        target: NoxWorkAliasTarget;
        priorityBucket: number;
        result: WorkAliasEnrichmentResult;
    }>): Promise<{
        worksUpdated: number;
        aliasesPersisted: number;
        ptBrPersisted: number;
    }>;
    /**
     * Used by the permanent ImporterLiteRuntime background loop to pick up:
     * 1. Newly published works that have never passed through the 4 ALIAS_ONLY sources,
     * 2. Bounded transient retries (`SOURCE_ERROR` with `attempts <= 3` and `next_retry_at <= now()`),
     * 3. Lightweight 14-day periodic rechecks for zero-alias works (`next_recheck_at <= now()`).
     */
    findDueWorksForRuntimeEnrichment(limit?: number): Promise<Array<{
        target: NoxWorkAliasTarget;
        priorityBucket: number;
        onlySources?: AliasSourceId[];
        reason: 'NEW_WORK' | 'BOUNDED_RETRY' | 'PERIODIC_INCOMPLETE_RECHECK';
    }>>;
    /**
     * Reverts ONLY the `works.aliases` updates performed by a specific `campaignId`.
     */
    rollbackCampaign(campaignId: string): Promise<{
        restoredWorks: number;
    }>;
}
