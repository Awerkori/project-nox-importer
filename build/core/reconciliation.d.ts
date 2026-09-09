import type { SupabaseClient } from '@supabase/supabase-js';
import { ImporterQueue } from './queue.js';
import { SourceRegistry } from '../sources/registry.js';
export interface ReconciliationStats {
    worksScanned: number;
    worksWithConfirmedGaps: number;
    confirmedGapsDiscovered: number;
    newChaptersDiscovered: number;
    jobsEnqueued: number;
    stagedSkipped: number;
    duplicatesAvoided: number;
}
export interface WorkReconciliationResult {
    workId: string;
    title: string;
    totalKnownChapters: number;
    totalImportedChapters: number;
    missingStart: boolean;
    firstChapterNumber: number | null;
    latestChapterNumber: number | null;
    gaps: Array<{
        from: number;
        to: number;
        type: 'MISSING_START' | 'INTERNAL_GAP';
    }>;
    unresolvedGaps: number[];
    providersSummary: Array<{
        provider: string;
        chaptersAvailable: number;
        active: boolean;
        confidenceScore?: number;
        matchMethod?: string;
    }>;
    enqueuedCount: number;
    healthStatus: 'HEALTHY' | 'INCOMPLETE' | 'RECONCILING' | 'UNVERIFIED' | 'BLOCKED';
}
export declare class ExistingWorksReconciler {
    private supabase;
    private queue;
    private registry;
    private logger;
    private lastReconciliationAt;
    constructor(supabase: SupabaseClient, queue: ImporterQueue, registry: SourceRegistry);
    private isSourceOperationallyAvailable;
    /**
     * Discovers alternative provider mappings for a given work by searching across
     * all registered source adapters and matching candidate titles.
     */
    discoverCrossProviderMappings(work: {
        id: string;
        title: string;
        slug: string;
        aliases?: string[];
        kind?: string;
    }): Promise<number>;
    /**
     * Reconciles a single work end-to-end:
     * 1. Discovers cross-provider mappings across all sources.
     * 2. Fetches chapters from all mapped sources concurrently.
     * 3. Merges into a canonical chapter manifest.
     * 4. Detects missing beginning and internal gaps.
     * 5. Enqueues missing chapters using the best available provider with fallback sources.
     * 6. Marks true missing chapters without any provider as UNRESOLVED_GAP.
     * 7. Updates `importer_work_health` and `importer_chapter_manifest`.
     */
    reconcileWorkManifest(workId: string, options?: {
        forceEnqueue?: boolean;
        priority?: number;
    }): Promise<WorkReconciliationResult>;
    /**
     * Reconciles existing works in batches, respecting rate limits and avoiding redundant work.
     */
    reconcileExistingWorks(batchSize?: number): Promise<ReconciliationStats>;
}
