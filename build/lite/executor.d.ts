import type { Pool } from 'pg';
import type { StorageProvider } from '../storage/provider.js';
import type { SourceRegistry } from '../sources/registry.js';
import { LiteRepository, type UploadReceiptInput, type UploadReservationInput } from './repository.js';
import type { LiteTask } from './types.js';
import { AsyncSemaphore } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
import { PageByteBudget } from './page-byte-budget.js';
import type { SourcePerformanceSnapshot, SourcePerformanceTracker } from './source-performance.js';
export declare const KNOWN_PLACEHOLDER_HASHES: Set<string>;
export declare const DURABLE_RESERVATION_BATCH_JOIN_MS = 350;
export declare const DURABLE_RECEIPT_BATCH_JOIN_MS = 900;
export declare const UPLOAD_BATCH_FLUSH_THRESHOLD = 24;
export type UploadBatchSnapshot = {
    sampledFlushes: number;
    totalFlushes: number;
    totalItems: number;
    totalWrites: number;
    failedFlushes: number;
    batchSize: {
        avg: number;
        p50: number;
        p95: number;
        max: number;
    };
    flushDurationMs: {
        avg: number;
        p50: number;
        p95: number;
        max: number;
    };
};
export type SourceFairnessBurstSnapshot = {
    /** Number of bounded re-samples requested before a singleton source burst. */
    refreshes: number;
    /** Source count in the last successfully installed fair-demand sample. */
    demandSources: number;
    /** True only while the one bounded re-sample is using the shared YSQL pool. */
    refreshInFlight: boolean;
};
/**
 * Coalesce only post-Telegram durable receipts.  The pre-upload intent stays
 * a per-page fence, so a crash can never make a successful send retryable.
 * A bounded join is short relative to the measured durable write and source
 * latencies, while allowing staggered pages in one bounded wave to share a
 * write. It adds no queue, state, or retry path and a full batch still flushes
 * immediately.
 */
export declare class UploadReceiptBatcher {
    private readonly queue;
    private pending;
    private flushTimer;
    private flushing;
    private readonly stats;
    constructor(queue: LiteRepository);
    persist(input: UploadReceiptInput): Promise<void>;
    snapshot(): UploadBatchSnapshot;
    private flush;
}
/**
 * Coalesce only the durable pre-upload intents.  The batch is committed before
 * any caller receives its media id, so it preserves the crash fence that
 * prevents ambiguous Telegram sends from being replayed.
 */
export declare class UploadReservationBatcher {
    private readonly queue;
    private pending;
    private flushTimer;
    private flushing;
    private readonly stats;
    constructor(queue: LiteRepository);
    reserve(input: UploadReservationInput): Promise<{
        mediaId: string;
        state: string;
    }>;
    snapshot(): UploadBatchSnapshot;
    private flush;
}
export declare class LiteExecutor {
    private readonly db;
    private readonly queue;
    private readonly registry;
    private readonly storage;
    private readonly importerUserId;
    private readonly downloads;
    private readonly perChapterPageConcurrency;
    private readonly bufferedPages;
    private readonly sourceRateLimiter;
    private readonly chapterWorkerCapacity;
    private readonly onRouteSelected?;
    private readonly pageBytes;
    private readonly chapterManifestTimeoutMs;
    private readonly refreshFairDemandOnSourceBurst?;
    private readonly pageAdmissionTimeoutMs;
    private readonly pageTransferTimeoutMs;
    private readonly sourceSlots;
    private readonly prioritySourceWaitGate;
    private readonly receiptBatcher;
    private readonly reservationBatcher;
    private fairDemandSources;
    private fairDemandCounts;
    private sourceBurstRefresh;
    private lastSourceBurstRefreshAt;
    private sourceBurstRefreshes;
    private effectiveChapterAdmissionCap;
    private performanceTracker;
    private readonly slowBackgroundGate;
    constructor(db: Pool, queue: LiteRepository, registry: SourceRegistry, storage: StorageProvider, importerUserId: string, downloads: AsyncSemaphore, perChapterPageConcurrency?: number, bufferedPages?: AsyncSemaphore, sourceRateLimiter?: HostRateLimiter, chapterWorkerCapacity?: number, onRouteSelected?: ((taskId: string, source: string) => void) | undefined, pageBytes?: PageByteBudget, chapterManifestTimeoutMs?: number, refreshFairDemandOnSourceBurst?: (() => Promise<void>) | undefined, pageAdmissionTimeoutMs?: number, pageTransferTimeoutMs?: number);
    setPerformanceTracker(tracker: SourcePerformanceTracker | null): void;
    getSourcePerformanceSnapshot(): (SourcePerformanceSnapshot & {
        slowBackgroundPool: {
            active: number;
            capacity: number;
            waiters: number;
        };
    }) | null;
    private recomputeSourceCapacities;
    /**
     * A bounded runtime sample of sources with ready imports. It only controls
     * local execution permits; the durable queue remains authority for task
     * existence, priority and work-level round-robin.
     */
    setFairSourceDemand(sources: Iterable<string>): void;
    /**
     * Use idle chapter capacity when a fair source has fewer executable works
     * than its nominal equal share.  A source can never exceed its own measured
     * limit (or explicit fair maximum), and every ready source still receives a
     * first seat before demand-weighted seats are distributed.
     */
    setFairSourceDemandCounts(demand: ReadonlyMap<string, number>): void;
    mergeFairSourceDemandCounts(counts: Record<string, number>): void;
    /** Keep local source shares aligned with the runtime's durable chapter cap. */
    setEffectiveChapterAdmissionCap(capacity: number): void;
    getFairSourceDemand(): string[];
    /** Compact evidence that an initially-singleton source was re-sampled. */
    getSourceFairnessBurstSnapshot(): SourceFairnessBurstSnapshot;
    /** The durable chapter-admission cap currently used to calculate source shares. */
    getEffectiveChapterAdmissionCap(): number;
    /** Aggregate-only batch evidence for the existing heartbeat. */
    getUploadBatchSnapshot(): {
        reservations: UploadBatchSnapshot;
        receipts: UploadBatchSnapshot;
    };
    private hasPrimaryAlternative;
    private computeSlowPoolBudget;
    private sourceCapacity;
    private demandAwareSourceCapacity;
    /**
     * Page waves remain source-scoped even when the runtime permits a wider
     * maximum.  Unknown sources retain the established six-page default; only
     * a measured source may opt into a larger wave. The shared page, byte and
     * upload fences remain the hard aggregate authority.
     */
    /**
     * The exact page-wave limit used for a source.  This is intentionally
     * exposed to the runtime heartbeat: a global configuration value alone is
     * not evidence that a source-specific cap was applied.
     */
    getPageConcurrencyForSource(source: string, plannedPageCount?: number): number;
    private hasFairAlternative;
    getSaturatedFairSources(): string[];
    private sourceSlot;
    /**
     * Re-sample exactly once before a source can grow past the share it would
     * receive with four plausible alternatives.  A true singleton remains
     * unconstrained after this one check; a real alternative immediately lowers
     * the existing source semaphore and the normal durable yield path handles
     * the not-yet-started claim.  This avoids a periodic query per chapter and
     * never preempts media already in flight.
     */
    private refreshStaleSingletonDemand;
    /**
     * Preserve one STAFF/P0 source reservation when source-share fairness is
     * currently full. Normal work still yields to a healthy alternative; the
     * bounded priority waiter merely receives the next released slot and never
     * preempts an in-flight chapter or expands source/global concurrency.
     */
    private acquireSourcePermit;
    private routes;
    /**
     * Existing legacy mappings are exact source identities, unlike a numeric
     * display value. Reuse them during migration so Lite never overwrites a
     * published editorial chapter merely because its own mapping table is new.
     */
    private legacyMappedChapter;
    private mapCanonicalChapter;
    private createOrLoadChapter;
    /**
     * Coalesces the pre-media lease-fenced checkpoint write and fresh canonical
     * chapter creation into one atomic YSQL round trip (instead of four).
     */
    private prepareFreshChapterImport;
    importChapter(task: LiteTask, workerId: string, options?: {
        inlinePublish?: boolean;
    }): Promise<void>;
    publish(task: LiteTask, workerId: string): Promise<void>;
}
