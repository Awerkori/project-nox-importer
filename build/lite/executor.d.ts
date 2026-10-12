import type { Pool } from 'pg';
import type { StorageProvider } from '../storage/provider.js';
import type { SourceRegistry } from '../sources/registry.js';
import { LiteRepository, type UploadReceiptInput, type UploadReservationInput } from './repository.js';
import type { LiteTask } from './types.js';
import { AsyncSemaphore } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
import { PageByteBudget } from './page-byte-budget.js';
/**
 * Coalesce only post-Telegram durable receipts.  The pre-upload intent stays
 * a per-page fence, so a crash can never make a successful send retryable.
 * Five milliseconds is enough to join a page wave without adding meaningful
 * latency or a second scheduler/queue.
 */
export declare class UploadReceiptBatcher {
    private readonly queue;
    private pending;
    private flushTimer;
    private flushing;
    constructor(queue: LiteRepository);
    persist(input: UploadReceiptInput): Promise<void>;
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
    constructor(queue: LiteRepository);
    reserve(input: UploadReservationInput): Promise<{
        mediaId: string;
        state: string;
    }>;
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
    private readonly sourceSlots;
    private readonly receiptBatcher;
    private readonly reservationBatcher;
    private fairDemandSources;
    constructor(db: Pool, queue: LiteRepository, registry: SourceRegistry, storage: StorageProvider, importerUserId: string, downloads: AsyncSemaphore, perChapterPageConcurrency?: number, bufferedPages?: AsyncSemaphore, sourceRateLimiter?: HostRateLimiter, chapterWorkerCapacity?: number, onRouteSelected?: ((taskId: string, source: string) => void) | undefined, pageBytes?: PageByteBudget);
    /**
     * A bounded runtime sample of sources with ready imports. It only controls
     * local execution permits; the durable queue remains authority for task
     * existence, priority and work-level round-robin.
     */
    setFairSourceDemand(sources: Iterable<string>): void;
    getFairSourceDemand(): string[];
    private sourceCapacity;
    private hasFairAlternative;
    private sourceSlot;
    private routes;
    /**
     * Existing legacy mappings are exact source identities, unlike a numeric
     * display value. Reuse them during migration so Lite never overwrites a
     * published editorial chapter merely because its own mapping table is new.
     */
    private legacyMappedChapter;
    private mapCanonicalChapter;
    private createOrLoadChapter;
    importChapter(task: LiteTask, workerId: string): Promise<void>;
    publish(task: LiteTask, workerId: string): Promise<void>;
}
