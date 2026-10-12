import type { Pool } from 'pg';
import type { StorageProvider } from '../storage/provider.js';
import type { SourceRegistry } from '../sources/registry.js';
import { LiteRepository, type UploadReceiptInput } from './repository.js';
import type { LiteTask } from './types.js';
import { AsyncSemaphore } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
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
    private readonly sourceSlots;
    private readonly receiptBatcher;
    constructor(db: Pool, queue: LiteRepository, registry: SourceRegistry, storage: StorageProvider, importerUserId: string, downloads: AsyncSemaphore, perChapterPageConcurrency?: number, bufferedPages?: AsyncSemaphore, sourceRateLimiter?: HostRateLimiter);
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
