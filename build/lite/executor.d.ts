import type { Pool } from 'pg';
import type { StorageProvider } from '../storage/provider.js';
import type { SourceRegistry } from '../sources/registry.js';
import { LiteRepository } from './repository.js';
import type { LiteTask } from './types.js';
import { AsyncSemaphore } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
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
