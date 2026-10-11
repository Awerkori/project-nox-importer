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
    private createOrLoadChapter;
    private recordSourceFailure;
    private hasPersistedUpload;
    importChapter(task: LiteTask, workerId: string): Promise<void>;
    publish(task: LiteTask, workerId: string): Promise<void>;
}
