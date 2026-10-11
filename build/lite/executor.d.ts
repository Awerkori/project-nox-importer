import type { Pool } from 'pg';
import type { StorageProvider } from '../storage/provider.js';
import type { SourceRegistry } from '../sources/registry.js';
import { LiteRepository } from './repository.js';
import type { LiteTask } from './types.js';
import { AsyncSemaphore } from '../core/concurrency.js';
export declare class LiteExecutor {
    private readonly db;
    private readonly queue;
    private readonly registry;
    private readonly storage;
    private readonly importerUserId;
    private readonly downloads;
    private readonly perChapterPageConcurrency;
    constructor(db: Pool, queue: LiteRepository, registry: SourceRegistry, storage: StorageProvider, importerUserId: string, downloads: AsyncSemaphore, perChapterPageConcurrency?: number);
    private routes;
    private createOrLoadChapter;
    private recordSourceFailure;
    importChapter(task: LiteTask, workerId: string): Promise<void>;
    publish(task: LiteTask, workerId: string): Promise<void>;
}
