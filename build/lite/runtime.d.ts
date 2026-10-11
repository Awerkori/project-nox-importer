import type { Pool } from 'pg';
import type { Config } from '../config.js';
import type { SourceRegistry } from '../sources/registry.js';
import type { StorageProvider } from '../storage/provider.js';
/** One process, one executor pool, one bounded reconciler. */
export declare class ImporterLiteRuntime {
    private readonly pool;
    private readonly catalogClient;
    private readonly registry;
    private readonly storage;
    private readonly config;
    private stopped;
    private timers;
    private readonly queue;
    private readonly executor;
    private readonly dedupe;
    private readonly logger;
    private readonly downloads;
    private sitePausedUntil;
    constructor(pool: Pool, catalogClient: any, registry: SourceRegistry, storage: StorageProvider, config: Config);
    start(): Promise<void>;
    stop(): void;
    private seedDiscovery;
    private worker;
    /** A small protective gate, deliberately not another scheduler. */
    private probeSharedResources;
    private handle;
    private discover;
    private catalog;
    /** Invalid/blocked covers remain absent; they never overwrite a known cover. */
    private loadCover;
}
