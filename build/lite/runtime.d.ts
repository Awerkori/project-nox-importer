import type { Pool } from 'pg';
import type { Config } from '../config.js';
import type { SourceRegistry } from '../sources/registry.js';
import type { StorageProvider } from '../storage/provider.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
/** One process, one executor pool, one bounded reconciler. */
export declare class ImporterLiteRuntime {
    private readonly pool;
    private readonly catalogClient;
    private readonly registry;
    private readonly storage;
    private readonly config;
    private readonly sourceRateLimiter;
    private stopped;
    private timers;
    private readonly queue;
    private readonly executor;
    private readonly dedupe;
    private readonly logger;
    private readonly downloads;
    private readonly bufferedPages;
    private readonly claimGate;
    private readonly monitorGate;
    private readonly runtimeInstanceId;
    private readonly runtimeBootedAt;
    private isRuntimeLeader;
    private runtimeLeadershipTimer;
    private runtimeLeadershipRenewing;
    private sitePausedUntil;
    private idleClaimUntil;
    private idleMonitorUntil;
    private lastSiteProbe;
    constructor(pool: Pool, catalogClient: any, registry: SourceRegistry, storage: StorageProvider, config: Config, sourceRateLimiter?: HostRateLimiter);
    start(): Promise<void>;
    private assertReaderCanonicalIdentitySchema;
    stop(): void;
    /**
     * The shared settings lease is the only cross-runtime coordination point.
     * It gates admission only: workers which already hold a task lease may
     * finish their durable write while a new STAFF request or a successor
     * runtime waits to claim new work.
     */
    private tryAcquireRuntimeLeadership;
    private startRuntimeLeadershipRenewal;
    private waitForRuntimeLeadership;
    private releaseRuntimeLeadership;
    private seedDiscovery;
    private upsertChapterSources;
    private worker;
    /** One empty claim probe per idle interval, shared by every worker. */
    private claimNext;
    /**
     * Claim at most one due P0 observation. A failed/empty probe backs off for
     * fifteen seconds, so thousands of watched works do not turn into empty DB
     * claims. This is a resource budget, not a second priority scheduler.
     */
    private runMonitorTurn;
    /** A small protective gate, deliberately not another scheduler. */
    private probeSharedResources;
    /** A small diagnostic snapshot; it deliberately skips contention and scans. */
    private emitOperationalSnapshot;
    private handle;
    private discover;
    private catalog;
    /** Release bursts get a quick recheck; quiet works exponentially back off. */
    private monitorIntervalMs;
    /** Bounded health recovery for the two recurring observation task types. */
    private recurringRetryDelayMs;
    /** Invalid/blocked covers remain absent; they never overwrite a known cover. */
    private loadCover;
}
