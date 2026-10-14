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
    private readonly bufferedPageBytes;
    private readonly claimGate;
    private readonly historicalP1BackfillGate;
    private lastMetadataEnrichmentAt;
    private lastManifestP1RepairAt;
    private readonly monitorGate;
    private readonly runtimeInstanceId;
    private readonly runtimeBootedAt;
    private readonly chapterWorkerCapacity;
    private isRuntimeLeader;
    private runtimeLeadershipTimer;
    private runtimeLeadershipRenewing;
    private leaseRenewStartedAt;
    private leaseRenewCompletedAt;
    private leaseRenewError;
    private heartbeatStartedAt;
    private heartbeatCompletedAt;
    private operationalSnapshotRunning;
    private readonly workerLiveness;
    private sitePausedUntil;
    private idleClaimUntil;
    private idleMonitorUntil;
    private lastSiteProbe;
    /**
     * A queued checkout from the importer's two-connection pool is a local,
     * transient condition, not proof that the Reader is unhealthy.  Claims are
     * already serialized by `claimGate`; freezing every chapter slot whenever
     * one write is queued turns ordinary receipt persistence into avoidable
     * idle time.  Only the independent site-health gate pauses new work.
     */
    private shouldPauseChapterAdmission;
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
    /**
     * One periodic, bounded P1 reserve refill.  It runs after reconciliation
     * and never in the claim path, so legacy mappings cannot turn into a new
     * global scheduler scan.  A healthy reserve keeps only a tiny, deferred
     * source-diverse producer set; low reserve checks retain fair P1 admission
     * so a giant work cannot hide missing manifests indefinitely.
     */
    private refillHistoricalP1Catalog;
    private runRecoveryTurn;
    /** Existing positive manifests may outlive an older completed catalog task. */
    private refillManifestProvenP1Imports;
    /**
     * Historical aliases/tags already present in a source mapping are cheap,
     * useful evidence. Feed a single P2 catalog task every five minutes; normal
     * strict admission still makes it yield completely to STAFF/P0/P1.
     */
    private refillStoredMetadataEnrichment;
    /** A bounded source view refreshes only outside claims and only once per recovery turn. */
    private refreshSourceFairness;
    private upsertChapterSources;
    /**
     * Catalog passes can rediscover a whole work that the legacy importer has
     * already published.  A source/id match plus the canonical work id is
     * sufficient evidence to reuse it; similarity or a numeric display value
     * is deliberately never used here.  Returning this map before admission
     * keeps hundreds of no-op IMPORT_CHAPTER claims out of the P1 lane.
     */
    private legacyPublishedChapterMap;
    /** Persist the exact legacy identity without rewriting any Reader chapter. */
    private upsertLegacyChapterMappings;
    private worker;
    private setWorkerLiveness;
    private setWorkerSource;
    private livenessSnapshot;
    /** One empty claim probe per idle interval, shared by every worker. */
    private claimNext;
    /**
     * Claim at most one due P0 observation. A failed/empty probe backs off for
     * fifteen seconds, so thousands of watched works do not turn into empty DB
     * claims. This is a resource budget, not a second priority scheduler.
     */
    private runMonitorTurn;
    /** A small protective gate, deliberately not another scheduler. */
    private probeSiteHealth;
    private probeSharedResources;
    /** A small diagnostic snapshot; it deliberately skips contention and scans. */
    private emitOperationalSnapshot;
    private emitOperationalSnapshotOnce;
    private handle;
    private discover;
    private catalog;
    /** A no-network P2 pass over explicit metadata saved with a known mapping. */
    private enrichStoredMappingMetadata;
    /** Release bursts get a quick recheck; quiet works exponentially back off. */
    private monitorIntervalMs;
    /** Bounded health recovery for the two recurring observation task types. */
    private recurringRetryDelayMs;
    /** Invalid/blocked covers remain absent; they never overwrite a known cover. */
    private loadCover;
}
