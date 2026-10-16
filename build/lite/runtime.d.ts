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
    private readonly sourceMetadataPool;
    private stopped;
    private timers;
    private readonly queue;
    private readonly executor;
    private readonly dedupe;
    private readonly historicalMetadataDedupe;
    private readonly logger;
    private readonly downloads;
    private readonly bufferedPages;
    private readonly bufferedPageBytes;
    private readonly memoryPressureGuard;
    private readonly memoryPressureEnabled;
    private readonly claimGate;
    private readonly historicalP1BackfillGate;
    private readonly metadataReplayGate;
    private readonly sourceMetadataRefillGate;
    private readonly sourceMetadataGate;
    private readonly sourceMetadataPreparationGate;
    private readonly sourceMetadataLaneGate;
    private readonly sourceMetadataPersistenceGate;
    private readonly newWorkCatalogGate;
    private readonly crossSourceMetadataGate;
    private sourceMetadataPoolWaitStartedAt;
    private readonly sourceAdmissionGate;
    private lastMetadataEnrichmentAt;
    private lastMetadataReplayAt;
    private lastSourceMetadataRefillAt;
    private sourceMetadataPreferredSource;
    private sourceMetadataPreferredSourceBurst;
    private sourceMetadataProducer;
    private lastSourceRecoveryProbeAt;
    private lastUploadOutcomeRecoveryAt;
    private lastManifestP1RepairAt;
    private manifestP1CatchupActive;
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
    /**
     * Metadata is optional P2 work. Yield it before taking away an in-flight
     * chapter's page/upload permits; page-level backpressure itself makes any
     * already-started chapter drain safely.
     */
    private shouldPauseLowPriorityMetadata;
    private refreshMemoryPressure;
    constructor(pool: Pool, catalogClient: any, registry: SourceRegistry, storage: StorageProvider, config: Config, sourceRateLimiter?: HostRateLimiter, sourceMetadataPool?: Pool);
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
    /**
     * Probe one expired source cooldown per recovery turn using the same
     * six-stage production admission gate as the legacy runtime. It never
     * bypasses a challenge or rate limit: failures remain parked with a new
     * durable cooldown, while a full PASS explicitly releases the source.
     */
    private probeDueSourceRecovery;
    /** Existing positive manifests may outlive an older completed catalog task. */
    private refillManifestProvenP1Imports;
    /**
     * Historical aliases/tags already present in a source mapping are cheap,
     * useful evidence. Feed at most one P2 catalog task per second into the
     * bounded replay lane; normal chapter claims retain strict priority.
     */
    private refillStoredMetadataEnrichment;
    /** Source-backed historical metadata remains a P2 catalog task and shares
     * normal priority claims; it never steals a chapter worker or bypasses a
     * STAFF/P0/P1 reserve. At most four network-only tasks are admitted across
     * sources; YSQL preparation and persistence remain gated below.
     */
    private refillSourceMetadataEnrichment;
    private noteSourceMetadataProducer;
    /**
     * Keep at most a few consecutive selections on a proven productive source.
     * This is deliberately process-local: the durable cursor still guarantees
     * recovery and fairness after a restart, while a live runtime avoids an
     * expensive lap over exhausted providers between every useful refetch.
     */
    private nextPreferredSourceMetadataSource;
    /** Dedicated bounded workers give source refetch continuous progress without
     * consuming chapter workers or competing with P0/P1 task selection. */
    private runSourceMetadataRefresh;
    /**
     * Catalog HTTP intentionally happens outside YSQL transactions.  A first
     * catalog pass can outlive its initial lease while it checks bounded,
     * complementary sources, so retain the normal owner fence with a small
     * periodic lease renewal.  If ownership has already been lost, finalizers
     * remain fenced by the repository and the reconciler owns recovery.
     */
    private withCatalogLeaseHeartbeat;
    /**
     * This is intentionally a one-seat onboarding lane, not a second chapter
     * scheduler.  It only claims unmapped CATALOG rows produced by discovery;
     * once a work exists, every manifest chapter returns to the ordinary
     * STAFF/P0/P1/P2 queue.  The catalog path itself holds no transaction while
     * fetching the source, and memory/site/pool guardrails are shared with the
     * low-priority metadata lanes.
     */
    private runNewWorkCatalog;
    /**
     * A new work is not limited to the aliases exposed by its first source.
     * Search only operational providers, require an exact/known-alias identity
     * match twice (summary and detail), then serialize the short YSQL attach
     * phase.  Failed/slow providers are deliberately non-blocking: their
     * ordinary discovery can still attach them later, and no synthetic
     * metadata-backfill task is created here.
     */
    private enrichNewWorkFromComplementarySources;
    private hasSustainedSourceMetadataPoolPressure;
    /**
     * One local metadata merge at a time prevents P1's legitimate chapter
     * reserve from starving aliases/tags. It reads only a saved mapping and
     * runs through the ordinary durable task/lease/finalize path; no network,
     * source slot, page buffer or chapter worker is admitted here.
     */
    private runStoredMetadataReplay;
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
    /**
     * Pre-Lite Reader rows have `legacy:*` keys and `und` language because the
     * old importer did not persist an edition/language identity.  That missing
     * value is not an explicit alternate language.  For the narrow, objective
     * case below, bind it to the modern canonical key before an alternative
     * source can materialize a second public chapter:
     *
     *   pt-BR/default regular whole chapter 28 == legacy 28.0 == legacy 28.00
     *
     * Fractions, specials, non-default editions and explicitly labelled
     * languages are deliberately excluded.  This is one batched indexed lookup
     * per catalog snapshot, never a per-chapter similarity probe.
     */
    private legacyPublishedWholeNumberMap;
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
    /** Re-fetch explicit provider metadata for one existing mapping. The task is
     * P2, single-flight, host rate limited, and only calls fetchWorkDetails (no
     * manifest/page discovery). A retryable provider failure is durably marked
     * EXTERNAL_BLOCKED and retried on a bounded exponential cooldown.
     */
    private refreshMappingMetadataFromSource;
    /** A no-network P2 pass over explicit metadata saved with a known mapping. */
    private enrichStoredMappingMetadata;
    /** Release bursts get a quick recheck; quiet works exponentially back off. */
    private monitorIntervalMs;
    /** Bounded health recovery for the two recurring observation task types. */
    private recurringRetryDelayMs;
    /** Invalid/blocked covers remain absent; they never overwrite a known cover. */
    private loadCover;
}
