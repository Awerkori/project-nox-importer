import { MonitorFinishBatcher } from './monitor-finish-batcher.js';
import { createHash, randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import type { Pool } from 'pg';
import { DeduplicationEngine } from '../core/deduplication.js';
import { DirectSupabaseClient } from '../db/direct-supabase-client.js';
import { evaluateMetadataMatchEvidence, matchWorkCandidate, normalizeTitle, type MatchCandidate } from '../core/matching.js';
import type { Config } from '../config.js';
import type { SourceRegistry } from '../sources/registry.js';
import type { StorageProvider } from '../storage/provider.js';
import { canonicalChapterKey, chapterKind, chapterOrderKey } from './chapter-identity.js';
import { LiteExecutor } from './executor.js';
import { LiteRepository, type EnqueueLiteTask, type LiteSourcePolicy } from './repository.js';
import type { LitePriority, LiteTask } from './types.js';
import { Logger } from '../core/logger.js';
import { readImageBody } from '../core/bounded-body.js';
import { processAndStoreMedia } from '../storage/media.js';
import { AsyncSemaphore } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
import { telemetryCollector } from '../core/telemetry-collector.js';
import { SourceAdmissionGate } from '../core/source-admission-gate.js';
import { sanitizeSynopsis } from './metadata.js';
import { buildDiscoveryQueryCandidates, classifyExplicitSourceMetadata, extractStoredMappingMetadata, isProviderDisplayTitleAlias } from './mapping-metadata.js';
import { mergeAlternativeTitles, alternativeTitleKey } from '../core/alternative-titles.js';
import { isPtBrTitle } from '../core/ptbr-title-localization.js';
import { inspectCoverForPtBrTitle } from '../core/cover-vision-inspector.js';
import { CoverVisionQueue, type CoverCandidate } from '../core/cover-vision-queue.js';
import { PageByteBudget, LITE_PAGE_BUFFER_BUDGET_BYTES } from './page-byte-budget.js';
import { detectContainerRamMiB, LiteMemoryPressureGuard, type LiteMemoryPressureSnapshot } from './memory-pressure.js';
import { SourcePerformanceTracker } from './source-performance.js';
import {
  ALIAS_ONLY_SOURCE_IDS,
  AliasOnlyEnrichmentService,
  AliasOnlyRepository,
} from '../alias-sources/index.js';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const withTimeout = async <T>(operation: Promise<T>, timeoutMs: number, code: string): Promise<T> => {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(code)), timeoutMs);
        timer.unref();
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};

const QUERY_TOKEN_STOP_WORDS = new Set([
  'the', 'and', 'for', 'you', 'with', 'from', 'que', 'com', 'para', 'uma', 'dos', 'das', 'manga', 'manhwa', 'manhua', 'comic', 'scan'
]);

function hasQueryTokenOverlap(candidateTitle: string, candidateSlug: string, query: string): boolean {
  const normCand = `${candidateTitle} ${candidateSlug}`.toLowerCase();
  const words = query.toLowerCase().split(/[\s:,\-_./+]+/).filter((w) => w.length >= 3 && !QUERY_TOKEN_STOP_WORDS.has(w));
  return words.some((w) => normCand.includes(w));
}
const rankForSource = (source: string) => source === 'mangaflix' ? 10 : source === 'manhastro' ? 20 : 100;
// Deliberately shared with the legacy runtime.  This fences a rolling
// cutover: Lite must stand by while v1 owns the producer lease (and vice
// versa), rather than merely avoiding duplicate Lite claims.
const RUNTIME_LEADER_KEY = 'importer_runtime_leader';
const RUNTIME_LEASE_SECONDS = 45;
const RUNTIME_LEASE_RENEW_MS = 15_000;
// This is the existing operator-controlled brake used by the legacy runtime.
// Lite must honor the same durable setting during a rolling cutover: a manual
// site-incident pause must never be bypassed simply because Lite owns the
// shared producer lease.
const MANUAL_PROTECTIVE_STOP_KEY = 'importer_protective_stop';
// An operator may temporarily lower the number of in-flight chapter claims
// without changing DIScloud environment variables or restarting healthy work.
// It is intentionally a durable setting beside the manual stop, so a rollback
// or controlled ramp is observable and survives a native Git deployment.
const CHAPTER_ADMISSION_CAP_KEY = 'importer_chapter_admission_cap';
// Declared-size page bodies are filled directly into their final Uint8Array.
// Match the measured 24-download fence so five six-page chapter waves do not
// idle behind a smaller count-only gate. The independent live-body
// budget remains authoritative, and RSS pressure can cut both admissions
// before the container approaches its 512 MiB limit.
const SAFE_BUFFERED_PAGE_CAP = 120;
// Stored metadata replay is local and no-network. Keep replay admission
// responsive, but poll the source-mapping producer less often: it uses the
// same small YSQL pool as chapter work and only needs to refill a two-task
// buffer, not rescan mappings every timer tick. A completed source refresh
// explicitly asks for its replacement, so this cadence is an idle probe, not
// an artificial 15-second gap between productive refetches.
// The metadata replay remains a single, worker-independent lane, but each
// no-network merge still performs YSQL reads/writes. Production query stats
// showed a costly mapping scan; pace its refill to once per 15 seconds while
// retaining a two-task reserve and leaving chapter workers unchanged.
const STORED_METADATA_REPLAY_INTERVAL_MS = 2_000;
// A fully idle chapter pool has already been observed completing a local V4
// merge in roughly one replay turn.  Use the repository's five-second floor
// only in that state; one queued chapter operation immediately restores the
// conservative cadence below.
const STORED_METADATA_REPLAY_V4_IDLE_INTERVAL_MS = 5_000;
const STORED_METADATA_REPLAY_V4_PRESSURED_INTERVAL_MS = 20_000;
const STORED_METADATA_REPLAY_V4_EXCLUSIVE_DEFAULT_INTERVAL_MS = 2_000;
const STORED_METADATA_REPLAY_V4_EXCLUSIVE_REFRESH_MS = 30_000;
const STORED_METADATA_REPLAY_V4_EXCLUSIVE_SOURCE_WINDOWS_PER_TURN = 12;
// A v4 merge performs several local, idempotent YSQL operations.  During
// observed production contention each operation may consume the database
// timeout budget, so the normal 45-second claim lease can expire while its
// owner is still safely completing.  This affects only the single-flight,
// no-network v4 lane; it does not add concurrency or change chapter leases.
const STORED_METADATA_REPLAY_V4_LEASE_SECONDS = 120;
const STORED_METADATA_ENRICHMENT_REFILL_INTERVAL_MS = 15_000;
const SOURCE_METADATA_REFILL_INTERVAL_MS = 15_000;
// A recently productive source may own hundreds of still-unclassified
// mappings while the durable cursor is walking providers that are already
// exhausted. Reuse it for only a short burst, then fall back to the cursor so
// fairness and alternative metadata remain intact.
const SOURCE_METADATA_PREFERRED_SOURCE_BURST = 3;
// A two-source probe is intentionally cheap, but many providers can be
// exhausted while a later healthy provider still has historical debt. Advance
// through a few empty turns immediately; then return to the normal cadence so
// an entirely exhausted catalog cannot poll YSQL continuously.
const SOURCE_METADATA_EMPTY_PROBE_TURNS = 3;
// Stage 2 metadata lane experiment: two dedicated slots, polled every five
// seconds. Source adapter host limits and source cooldowns remain authoritative.
const SOURCE_METADATA_LANE_INTERVAL_MS = 5_000;
// Historical P1 catalog production can hold a primary-pool client across a
// bounded source cursor and several provider probes.  It is useful reserve
// work, but never worth extending a live publication queue on every recovery
// turn.  Under measured pool contention defer it, while this maximum gap
// keeps the producer source-fair and prevents permanent P1 starvation.
const HISTORICAL_P1_BUSY_MAX_GAP_MS = 120_000;
// A first catalog pass creates the canonical work, cover and its explicit
// metadata.  It must not be stranded behind an indefinitely non-empty P1
// chapter reserve, but one serial task is sufficient and remains below the
// page/chapter admission path.
const NEW_WORK_CATALOG_LANE_INTERVAL_MS = 5_000;
// The first new-work catalog pass may include bounded cross-source metadata
// collection. Keep its initial claim short for crash recovery, but renew it
// while that legitimate external I/O is in flight rather than letting the
// reconciler reclaim a healthy onboarding task mid-pass.
const CATALOG_TASK_LEASE_SECONDS = 90;
const CATALOG_TASK_LEASE_HEARTBEAT_MS = 30_000;
// A new work gets one bounded, network-only complement pass.  It runs outside
// YSQL transactions and never creates a retroactive task: unavailable sources
// are simply left to their normal future discovery instead of blocking the
// work's first publication indefinitely.
const CROSS_SOURCE_METADATA_CONCURRENCY = 4;
const CROSS_SOURCE_METADATA_TIMEOUT_MS = 7_500;
const CROSS_SOURCE_METADATA_MIN_CONFIDENCE = 0.95;
// A durable global task checkpoints several independent providers at once.
// The shared source gate still caps aggregate external I/O, while batching
// avoids spending one task turn per provider for a 43-source coverage pass.
// Bound cross-source discovery independently of the 43-provider catalogue.
// Durable checkedSources resumes the remaining providers in later turns.
const CROSS_SOURCE_DISCOVERY_SOURCES_PER_TURN = 2;
const CROSS_SOURCE_DISCOVERY_TURN_DELAY_MS = 30_000;
const CROSS_SOURCE_DISCOVERY_REFILL_INTERVAL_MS = 15_000;
// The direct importer pool is intentionally tiny and short chapter commits
// can produce two queued checkouts momentarily.  Treat only a persistent
// queue as pressure; an instantaneous sample must not strand network-only
// metadata work while the public site and YSQL are otherwise healthy.
const SOURCE_METADATA_POOL_PRESSURE_SUSTAIN_MS = 15_000;
// Under a real pool queue the producer needs one task, not an eight-source
// ranking horizon. Keeping this bounded avoids a distributed TableRead from
// monopolising the one background checkout while P1 persists chapter work.
const SOURCE_METADATA_CONSTRAINED_PROBE_LIMIT = 2;
// Under YSQL pressure the source lane still executes exactly one request and
// one merge at a time. Keep one additional durable P2 task READY so its next
// fetch does not wait for a multi-second cursor/materialisation transaction
// after every completion. This is prefetch depth, not execution concurrency.
const SOURCE_METADATA_CONSTRAINED_PREFETCH_DEPTH = 2;
const SOURCE_RECOVERY_PROBE_INTERVAL_MS = 30_000;
const CROSS_SOURCE_DEFERRED_RECHECK_INTERVAL_MS = 15 * 60_000;
// UNKNOWN Telegram recovery is exceptional and requires a historical
// uploads/media proof query. Keep it durable, but do not place that cold path
// in every 30-second chapter recovery turn.
const UPLOAD_OUTCOME_RECOVERY_INTERVAL_MS = 5 * 60_000;
/** Re-admit due source-cooldown work promptly once the chapter lane is quiet. */
const SOURCE_COOLDOWN_RECOVERY_IDLE_INTERVAL_MS = 30_000;
/** Max gap between SOURCE_RATE_LIMITED re-admission passes during a chapter wave. */
const SOURCE_COOLDOWN_RECOVERY_MAX_GAP_MS = 90_000;
/** Pause between V5 source-metadata producer chains that found no candidate. */
const SOURCE_METADATA_NO_CANDIDATE_BACKOFF_MS = 5 * 60_000;
// Confirmed manifest rows are the durable evidence for a historical chapter.
// At boot, repair any rows that older releases left without their idempotent
// import task quickly, but only in small database-only batches.  Once the
// orphan reserve is empty, retain the five-minute low-cost safety sweep for
// exceptional crash windows; normal catalog ingestion creates its tasks in
// the same flow as the manifest observation.
const MANIFEST_P1_CATCHUP_INTERVAL_MS = 30_000;
const MANIFEST_P1_STEADY_INTERVAL_MS = 5 * 60_000;
const MANIFEST_P1_CATCHUP_BATCH = 256;

type WorkerLiveness = {
  taskId: string | null;
  source: string | null;
  stage: 'CLAIM' | 'FETCH' | 'DOWNLOAD' | 'PUBLISH' | 'FINALIZE' | 'IDLE';
  since: string;
};

type SourceProbeRecord = {
  outcome:
    | 'MATCHED_ENRICHED'
    | 'MATCHED_NO_NEW_ALIASES'
    | 'NOT_FOUND'
    | 'REJECTED_AMBIGUOUS'
    | 'DEFERRED_EXTERNAL_ERROR'
    | 'ADAPTER_MISSING';
  queries: string[];
  candidateCount: number;
  topCandidateTitle?: string | null;
  sourceWorkId?: string | null;
  primaryTitle?: string | null;
  alternativeTitles?: string[];
  aliasesAdded?: number;
  tagsAdded?: number;
  confidence?: number;
  matchMethod?: string;
  error?: string | null;
};

export type ComplementaryCoverCandidate = {
  source: string;
  sourceWorkId: string;
  coverUrl: string;
  isBrazilianSource: boolean;
};

type CrossSourceMetadataResult = {
  busy: boolean;
  incomplete?: boolean;
  attemptedSources: string[];
  deferredSources: string[];
  attached: Array<{ source: string; sourceWorkId: string; aliasesAdded: number; tagsAdded: number }>;
  discoveredAliases?: string[];
  matchedCovers?: ComplementaryCoverCandidate[];
  sourceProbes?: Record<string, SourceProbeRecord>;
};

/** One process, one executor pool, one bounded reconciler. */
export class ImporterLiteRuntime {
  private stopped = false;
  private timers: NodeJS.Timeout[] = [];
  private metadataTurnTimer: NodeJS.Timeout | null = null;
  private readonly queue: LiteRepository;
  private readonly spareMonitorQueue: LiteRepository;
  private monitorSqlBorrowedOperations = 0;
  private readonly executor: LiteExecutor;
  private readonly dedupe: DeduplicationEngine;
  private readonly historicalMetadataDedupe: DeduplicationEngine;
  private readonly aliasOnlyService: AliasOnlyEnrichmentService;
  private readonly aliasOnlyRepo: AliasOnlyRepository;
  private readonly aliasOnlyGate = new AsyncSemaphore(1, 'lite_alias_only_enrichment');
  private readonly pendingImmediateAliasWorkIds = new Set<string>();
  private lastAliasOnlyTurnAt = 0;
  private aliasOnlyTelemetry = {
    processedWorks: 0,
    aliasesAdded: 0,
    lastRunAt: null as string | null,
    lastOutcome: 'IDLE' as string,
  };
  private readonly coverVisionQueue: CoverVisionQueue;
  private readonly coverVisionGate = new AsyncSemaphore(1, 'lite_cover_vision');
  private readonly pendingImmediateCoverVisionWorkIds = new Set<string>();
  private lastCoverVisionTurnAt = 0;
  private coverVisionTelemetry = {
    processedWorks: 0,
    promotedTitles: 0,
    lastRunAt: null as string | null,
    lastOutcome: 'IDLE' as string,
  };
  private readonly logger = new Logger('ImporterLite');
  private readonly downloads: AsyncSemaphore;
  private readonly bufferedPages: AsyncSemaphore;
  private readonly bufferedPageBytes: PageByteBudget;
  private readonly memoryPressureGuard: LiteMemoryPressureGuard;
  private readonly containerRamMiB: number;
  private readonly sourcePerformance = new SourcePerformanceTracker();
  private readonly configuredPageBufferCapacity: number;
  // Vitest shares one Node process across many integration fixtures, so its
  // host RSS is not the Lite runtime's container RSS. The guard itself is
  // exercised with deterministic memory snapshots; production always enables
  // it against the real process.
  private readonly memoryPressureEnabled = process.env.NODE_ENV !== 'test';
  private readonly claimGate = new AsyncSemaphore(1, 'lite_claim_gate');
  private readonly chapterAdmissionGate: AsyncSemaphore;
  private readonly historicalP1BackfillGate = new AsyncSemaphore(1, 'lite_historical_p1_backfill');
  // Coverage enrolment is a short, isolated producer transaction.  It is not
  // a second P0 executor: the existing monitor lane remains the only path
  // that fetches a source snapshot and materialises a P0 chapter.
  private readonly p0MonitorCoverageRefillGate = new AsyncSemaphore(1, 'lite_p0_monitor_coverage_refill');
  private readonly metadataReplayGate = new AsyncSemaphore(1, 'lite_metadata_replay');
  // Metadata ticks are rearmed before a prior turn completes so recovery is
  // prompt after long external work. The tick itself must nevertheless stay
  // single-flight: otherwise each overlapping turn queues its settings and
  // producer reads on the one isolated YSQL connection.
  private readonly metadataTurnGate = new AsyncSemaphore(1, 'lite_metadata_turn');
  // The exclusive V4 campaign may merge four distinct canonical works. A
  // separate gate keeps normal operation strictly single-flight.
  private readonly metadataReplayExclusiveGate = new AsyncSemaphore(4, 'lite_metadata_replay_exclusive');
  private readonly metadataReplayV4RefillGate = new AsyncSemaphore(1, 'lite_metadata_replay_v4_refill');
  private readonly metadataReplayV4PersistenceGate = new AsyncSemaphore(4, 'lite_metadata_replay_v4_persistence');
  private readonly sourceMetadataRefillGate = new AsyncSemaphore(1, 'lite_source_metadata_refill');
  // The regular metadata turn runs on a short cadence so an idle P2 lane can
  // recover promptly. Cross-source materialisation can outlive that cadence,
  // though, and shares the one isolated YSQL connection with source fairness
  // sampling. A second timer turn cannot admit useful extra work; it only
  // queues another transaction behind the first.
  private readonly crossSourceMetadataRefillGate = new AsyncSemaphore(1, 'lite_cross_source_metadata_refill');
  // Network waits are independent from YSQL work. Four source requests may
  // wait on distinct providers, while preparing a mapping and committing its
  // merge remain deliberately narrow. No transaction or pool checkout spans
  // the HTTP phase.
  private readonly sourceMetadataGate = new AsyncSemaphore(4, 'lite_source_metadata_refetch');
  private readonly sourceMetadataPreparationGate = new AsyncSemaphore(1, 'lite_source_metadata_prepare');
  // One physical P2 YSQL seat is reserved. Keep the durable source-refresh
  // lane aligned with that budget so low-priority work never builds a local
  // checkout queue that competes with reader traffic.
  private readonly sourceMetadataLaneGate = new AsyncSemaphore(1, 'lite_source_metadata_lane');
  private readonly sourceMetadataPersistenceGate = new AsyncSemaphore(1, 'lite_source_metadata_persistence');
  private readonly newWorkCatalogGate = new AsyncSemaphore(1, 'lite_new_work_catalog_lane');
  // P2 has dedicated lanes so aliases, metadata and new works never depend on
  // an empty P1 queue.  Those lanes previously started independently just as
  // a full chapter wave began, however, which let three low-priority catalog
  // transactions contend with the three-seat primary pool.  While a chapter
  // is admitted, retain one P2 seat: it prevents permanent starvation but
  // reserves the remaining shared YSQL capacity for publication.
  private readonly backgroundMetadataUnderChapterLoadGate = new AsyncSemaphore(1, 'lite_background_metadata_under_chapter_load');
  private lastNewWorkDiscoveryAt = 0;
  private lastNewWorkPressureTurnAt = 0;
  private pressureDiscoveryTurn = true;
  // Reserve 3 permits (2 for background V7 discovery + 1 for NEW_WORK onboarding)
  // so background V7 tasks can never starve a newly created work's onboarding pass.
  private readonly crossSourceMetadataGate = new AsyncSemaphore(3, 'lite_cross_source_metadata');
  private readonly crossSourceDiscoveryLaneGate = new AsyncSemaphore(1, 'lite_cross_source_discovery_lane');
  private sourceMetadataPoolWaitStartedAt = 0;
  private readonly sourceAdmissionGate = new SourceAdmissionGate(10_000);
  private lastMetadataEnrichmentAt = 0;
  private lastMetadataReplayAt = 0;
  private lastSourceMetadataRefillAt = 0;
  private lastCrossSourceDiscoveryRefillAt = 0;
  private sourceMetadataPreferredSource: string | null = null;
  private sourceMetadataPreferredSourceBurst = 0;
  // V4 is a finite, no-network campaign over metadata we already possess.
  // It uses an isolated pool and defers optional external re-fetches until
  // its durable setting says the campaign is done;
  // source re-fetch tasks stay durable and resume afterwards.
  private sourceMetadataRefreshDeferredForV4 = false;
  // An operator may temporarily reserve importer capacity for the finite V4
  // replay. The setting is durable and is cleared automatically when V4
  // exhausts its cursors, so a restart cannot strand chapter ingestion.
  private metadataReplayV4Exclusive = { enabled: false, intervalMs: STORED_METADATA_REPLAY_V4_EXCLUSIVE_DEFAULT_INTERVAL_MS };
  private lastMetadataReplayV4ExclusiveRefreshAt = 0;
  private metadataTurboMode: {
    enabled: boolean;
    discoveryConcurrency?: number;
    searchConcurrency?: number;
    metadataGateCapacity?: number;
    persistenceCapacity?: number;
  } = { enabled: false };
  private lastMetadataTurboModeRefreshAt = 0;
  private readonly metadataReplayV4WorkTails = new Map<string, Promise<void>>();
  private sourcePoliciesCache: { map: Map<string, LiteSourcePolicy>; expiresAt: number } | null = null;
  // The historical producer is intentionally low-rate. Persist its last
  // admission decision in the heartbeat so a guarded skip cannot look like a
  // silently stalled backlog after a native deploy.
  private sourceMetadataProducer = {
    observedAt: null as string | null,
    outcome: 'NEVER' as string,
    admitted: 0,
    error: null as string | null,
  };
  // This is deliberately in-memory telemetry for the bounded P1 producer.
  // Its durable cursor remains the source of truth; recording each recovery
  // turn lets an operator distinguish a deployed-but-idle cursor from a
  // runtime that never received the source-fairness implementation.
  private historicalP1Producer = {
    observedAt: null as string | null,
    admitted: 0,
    promoted: false,
    error: null as string | null,
  };
  // Keep this in the heartbeat so an operator can distinguish “P0 found no
  // release” from “this runtime never enrolled most existing works for P0
  // observation”.  The durable cursor/task rows remain authoritative.
  private p0MonitorCoverage = {
    observedAt: null as string | null,
    source: null as string | null,
    admitted: 0,
    revived: 0,
    outcome: 'NEVER' as string,
    error: null as string | null,
  };
  private lastHistoricalP1ProducerAttemptAt = 0;
  private lastSourceRecoveryProbeAt = 0;
  private lastUploadOutcomeRecoveryAt = 0;
  // Watermark for the cold UNKNOWN-receipt proof. The indexed newest-row
  // probe costs one storage read; the proof itself otherwise joins every
  // historical UNKNOWN receipt to media even after an empty pass.
  private lastUnknownUploadRecoverySeenAt = 0;
  private lastSourceCooldownRecoveryAt = 0;
  private lastColdReconcileMaintenanceAt = 0;
  // Source discovery rows are durable and idempotent. A transient YSQL
  // timeout while seeding them must never take down a freshly elected daemon;
  // retain the failed seed for a bounded retry after workers/heartbeats start.
  private nextDiscoverySeedRetryAt = 0;
  private discoverySeed = {
    attemptedAt: null as string | null,
    completedAt: null as string | null,
    sources: 0,
    inserted: 0,
    failed: 0,
    error: null as string | null,
  };
  // Backoff after an exhausted V5 source-metadata probe chain. Incremental
  // coverage continues; it just stops rescanning empty horizons every 15 s.
  private sourceMetadataNoCandidateUntil = 0;
  private lastManifestP1RepairAt = 0;
  private manifestP1CatchupActive = false;
  // P0 observation has ten bounded network slots. They keep independent
  // source watches current across 43 sources while chapters run. Only one
  // short SQL claim is ever in flight (monitorClaimGate); the remaining slots
  // run independent source network fetches in parallel without YSQL contention.
  private readonly monitorGate = new AsyncSemaphore(10, 'lite_p0_monitor');
  private readonly monitorClaimGate = new AsyncSemaphore(1, 'lite_p0_monitor_claim');
  private readonly monitorClaimBuffer: LiteTask[] = [];
  private readonly monitorActiveBySource = new Map<string, number>();
  private readonly monitorFinishBatcher: MonitorFinishBatcher;
  private monitorClaimBatchCount = 0;
  private readonly runtimeInstanceId: string;
  private readonly runtimeBootedAt = new Date();
  private readonly chapterWorkerCapacity: number;
  private isRuntimeLeader = false;
  private runtimeLeadershipTimer: NodeJS.Timeout | null = null;
  private runtimeLeadershipRenewing = false;
  private leaseRenewStartedAt: string | null = null;
  private leaseRenewCompletedAt: string | null = null;
  private leaseRenewError: string | null = null;
  private heartbeatStartedAt: string | null = null;
  private heartbeatCompletedAt: string | null = null;
  // Full snapshots aggregate several large task-table views. Retain the
  // timestamp of the last completed one so the compact heartbeat is explicit
  // about how fresh those optional diagnostics are while chapters are busy.
  private lastFullOperationalSnapshotAt: string | null = null;
  // A full operational snapshot includes optional aggregate diagnostics. It
  // must never overlap itself when the shared two-connection pool is slow:
  // concurrent timer turns otherwise amplify the very YSQL wait they report.
  private operationalSnapshotRunning = false;
  private readonly workerLiveness = new Map<string, WorkerLiveness>();
  private sitePausedUntil = 0;
  // A confirmed site-health failure still stops new claims immediately.  A
  // single early recheck prevents a brief Cloudflare/egress failure from
  // consuming the entire 30-second protection interval after Reader has
  // already recovered.  Repeated rechecks are deliberately avoided: the
  // normal health cadence remains authoritative during a real outage.
  private siteRecoveryProbeScheduled = false;
  private idleClaimUntil = 0;
  private idleMonitorUntil = 0;
  // `readyImportSources` is sampled outside the durable claim SQL. When the
  // highest runnable import priority changes, retaining the prior P0-only
  // sample until the 30-second recovery turn lets the first P1 wave bypass
  // source-share limits even though alternatives are already READY.
  private fairDemandImportPriority: LitePriority | null = null;
  // The runnable-source view is a fairness hint, never the durable claim
  // authority.  Keep one refresh in flight and retain its last successful
  // result while chapter persistence is already waiting for the small YSQL
  // pool; a second multi-work scan under that pressure only delays the
  // publication it is meant to distribute fairly.
  private sourceFairnessRefresh: Promise<boolean> | null = null;
  private manualProtectiveStop = {
    active: false,
    reason: null as string | null,
    checkedAt: new Date(0).toISOString(),
  };
  private operatorChapterAdmissionCap = 0;
  private adaptiveChapterAdmissionCap = 0;
  private adaptiveHealthyStreak = 0;
  private adaptiveAdmissionReason = 'operator-ceiling';
  private lastLegacyBlockedRepairAt = 0;
  private p0CoverageZeroStreak = 0;
  private lastP0CoverageRefillAt = 0;
  private p1ClaimCounter = 0;
  private lastP1FocusRefreshAt = 0;
  private p1FocusRefreshInFlight: Promise<void> | null = null;
  private lastSourceFairnessRefreshAt = 0;
  private idleHistoricalP1CatalogUntil = 0;
  private readonly historicalP1CatalogExecGate = new AsyncSemaphore(2, 'lite_historical_p1_catalog_exec');
  private readonly historicalP1CatalogClaimGate = new AsyncSemaphore(1, 'lite_historical_p1_catalog_claim');
  private p2SubordinationState: { subordinated: boolean; reason: string | null; checkedAt: string | null } = {
    subordinated: false,
    reason: null,
    checkedAt: null,
  };
  private lastSiteProbe: { state: 'UNKNOWN' | 'HEALTHY' | 'DEGRADED' | 'PAUSED'; checkedAt: string; latencyMs: number | null } = {
    state: 'UNKNOWN', checkedAt: new Date(0).toISOString(), latencyMs: null,
  };

  /**
   * A queued checkout from the importer's two-connection pool is a local,
   * transient condition, not proof that the Reader is unhealthy. Claims are
   * already serialized by `claimGate`; freezing every chapter slot whenever
   * one write is queued turns ordinary receipt persistence into avoidable
   * idle time. A full diagnostic snapshot is the exception: it begins only
   * while idle, then runs several optional reads. Do not admit a new chapter
   * into that short pool window after its idle check has already passed.
   */
  private shouldPauseChapterAdmission(): boolean {
    return this.manualProtectiveStop.active
      || Date.now() < this.sitePausedUntil
      || this.metadataReplayV4Exclusive.enabled
      || this.metadataTurboMode.enabled
      || this.operationalSnapshotRunning;
  }

  /**
   * Read the durable staff brake and chapter ramp cap on the existing
   * 30-second health cadence. This is one indexed lookup for the whole
   * process, never a per-slot poll. A stop prevents a new claim; a reduced
   * cap drains in-flight work naturally instead of cancelling a safe write.
   */
  private async refreshManualProtectiveStop(): Promise<void> {
    try {
      const result = await this.pool.query<{ key: string; value: string }>(
        'SELECT key,value FROM settings WHERE key IN ($1,$2)',
        [MANUAL_PROTECTIVE_STOP_KEY, CHAPTER_ADMISSION_CAP_KEY],
      );
      const values = new Map(result.rows.map((row) => [row.key, row.value]));
      const parse = (raw: string | undefined): unknown => {
        try { return raw ? JSON.parse(raw) : null; } catch { return raw; }
      };
      const parsed = parse(values.get(MANUAL_PROTECTIVE_STOP_KEY));
      const object = parsed && typeof parsed === 'object' ? parsed as { active?: unknown; reason?: unknown } : null;
      const active = parsed === true || parsed === 'true' || object?.active === true;
      const reason = object && typeof object.reason === 'string' ? object.reason.slice(0, 240) : null;
      const changed = active !== this.manualProtectiveStop.active || reason !== this.manualProtectiveStop.reason;
      this.manualProtectiveStop = { active, reason, checkedAt: new Date().toISOString() };
      if (changed) {
        const log = active ? this.logger.warn.bind(this.logger) : this.logger.info.bind(this.logger);
        log(active ? 'Manual protective stop is blocking new Lite claims' : 'Manual protective stop cleared; Lite may resume claims', {
          reason,
        });
      }

      const capValue = parse(values.get(CHAPTER_ADMISSION_CAP_KEY));
      const capObject = capValue && typeof capValue === 'object'
        ? capValue as { cap?: unknown; capacity?: unknown; chapterWorkers?: unknown }
        : null;
      const requestedCap = capObject?.chapterWorkers ?? capObject?.capacity ?? capObject?.cap ?? capValue;
      const numericCap = typeof requestedCap === 'number' ? requestedCap : Number(requestedCap);
      const nextCap = Number.isFinite(numericCap) && numericCap >= 1
        ? Math.min(this.chapterWorkerCapacity, Math.max(1, Math.floor(numericCap)))
        : this.chapterWorkerCapacity;
      if (nextCap !== this.operatorChapterAdmissionCap) {
        const previousCap = this.operatorChapterAdmissionCap;
        this.operatorChapterAdmissionCap = nextCap;
        this.adaptiveChapterAdmissionCap = nextCap;
        this.adaptiveHealthyStreak = 0;
        this.adaptiveAdmissionReason = values.has(CHAPTER_ADMISSION_CAP_KEY) ? 'operator-setting' : 'runtime-default';
        this.applyEffectiveChapterAdmissionCap(nextCap, this.adaptiveAdmissionReason, previousCap);
      } else if (this.adaptiveChapterAdmissionCap > nextCap) {
        const prevEffective = this.chapterAdmissionGate.capacity;
        this.adaptiveChapterAdmissionCap = nextCap;
        this.applyEffectiveChapterAdmissionCap(nextCap, 'operator-ceiling-clamp', prevEffective);
      }
    } catch (error: any) {
      // Preserve the last known durable decision. A diagnostic lookup must
      // not turn a transient YSQL wait into a restart or an aggressive retry.
      this.logger.warn('Manual protective-stop check deferred', { error: error?.message });
    }
  }

  private applyEffectiveChapterAdmissionCap(effectiveCap: number, source: string, previousCap = this.chapterAdmissionGate.capacity): void {
    const bounded = Math.min(this.chapterWorkerCapacity, Math.max(1, Math.floor(effectiveCap)));
    if (bounded === this.chapterAdmissionGate.capacity && this.executor.getEffectiveChapterAdmissionCap() === bounded) {
      return;
    }
    this.chapterAdmissionGate.setCapacity(bounded);
    this.executor.setEffectiveChapterAdmissionCap(bounded);
    this.memoryPressureGuard.setNormalMediaProfile(this.mediaProfileForChapterAdmissionCap(bounded));
    this.logger.info('Adjusted Lite chapter admission cap', {
      previousCap,
      nextCap: bounded,
      operatorCap: this.operatorChapterAdmissionCap,
      configuredWorkers: this.chapterWorkerCapacity,
      mediaProfile: this.memoryPressureGuard.getNormalMediaProfile(),
      source,
    });
  }

  /**
   * Coherent adaptive backpressure with hysteresis reusing the single existing
   * chapterAdmissionGate (never interrupting in-flight chapters or critical DB
   * writes). Steps down when Reader latency or YSQL pool pressure rises, and
   * ramps back +1 only after 2 consecutive healthy 30-second observations.
   */
  private updateAdaptiveAdmissionControl(): void {
    if (this.manualProtectiveStop.active) {
      this.adaptiveHealthyStreak = 0;
      return;
    }
    const ceiling = this.operatorChapterAdmissionCap > 0
      ? this.operatorChapterAdmissionCap
      : this.chapterWorkerCapacity;
    if (this.adaptiveChapterAdmissionCap <= 0) {
      this.adaptiveChapterAdmissionCap = ceiling;
    }
    const floor = Math.min(this.operatorChapterAdmissionCap >= 15 ? 12 : 8, ceiling);
    const current = Math.min(ceiling, Math.max(floor, this.adaptiveChapterAdmissionCap));
    const siteLatencyMs = this.lastSiteProbe.latencyMs;
    const dbSnap = (telemetryCollector.getSnapshotReport() as any)?.database || {};
    const sqlP95Ms = Number(dbSnap.sqlP95Ms || 0);
    const poolWaiting = Number(this.pool.waitingCount || 0);
    const memState = this.memoryPressureGuard.snapshot().state;

    const highSitePressure = this.lastSiteProbe.state === 'PAUSED'
      || (this.lastSiteProbe.state !== 'HEALTHY' && typeof siteLatencyMs === 'number' && siteLatencyMs > 5_000)
      || (typeof siteLatencyMs === 'number' && siteLatencyMs > 4_500);
    const highYsqlPressure = (poolWaiting >= 6 && sqlP95Ms > 2_500)
      || (poolWaiting >= 2 && sqlP95Ms > 4_500);

    if (highSitePressure || highYsqlPressure) {
      this.adaptiveHealthyStreak = 0;
      const reduced = Math.max(floor, Math.min(current - 1, Math.floor(current * 0.75)));
      this.adaptiveAdmissionReason = highSitePressure ? 'adaptive-site-pressure' : 'adaptive-ysql-pressure';
      if (reduced < current) {
        this.adaptiveChapterAdmissionCap = reduced;
        this.applyEffectiveChapterAdmissionCap(reduced, this.adaptiveAdmissionReason, current);
      }
      return;
    }

    const healthyForRamp = (this.lastSiteProbe.state === 'HEALTHY' || this.lastSiteProbe.state === 'UNKNOWN' || this.lastSiteProbe.state === 'DEGRADED')
      && (siteLatencyMs === null || siteLatencyMs <= 3_800)
      && !highYsqlPressure
      && memState === 'NORMAL';

    if (healthyForRamp) {
      this.adaptiveHealthyStreak++;
      if (current < ceiling && this.adaptiveHealthyStreak >= 1) {
        this.adaptiveHealthyStreak = 0;
        const steppedUp = Math.min(ceiling, current + 3);
        this.adaptiveChapterAdmissionCap = steppedUp;
        this.adaptiveAdmissionReason = steppedUp === ceiling ? 'adaptive-restored-ceiling' : 'adaptive-step-up';
        this.applyEffectiveChapterAdmissionCap(steppedUp, this.adaptiveAdmissionReason, current);
      }
    } else {
      this.adaptiveHealthyStreak = 0;
    }
  }

  /**
   * A process has no workers before start() reaches its durable settings
   * check. Arm the brake at that boundary, rather than making every
   * unstarted runtime instance look manually paused. If the lookup fails,
   * refreshManualProtectiveStop intentionally preserves this state.
   */
  private armStartupProtectiveStop(): void {
    this.manualProtectiveStop = {
      active: true,
      reason: 'startup pending durable protective-stop check',
      checkedAt: new Date().toISOString(),
    };
  }

  /**
   * Metadata is optional P2 work. Yield it before taking away an in-flight
   * chapter's page/upload permits; page-level backpressure itself makes any
   * already-started chapter drain safely.
   */
  private shouldPauseLowPriorityMetadata(): boolean {
    return this.refreshMemoryPressure().pauseLowPriorityMetadata;
  }

  private refreshMemoryPressure(): LiteMemoryPressureSnapshot {
    if (!this.memoryPressureEnabled) return this.memoryPressureGuard.snapshot();
    const before = this.memoryPressureGuard.snapshot().state;
    const snapshot = this.memoryPressureGuard.sample();
    // Source metadata is P2 and yields first, but a retained native RSS high
    // watermark must not freeze aliases/tags forever. Reduced mode keeps one
    // fetch/merge lane; critical mode accepts no new metadata work.
    this.sourceMetadataLaneGate.setCapacity(snapshot.state === 'NORMAL' && this.metadataTurboMode.enabled ? 6 : 1);
    if (snapshot.state !== before) {
      const log = snapshot.state === 'NORMAL' ? this.logger.info.bind(this.logger) : this.logger.warn.bind(this.logger);
      log('Adjusted Lite page admission for memory pressure', snapshot);
    }
    return snapshot;
  }

  /**
   * The clean #832 cap-three ramp measured 18 simultaneous page downloads
   * and reached 375 MiB RSS with the former 24-body/48-MiB envelope.  The
   * media wall is still the throughput bottleneck, so a wider chapter ramp
   * must share a smaller page wave rather than multiply that allocation
   * high-water. Twenty bodies remain fixed for caps three through seven.
   *
   * The #833 cap-five sample established a distinct, bounded constraint:
   * its 40 MiB page budget was fully reserved with five page waiters while
   * RSS was 318 MiB and YSQL had no waiter. #834 raised that envelope to
   * 48 MiB, but the next sample still had 31 MiB reserved and 21 waiters.
   * A lengthless body needs a final 19 MiB promotion after its 1 MiB initial
   * reservation, so 48 MiB cannot make terminal progress at that observed
   * live occupancy. Caps five through seven therefore share 56 MiB: enough for
   * that one bounded completion without widening the fixed 20-page fence.
   * REDUCED remains bounded at 48 MiB and CRITICAL remains the one-page
   * brake. In particular, caps six and seven must not fall through to the legacy
   * 48-body/80-MiB profile: that would turn a one-seat admission experiment
   * into an unmeasured media expansion.
   */
  private mediaProfileForChapterAdmissionCap(cap: number): { pageBufferCapacity: number; pageByteCapacity: number } {
    const normalizedCap = Math.max(1, Math.floor(cap));
    if (normalizedCap >= 3 && normalizedCap <= 7) {
      return {
        // The byte budget remains the authority for live media memory.  A
        // production cap-seven sample left four download permits idle while
        // twenty count-only buffer slots had nine waiters (p95 15.9 s), with
        // only 27 MiB of live external memory.  Thirty-two slots remove that
        // artificial count fence without widening the measured 56 MiB byte
        // envelope used by cap five through seven.
        pageBufferCapacity: Math.min(this.configuredPageBufferCapacity, normalizedCap >= 5 ? 32 : 20),
        pageByteCapacity: (normalizedCap >= 5 ? 56 : 40) * 1024 * 1024,
      };
    }
    return {
      pageBufferCapacity: Math.min(this.configuredPageBufferCapacity, Math.max(8, normalizedCap * 8)),
      pageByteCapacity: normalizedCap <= 2
        ? 48 * 1024 * 1024
        : (this.containerRamMiB >= 600 ? 128 * 1024 * 1024 : LITE_PAGE_BUFFER_BUDGET_BYTES),
    };
  }

  constructor(
    private readonly pool: Pool,
    private readonly catalogClient: any,
    private readonly registry: SourceRegistry,
    private readonly storage: StorageProvider,
    private readonly config: Config,
    private readonly sourceRateLimiter: HostRateLimiter = new HostRateLimiter(5.0),
    private readonly sourceMetadataPool: Pool = pool,
  ) {
    if (!config.IMPORTER_USER_ID) throw new Error('IMPORTER_USER_ID is required when IMPORTER_RUNTIME_MODE=lite');
    this.runtimeInstanceId = `${config.WORKER_ID}@${process.pid}-${randomUUID().slice(0, 8)}`;
    this.queue = new LiteRepository(pool, sourceMetadataPool);
    this.spareMonitorQueue = new LiteRepository(pool, pool);
    this.monitorFinishBatcher = new MonitorFinishBatcher((batch) => this.monitorSqlQueue().finishUnchangedMonitors(batch));
    this.downloads = new AsyncSemaphore(config.DOWNLOAD_INFLIGHT_CONCURRENCY, 'lite_global_page_downloads');
    this.configuredPageBufferCapacity = Math.min(config.BUFFERED_PAGE_CONCURRENCY, SAFE_BUFFERED_PAGE_CAP);
    this.bufferedPages = new AsyncSemaphore(this.configuredPageBufferCapacity, 'lite_global_page_buffers');
    this.containerRamMiB = detectContainerRamMiB();
    const initialNormalPageBytes = this.containerRamMiB >= 600
      ? 128 * 1024 * 1024
      : LITE_PAGE_BUFFER_BUDGET_BYTES;
    this.bufferedPageBytes = new PageByteBudget(initialNormalPageBytes);
    this.memoryPressureGuard = new LiteMemoryPressureGuard(
      this.bufferedPages,
      this.bufferedPageBytes,
      this.bufferedPages.capacity,
      initialNormalPageBytes,
      this.containerRamMiB,
    );
    this.chapterWorkerCapacity = Math.max(1, Math.min(config.LITE_CHAPTER_WORKERS ?? 1,
      config.MAX_CONCURRENT_CHAPTERS, config.TESTED_CONCURRENCY_CEILING));
    this.operatorChapterAdmissionCap = this.chapterWorkerCapacity;
    this.chapterAdmissionGate = new AsyncSemaphore(this.chapterWorkerCapacity, 'lite_chapter_admission');
    // Fast/normal sources use their measured source-scoped page wave up to 10
    // inside the unchanged global download (40), buffer (80), byte (128 MiB),
    // and Telegram (24) hard fences.
    this.executor = new LiteExecutor(pool, this.queue, registry, storage, config.IMPORTER_USER_ID, this.downloads,
      Math.max(1, Math.min(10, config.BATCH_PAGE_DOWNLOAD_CONCURRENCY)), this.bufferedPages, sourceRateLimiter,
      this.chapterWorkerCapacity, (taskId, source) => this.setWorkerSource(taskId, source), this.bufferedPageBytes,
      config.SOURCE_OPERATION_TIMEOUT_MS, async () => { await this.refreshSourceFairness(); });
    this.executor.setPerformanceTracker(this.sourcePerformance, () => {
      if (Date.now() - this.lastP1FocusRefreshAt >= 15_000) {
        void this.refreshP1FocusGroupNow(true).catch(() => {});
      }
    });
    this.dedupe = new DeduplicationEngine(catalogClient);
    this.historicalMetadataDedupe = sourceMetadataPool === pool
      ? this.dedupe
      // DirectSupabaseClient implements the query surface used by the narrow
      // historical merge. The main catalog client remains the typed client
      // used for onboarding/full-work deduplication.
      : new DeduplicationEngine(new DirectSupabaseClient(sourceMetadataPool) as any);
    this.aliasOnlyService = new AliasOnlyEnrichmentService();
    this.aliasOnlyRepo = new AliasOnlyRepository(sourceMetadataPool);
    this.coverVisionQueue = new CoverVisionQueue(sourceMetadataPool);
  }

  /** Borrow only idle SQL capacity while a recent Reader probe is fast. */
  private mayBorrowPrimaryForMonitor(minimumIdle = 1): boolean {
    const probe = this.lastSiteProbe;
    const checkedAt = Date.parse(probe.checkedAt);
    return this.sourceMetadataPool !== this.pool
      && (this.sourceMetadataPool.waitingCount || 0) > 1
      && (this.pool.waitingCount || 0) === 0
      && (this.pool.idleCount || 0) >= minimumIdle
      && probe.state === 'HEALTHY'
      && Number.isFinite(checkedAt) && Date.now() - checkedAt <= 180_000
      && probe.latencyMs !== null && probe.latencyMs <= 2_000;
  }

  private monitorSqlQueue(): LiteRepository {
    if (!this.mayBorrowPrimaryForMonitor()) return this.queue;
    this.monitorSqlBorrowedOperations += 1;
    return this.spareMonitorQueue;
  }

  private getAvailablePool(): Pool {
    if (!this.sourceMetadataPool || this.sourceMetadataPool === this.pool) return this.pool;
    if (this.metadataTurboMode.enabled && this.sourceMetadataPool.waitingCount > 0 && this.pool.idleCount > 0) {
      return this.pool;
    }
    return this.sourceMetadataPool;
  }

  async start(): Promise<void> {
    // Fail closed before any boot-time work can make a future worker eligible.
    // A successful durable read below is the only way this temporary brake is
    // cleared; a YSQL timeout leaves it armed.
    this.armStartupProtectiveStop();
    // Do this before taking the shared producer lease or creating a task. A
    // Lite process must fail closed against an old Reader schema, rather than
    // discover a numeric collision after it has downloaded/uploaded media.
    // Use the isolated metadata connection here: during a rolling deploy the
    // primary importer pool can still be draining an old claim/receipt. If
    // this small read-only gate waits behind it, the successor cannot acquire
    // the expired leadership lease and source-metadata campaigns stall even
    // though their dedicated pool is healthy.
    await this.assertReaderCanonicalIdentitySchema();
    if (!(await this.waitForRuntimeLeadership())) return;
    await this.refreshManualProtectiveStop();
    await this.refreshMetadataReplayV4ExclusiveMode(true);
    await this.refreshMetadataTurboMode(true);
    // Probe before admitting work, then bring durable workers online without
    // waiting for optional catalog/metadata bootstrap to finish.
    await this.probeSharedResources();
    // Do not inherit an unmeasured legacy MAX_CONCURRENT_CHAPTERS during a
    // Lite cutover.  The Lite-specific cap begins safely and can only rise
    // through an explicit, benchmarked staging configuration.
    const workers = this.chapterWorkerCapacity;
    telemetryCollector.configureChapterSlots(workers, () => this.chapterAdmissionGate.capacity);
    // Queue lease owners include the per-boot leader identity. A process that
    // lost leadership can never checkpoint or finish a lease reclaimed by a
    // successor which happens to use the same configured WORKER_ID.
    for (let i = 0; i < workers; i++) void this.worker(`${this.runtimeInstanceId}:lite:${i + 1}`, i);
    const timer = setInterval(() => {
      if (this.isRuntimeLeader) void this.runRecoveryTurn();
    }, 30_000);
    timer.unref(); this.timers.push(timer);
    const monitorTimer = setInterval(() => void this.runMonitorTurn().catch(() => {}), 800);
    monitorTimer.unref(); this.timers.push(monitorTimer);
    // Sample RSS independently from the 60-second diagnostic heartbeat. The
    // fixed byte ceiling makes overshoot bounded; this one-second brake then
    // prevents the next page wave from rebuilding pressure.
    const memoryTimer = setInterval(() => this.refreshMemoryPressure(), 1_000);
    memoryTimer.unref(); this.timers.push(memoryTimer);
    this.scheduleMetadataTurn();
    const sourceMetadataTimer = setInterval(() => {
      if (!this.isRuntimeLeader) return;
      void this.runSourceMetadataRefresh();
    }, SOURCE_METADATA_LANE_INTERVAL_MS);
    sourceMetadataTimer.unref(); this.timers.push(sourceMetadataTimer);
    const newWorkCatalogTimer = setInterval(() => {
      if (!this.isRuntimeLeader) return;
      void this.runNewWorkCatalog();
    }, NEW_WORK_CATALOG_LANE_INTERVAL_MS);
    newWorkCatalogTimer.unref(); this.timers.push(newWorkCatalogTimer);
    const historicalP1CatalogTimer = setInterval(() => {
      if (!this.isRuntimeLeader) return;
      void this.runHistoricalP1CatalogTurn();
    }, 3_000);
    historicalP1CatalogTimer.unref(); this.timers.push(historicalP1CatalogTimer);
    const healthTimer = setInterval(() => void this.probeSharedResources(), 120_000);
    healthTimer.unref(); this.timers.push(healthTimer);
    const metricsTimer = setInterval(() => void this.emitOperationalSnapshot().catch(() => {}), 60_000);
    metricsTimer.unref(); this.timers.push(metricsTimer);
    void this.bootstrapRuntime().catch((err: any) => {
      this.logger.warn('Background startup maintenance failed', { error: err?.message });
    });
    await this.emitOperationalSnapshot().catch((err: any) => {
      this.logger.warn('Initial operational snapshot deferred', { error: err?.message });
    });
  }

  /** Optional bootstrap must not hold P0 or chapter workers offline. */
  private async bootstrapRuntime(): Promise<void> {
    if (this.metadataReplayV4Exclusive.enabled) {
      // Do not create discovery/catalog/chapter work while a durable
      // exclusive campaign is active. Existing in-flight operations retain
      // their leases and settle before their worker sees the admission gate.
      await Promise.all([this.runStoredMetadataReplay(), this.runStoredMetadataReplay(), this.runStoredMetadataReplay(), this.runStoredMetadataReplay()]);
      await this.refillStoredMetadataEnrichment();
    } else {
      // V5 has its own pool and must not wait for restart-time chapter
      // housekeeping on the saturated main pool. Start its bounded producer
      // before those optional scans; leadership, provider limits, memory and
      // its own pool gates remain authoritative.
      void (async () => {
        try {
          await this.refillSourceMetadataEnrichment();
          await Promise.all([this.runSourceMetadataRefresh(), this.runSourceMetadataRefresh()]);
          // Global discovery's admission uses the primary queue pool. It can
          // wait behind a rolling deploy's chapter claims; do not strand
          // already-durable V5 source tasks behind that unrelated refill.
          await this.refillCrossSourceMetadataDiscovery();
          await Promise.all([this.runCrossSourceMetadataDiscovery(), this.runCrossSourceMetadataDiscovery()]);
        } catch (error: any) {
          this.logger.warn('Early V5 metadata startup failed', { error: error?.message });
        }
      })();
      await this.seedDiscovery();
      // A former runtime may have left every source observation due at once.
      // Stagger only discovery rows; executable catalog/chapter work remains
      // immediately eligible under the normal strict priority contract.
      try {
        await this.queue.staggerReadyDiscoveries(60_000, 64);
      } catch (staggerError: any) {
        this.logger.warn('Early discovery stagger deferred or failed non-fatally', { error: staggerError?.message });
      }
      this.lastLegacyBlockedRepairAt = Date.now();
      try {
        await this.queue.recoverDuplicateCanonicalCatalogFailures();
      } catch (err: any) {
        this.logger.warn('Boot recoverDuplicateCanonicalCatalogFailures failed non-fatally', { error: err?.message });
      }
      try {
        await this.queue.reopenP0MonitorsBlockedByDiscoveryPolicy();
      } catch (err: any) {
        this.logger.warn('Boot reopenP0MonitorsBlockedByDiscoveryPolicy failed non-fatally', { error: err?.message });
      }
      try {
        await this.refillP0MonitorCoverage();
      } catch (err: any) {
        this.logger.warn('Boot refillP0MonitorCoverage failed non-fatally', { error: err?.message });
      }
      try {
        await this.refreshSourceFairness();
      } catch (err: any) {
        this.logger.warn('Boot refreshSourceFairness failed non-fatally', { error: err?.message });
      }
      try {
        await this.refillHistoricalP1Catalog();
      } catch (err: any) {
        this.logger.warn('Boot refillHistoricalP1Catalog failed non-fatally', { error: err?.message });
      }
      try {
        await this.refillManifestProvenP1Imports();
      } catch (err: any) {
        this.logger.warn('Boot refillManifestProvenP1Imports failed non-fatally', { error: err?.message });
      }
      try {
        await this.refreshP1FocusGroupNow(true);
      } catch (err: any) {
        this.logger.warn('Boot refreshP1FocusGroupNow failed non-fatally', { error: err?.message });
      }
      void Promise.all([this.runHistoricalP1CatalogTurn(), this.runHistoricalP1CatalogTurn()]).catch(() => {});
      try {
        await this.refillStoredMetadataEnrichment();
      } catch (err: any) {
        this.logger.warn('Boot refillStoredMetadataEnrichment failed non-fatally', { error: err?.message });
      }
      try {
        await this.refillSourceMetadataEnrichment();
      } catch (err: any) {
        this.logger.warn('Boot refillSourceMetadataEnrichment failed non-fatally', { error: err?.message });
      }
      try {
        await this.refillCrossSourceMetadataDiscovery();
      } catch (err: any) {
        this.logger.warn('Boot refillCrossSourceMetadataDiscovery failed non-fatally', { error: err?.message });
      }
      try {
        await Promise.all([this.runSourceMetadataRefresh(), this.runSourceMetadataRefresh()]);
      } catch (err: any) {
        this.logger.warn('Boot runSourceMetadataRefresh failed non-fatally', { error: err?.message });
      }
      try {
        await Promise.all([this.runCrossSourceMetadataDiscovery(), this.runCrossSourceMetadataDiscovery()]);
      } catch (err: any) {
        this.logger.warn('Boot runCrossSourceMetadataDiscovery failed non-fatally', { error: err?.message });
      }
      try {
        await this.runNewWorkCatalog();
      } catch (err: any) {
        this.logger.warn('Boot runNewWorkCatalog failed non-fatally', { error: err?.message });
      }
      try {
        await this.runStoredMetadataReplay();
      } catch (err: any) {
        this.logger.warn('Boot runStoredMetadataReplay failed non-fatally', { error: err?.message });
      }
    }
  }

  private async assertReaderCanonicalIdentitySchema(): Promise<void> {
    const required = ['canonical_chapter_key', 'display_number', 'chapter_order_key', 'chapter_kind', 'edition', 'language'];
    const schemaPool = this.sourceMetadataPool;
    const columns = await schemaPool.query<{ column_name: string; is_nullable: string }>(
      `SELECT column_name,is_nullable
       FROM information_schema.columns
       WHERE table_schema='public' AND table_name='chapters'
         AND column_name IN ('canonical_chapter_key','display_number','chapter_order_key','chapter_kind','edition','language','number')`,
    );
    const present = new Map(columns.rows.map((row) => [row.column_name, row.is_nullable]));
    const missing = required.filter((column) => !present.has(column));
    const numberNullable = present.get('number') === 'YES';
    const canonicalIndex = await schemaPool.query<{ exists: boolean }>(
      `SELECT EXISTS(
         SELECT 1 FROM pg_indexes
         WHERE schemaname='public' AND tablename='chapters'
           AND indexname='chapters_work_canonical_chapter_key_uq'
       ) AS exists`,
    );
    const numericConstraint = await schemaPool.query<{ exists: boolean }>(
      `SELECT EXISTS(
         SELECT 1 FROM pg_indexes
         WHERE schemaname='public' AND tablename='chapters'
           AND indexname='chapters_work_id_number_key'
       ) AS exists`,
    );
    if (missing.length || !numberNullable || canonicalIndex.rows[0]?.exists !== true || numericConstraint.rows[0]?.exists === true) {
      const detail = [
        missing.length ? `missing=${missing.join(',')}` : null,
        !numberNullable ? 'number_is_still_required' : null,
        canonicalIndex.rows[0]?.exists !== true ? 'canonical_unique_index_missing' : null,
        numericConstraint.rows[0]?.exists === true ? 'legacy_numeric_uniqueness_active' : null,
      ].filter(Boolean).join(';');
      throw new Error(`READER_CANONICAL_IDENTITY_MIGRATION_REQUIRED:${detail}`);
    }
  }

  stop(): void {
    this.stopped = true;
    if (this.metadataTurnTimer) clearTimeout(this.metadataTurnTimer);
    this.metadataTurnTimer = null;
    for (const timer of this.timers) clearInterval(timer);
    this.timers = [];
    void this.releaseRuntimeLeadership();
    if (this.sourceMetadataPool !== this.pool) void this.sourceMetadataPool.end().catch(() => {});
  }

  async shutdown(): Promise<void> {
    this.stopped = true;
    if (this.metadataTurnTimer) clearTimeout(this.metadataTurnTimer);
    this.metadataTurnTimer = null;
    for (const timer of this.timers) clearInterval(timer);
    this.timers = [];
    await Promise.race([
      this.releaseRuntimeLeadership(),
      sleep(3_000),
    ]);
    if (this.sourceMetadataPool !== this.pool) {
      await Promise.race([
        this.sourceMetadataPool.end().catch(() => {}),
        sleep(3_000),
      ]);
    }
  }

  /**
   * The normal importer retains its two-second metadata cadence. During the
   * explicit V4 campaign the durable one-second admission fence must not be
   * hidden behind that slower timer: a completed local merge would otherwise
   * leave all exclusive seats idle for up to another second. Schedule the
   * successor before starting the bounded turn, so a local merge does not
   * stretch the one-second cadence; the replay and producer gates fence any
   * overlap.
   */
  private scheduleMetadataTurn(): void {
    if (this.stopped) return;
    const chapterBusy = this.chapterAdmissionGate.active > 0
      || (this.pool.waitingCount || 0) > 0
      || (this.sourceMetadataPool.waitingCount || 0) > 0;
    const delayMs = this.metadataTurboMode.enabled
      ? 250
      : (this.metadataReplayV4Exclusive.enabled
        ? Math.max(250, Math.min(STORED_METADATA_REPLAY_INTERVAL_MS, this.metadataReplayV4Exclusive.intervalMs))
        : (chapterBusy ? 10_000 : STORED_METADATA_REPLAY_INTERVAL_MS));
    this.metadataTurnTimer = setTimeout(() => {
      this.scheduleMetadataTurn();
      if (this.isRuntimeLeader) void this.runMetadataTurn();
    }, delayMs);
    this.metadataTurnTimer.unref();
  }

  /**
   * The shared settings lease is the only cross-runtime coordination point.
   * It gates admission only: workers which already hold a task lease may
   * finish their durable write while a new STAFF request or a successor
   * runtime waits to claim new work.
   */
  private async tryAcquireRuntimeLeadership(): Promise<boolean> {
    try {
      const result = await this.pool.query(
        `INSERT INTO settings AS leader (key,value)
         VALUES($1,json_build_object('owner',$2::text,'started_at',$4::timestamptz,
           'expires_at',now()+($3::int * interval '1 second'))::text)
         ON CONFLICT(key) DO UPDATE SET value=json_build_object('owner',$2::text,
           'started_at',$4::timestamptz,'expires_at',now()+($3::int * interval '1 second'))::text
         WHERE (leader.value::jsonb->>'owner')=$2::text
            OR COALESCE((leader.value::jsonb->>'expires_at')::timestamptz,'epoch'::timestamptz)<=now()
         RETURNING value`,
        [RUNTIME_LEADER_KEY, this.runtimeInstanceId, RUNTIME_LEASE_SECONDS, this.runtimeBootedAt.toISOString()],
      );
      const acquired = result.rows.length > 0;
      if (acquired && !this.isRuntimeLeader) {
        this.isRuntimeLeader = true;
        this.logger.info('Acquired Lite runtime leadership', { instanceId: this.runtimeInstanceId, leaseSeconds: RUNTIME_LEASE_SECONDS });
      }
      return acquired;
    } catch (error: any) {
      this.logger.warn('Failed to acquire Lite runtime leadership', { error: error?.message });
      return false;
    }
  }

  private startRuntimeLeadershipRenewal(): void {
    if (this.runtimeLeadershipTimer) return;
    this.runtimeLeadershipTimer = setInterval(async () => {
      if (this.runtimeLeadershipRenewing || this.stopped) return;
      if (!this.isRuntimeLeader) {
        await this.tryAcquireRuntimeLeadership();
        return;
      }
      this.runtimeLeadershipRenewing = true;
      this.leaseRenewStartedAt = new Date().toISOString();
      try {
        const result = await this.pool.query(
          `UPDATE settings SET value=json_build_object('owner',$2::text,'started_at',$4::timestamptz,
             'expires_at',now()+($3::int * interval '1 second'))::text
           WHERE key=$1 AND (value::jsonb->>'owner')=$2::text RETURNING value`,
          [RUNTIME_LEADER_KEY, this.runtimeInstanceId, RUNTIME_LEASE_SECONDS, this.runtimeBootedAt.toISOString()],
        );
        if (result.rows.length === 0) {
          this.isRuntimeLeader = false;
          this.logger.error('Lost Lite runtime leadership; new claims are gated.', { instanceId: this.runtimeInstanceId });
        }
        this.leaseRenewCompletedAt = new Date().toISOString();
        this.leaseRenewError = null;
      } catch (error: any) {
        // The durable lease remains valid for its remaining window. Retrying
        // renewal is safer than treating a transient YSQL fault as permission
        // for another runtime to publish immediately.
        this.logger.warn('Lite runtime leadership renewal failed', { error: error?.message });
        this.leaseRenewCompletedAt = new Date().toISOString();
        this.leaseRenewError = String(error?.message || 'UNKNOWN').slice(0, 160);
      } finally {
        this.runtimeLeadershipRenewing = false;
      }
    }, RUNTIME_LEASE_RENEW_MS);
    this.runtimeLeadershipTimer.unref();
  }

  private async waitForRuntimeLeadership(): Promise<boolean> {
    let lastStandbyLog = 0;
    while (!this.stopped) {
      if (await this.tryAcquireRuntimeLeadership()) {
        this.startRuntimeLeadershipRenewal();
        return true;
      }
      if (Date.now() - lastStandbyLog >= 30_000) {
        this.logger.warn('Lite is on standby; another importer owns the runtime lease.');
        lastStandbyLog = Date.now();
      }
      await sleep(5_000);
    }
    return false;
  }

  private async releaseRuntimeLeadership(): Promise<void> {
    if (this.runtimeLeadershipTimer) {
      clearInterval(this.runtimeLeadershipTimer);
      this.runtimeLeadershipTimer = null;
    }
    if (!this.isRuntimeLeader) return;
    this.isRuntimeLeader = false;
    try {
      await this.pool.query(
        `UPDATE settings SET value=json_build_object('owner',$2::text,'expires_at',now())::text
         WHERE key=$1 AND (value::jsonb->>'owner')=$2::text`,
        [RUNTIME_LEADER_KEY, this.runtimeInstanceId],
      );
    } catch (error: any) {
      this.logger.warn('Failed to release Lite runtime leadership', { error: error?.message });
    }
  }

  private async seedDiscovery(): Promise<{ sources: number; inserted: number; failed: number }> {
    const attemptedAt = new Date().toISOString();
    let sources: { rows: Array<{ id: string; sync_interval_minutes: number }> };
    try {
      sources = await this.pool.query<{ id: string; sync_interval_minutes: number }>(`SELECT id,sync_interval_minutes FROM importer_sources
        WHERE enabled=true AND (
          COALESCE(status,'ACTIVE') IN ('ACTIVE','DEGRADED','RECOVERING') OR
          (status='COOLDOWN' AND cooldown_until IS NOT NULL AND cooldown_until<=now())
        )
          AND COALESCE(catalog_discovery_enabled,true)
        ORDER BY id`);
    } catch (error: any) {
      const message = String(error?.message || 'DISCOVERY_SEED_SOURCE_READ_FAILED').slice(0, 240);
      this.discoverySeed = { attemptedAt, completedAt: new Date().toISOString(), sources: 0, inserted: 0, failed: 1, error: message };
      this.nextDiscoverySeedRetryAt = Date.now() + 30_000;
      this.logger.warn('Discovery seed source read failed; startup continues and retry is scheduled', { error: message });
      return { sources: 0, inserted: 0, failed: 1 };
    }
    const firstRun = Date.now();
    let inserted = 0;
    let failed = 0;
    let lastError: string | null = null;
    for (const [index, { id, sync_interval_minutes }] of sources.rows.entries()) {
      try {
        const result = await this.queue.enqueue({ taskType: 'DISCOVER', workKey: `source:${id}`, priority: 'P2',
          idempotencyKey: `lite:discover:${id}`, payload: { source: id, syncIntervalMinutes: sync_interval_minutes },
          checkpoint: { cursor: null }, nextRunAt: new Date(firstRun + index * 60_000), skipReadyWorkRefresh: true });
        if (result.inserted) inserted++;
      } catch (error: any) {
        failed++;
        lastError = String(error?.message || 'DISCOVERY_SEED_ENQUEUE_FAILED').slice(0, 240);
        // An enqueue transaction is all-or-nothing and keyed by source, so
        // preserving the other sources and retrying this durable idempotent
        // key cannot duplicate discovery work or weaken queue integrity.
        this.logger.warn('Discovery seed enqueue failed; source will retry after startup', { source: id, error: lastError });
      }
    }
    this.discoverySeed = { attemptedAt, completedAt: new Date().toISOString(), sources: sources.rows.length, inserted, failed, error: lastError };
    this.nextDiscoverySeedRetryAt = failed > 0 ? Date.now() + 30_000 : 0;
    return { sources: sources.rows.length, inserted, failed };
  }

  /**
   * One periodic, bounded P1 reserve refill.  It runs after reconciliation
   * and never in the claim path, so legacy mappings cannot turn into a new
   * global scheduler scan.  A healthy reserve keeps only a tiny, deferred
   * source-diverse producer set; low reserve checks retain fair P1 admission
   * so a giant work cannot hide missing manifests indefinitely.
   */
  private async refillHistoricalP1Catalog(): Promise<void> {
    if (!this.isRuntimeLeader || !this.historicalP1BackfillGate.tryAcquire()) return;
    try {
      const now = Date.now();
      const producerPool = this.sourceMetadataPool !== this.pool ? this.sourceMetadataPool : this.pool;
      if ((producerPool.waitingCount || 0) > 1 && now - this.lastHistoricalP1ProducerAttemptAt < HISTORICAL_P1_BUSY_MAX_GAP_MS) {
        this.historicalP1Producer = {
          observedAt: new Date(now).toISOString(),
          admitted: 0,
          promoted: false,
          error: 'DEFERRED_FOR_LIVE_YSQL',
        };
        return;
      }
      this.lastHistoricalP1ProducerAttemptAt = now;
      const admitted = await this.queue.refillHistoricalP1Catalog();
      // One turn per already-bounded recovery interval lets an alternative
      // healthy source materialize without returning to a continuously
      // competing catalog lane. Even with an abundant materialised reserve,
      // this promotes only one previously-deferred P1 producer check; it does
      // not change STAFF/P0/P1/P2 ordering or chapter admission capacity.
      const promoted = await this.queue.promoteDeferredHistoricalP1Catalog(this.executor.getFairSourceDemand());
      this.historicalP1Producer = {
        observedAt: new Date().toISOString(),
        admitted,
        promoted,
        error: null,
      };
      if (admitted || promoted) {
        this.idleHistoricalP1CatalogUntil = 0;
        this.logger.info('Advanced bounded historical P1 catalog producer', { admitted, promoted });
      }
    } catch (error: any) {
      this.historicalP1Producer = {
        observedAt: new Date().toISOString(),
        admitted: 0,
        promoted: false,
        error: String(error?.message || 'UNKNOWN').slice(0, 240),
      };
      this.logger.warn('Historical P1 catalog refill failed', { error: error?.message });
    } finally {
      this.historicalP1BackfillGate.release();
    }
  }

  /**
   * Make P0 monitoring coverage a durable background control plane instead
   * of an accidental side effect of P2's first catalog pass.  It runs on the
   * isolated metadata pool, produces only a bounded watch reserve, and never
   * consumes a chapter admission or page/download permit.
   */
  private async refillP0MonitorCoverage(): Promise<void> {
    if (!this.isRuntimeLeader || this.stopped || this.manualProtectiveStop.active) return;
    // Coverage enrollment runs dynamically: while works are being enrolled,
    // progress rapidly in bounded batches. In steady-state with all sources covered,
    // it relaxes to a two-minute cadence to preserve SQL headroom.
    const minInterval = (this.p0CoverageZeroStreak >= 43) ? 120_000 : 5_000;
    if (Date.now() - this.lastP0CoverageRefillAt < minInterval) return;
    if (!this.p0MonitorCoverageRefillGate.tryAcquire()) return;
    this.lastP0CoverageRefillAt = Date.now();
    try {
      const refill = await this.queue.refillP0MonitorCoverage(128);
      this.p0MonitorCoverage = {
        observedAt: new Date().toISOString(),
        source: refill.source,
        admitted: refill.admitted,
        revived: refill.revived,
        outcome: refill.source ? (refill.admitted || refill.revived ? 'ENROLLED' : 'SOURCE_ALREADY_COVERED') : 'NO_RUNNABLE_SOURCE',
        error: null,
      };
      if (refill.admitted || refill.revived) {
        this.p0CoverageZeroStreak = 0;
        this.logger.info('Expanded durable P0 monitor coverage', refill);
        if (!this.stopped && this.isRuntimeLeader && !(process.env.NODE_ENV === 'test' || process.env.VITEST)) {
          const nextRefill = setTimeout(() => void this.refillP0MonitorCoverage().catch(() => {}), 1_000);
          nextRefill.unref();
        }
      } else if (refill.source) {
        this.p0CoverageZeroStreak += 1;
      }
    } catch (error: any) {
      this.p0MonitorCoverage = {
        observedAt: new Date().toISOString(), source: null, admitted: 0, revived: 0,
        outcome: 'ERROR', error: String(error?.message || 'UNKNOWN').slice(0, 240),
      };
      // Coverage is a bounded producer; a transient YSQL timeout must not
      // interfere with the monitor lane that already serves enrolled works.
      this.logger.warn('P0 monitor coverage refill deferred', { error: error?.message });
    } finally {
      this.p0MonitorCoverageRefillGate.release();
    }
  }

  private async refreshP1FocusGroupNow(force = false): Promise<void> {
    if (!this.isRuntimeLeader || this.stopped || this.manualProtectiveStop.active) return;
    if (typeof (this.queue as any).refreshP1FocusGroup !== 'function') return;
    const now = Date.now();
    const hasWarmFocusGroup = typeof (this.queue as any).getP1FocusWorkKeys === 'function'
      && this.queue.getP1FocusWorkKeys().length >= 12;
    const minFocusRefreshIntervalMs = hasWarmFocusGroup ? 60_000 : 45_000;
    if (!force && now - this.lastP1FocusRefreshAt < minFocusRefreshIntervalMs) return;
    if (!force && (this.sourceMetadataPool.waitingCount || 0) > 1 && typeof (this.queue as any).getP1FocusWorkKeys === 'function' && this.queue.getP1FocusWorkKeys().length >= 8) {
      return;
    }
    if (this.p1FocusRefreshInFlight) return this.p1FocusRefreshInFlight;
    this.lastP1FocusRefreshAt = now;
    const effectiveCap = Math.max(12, this.executor.getEffectiveChapterAdmissionCap());
    const targetSize = Math.max(24, Math.min(36, Math.round(effectiveCap * 1.6)));
    const sourceTiers = this.sourcePerformance.getTierMap([
      ...this.executor.getFairSourceDemand(),
      'nexus', 'taimumangas', 'tankouhentai', 'pizzariascan', 'nocturnesummer',
      'mangaflix', 'mangotoons', 'hipercool', 'manhastro',
    ]);
    this.p1FocusRefreshInFlight = this.queue.refreshP1FocusGroup({ targetSize, maxPerSource: 8, sourceTiers })
      .then((snap) => {
        if (snap?.focusSources && typeof (this.executor as any).mergeFairSourceDemandCounts === 'function') {
          (this.executor as any).mergeFairSourceDemandCounts(snap.focusSources);
        }
      })
      .catch((error: any) => {
        this.logger.warn('P1 focus group refresh deferred', { error: error?.message });
      })
      .finally(() => {
        this.p1FocusRefreshInFlight = null;
      });
    return this.p1FocusRefreshInFlight;
  }

  private async runHistoricalP1CatalogTurn(): Promise<boolean> {
    const producerPool = this.sourceMetadataPool !== this.pool ? this.sourceMetadataPool : this.pool;
    if (this.stopped || !this.isRuntimeLeader
      || Date.now() < this.idleHistoricalP1CatalogUntil
      || (producerPool.waitingCount || 0) > 1
      || (this.pool.waitingCount || 0) > 3
      || this.manualProtectiveStop.active
      || this.metadataReplayV4Exclusive.enabled
      || this.metadataTurboMode.enabled
      || this.shouldPauseChapterAdmission()
      || this.shouldPauseLowPriorityMetadata()) return false;
    if (typeof (this.queue as any).claimHistoricalP1Catalog !== 'function') return false;
    if (!this.historicalP1CatalogExecGate.tryAcquire()) return false;
    let claimed = false;
    try {
      const workerId = `${this.runtimeInstanceId}:p1-catalog`;
      const task = await this.historicalP1CatalogClaimGate.runExclusive(() =>
        this.queue.claimHistoricalP1Catalog(workerId, CATALOG_TASK_LEASE_SECONDS),
      );
      if (!task) {
        this.idleHistoricalP1CatalogUntil = Date.now() + 30_000;
        return false;
      }
      this.idleHistoricalP1CatalogUntil = 0;
      claimed = true;
      await this.withCatalogLeaseHeartbeat(task, workerId, () => this.catalog(task, workerId));
    } catch (error: any) {
      this.logger.warn('Historical P1 catalog lane failed', { error: error?.message });
    } finally {
      this.historicalP1CatalogExecGate.release();
      if (claimed && !this.stopped && this.isRuntimeLeader) {
        void this.refreshSourceFairness(true);
        void this.refreshP1FocusGroupNow();
        const timer = setTimeout(() => void this.runHistoricalP1CatalogTurn(), 500);
        timer.unref();
      }
    }
    return claimed;
  }

  private async runRecoveryTurn(): Promise<void> {
    await this.refreshMetadataReplayV4ExclusiveMode();
    await this.refreshMetadataTurboMode();
    // P0 discovery is not P2 metadata work.  Even an operator-selected
    // metadata campaign must not silently freeze the enrolment of existing
    // ongoing works; this tiny producer still yields under real pool pressure
    // and respects the durable manual protective stop.
    // Never delay lease recovery or P1/P2 while the single-flight coverage
    // producer waits for the metadata pool.
    void this.refillP0MonitorCoverage();
    if (this.metadataTurboMode.enabled) {
      // Turbo mode dedicates all worker and database resources to V5 and Global Discovery.
      // Do not run P1 catalog refills, P1 manifests, or legacy replay.
      return;
    }
    if (this.metadataReplayV4Exclusive.enabled) {
      // The exclusive campaign must not periodically re-admit chapter,
      // discovery, monitor, or source-refetch work. V4 reclaim is performed
      // by its own runner, so this still recovers an expired local lease.
      await Promise.all([this.runStoredMetadataReplay(), this.runStoredMetadataReplay(), this.runStoredMetadataReplay(), this.runStoredMetadataReplay()]);
      void this.refillStoredMetadataEnrichment();
      return;
    }
    const now = Date.now();
    // Unknown-upload repair is a cold historical proof path. It may need to
    // examine a large legacy UNKNOWN set, whereas a live chapter already has
    // its own receipt/intent fence and never depends on this sweep to finish.
    // Do not let that exceptional scan occupy the primary YSQL seats during a
    // productive wave. Leaving the timestamp untouched makes the next quiet
    // recovery turn run it automatically; it is deferred, not abandoned.
    const chapterOrPoolBusy = this.chapterAdmissionGate.active > 0 || this.pool.waitingCount > 0;
    let recoverUploadOutcomes = false;
    let newestUnknownUploadAt = 0;
    if (!chapterOrPoolBusy && now - this.lastUploadOutcomeRecoveryAt >= UPLOAD_OUTCOME_RECOVERY_INTERVAL_MS) {
      this.lastUploadOutcomeRecoveryAt = now;
      try {
        const newest = await this.queue.newestUnknownUploadAt();
        newestUnknownUploadAt = newest?.getTime() || 0;
        recoverUploadOutcomes = newestUnknownUploadAt > this.lastUnknownUploadRecoverySeenAt;
      } catch {
        // Keep the existing fail-safe proof path if the cheap watermark read
        // is unavailable. It is rare and never changes receipt semantics.
        recoverUploadOutcomes = true;
      }
    }
    // Lease, publication and unknown-upload repair must remain live during a
    // busy P1 wave. The SOURCE_RATE_LIMITED scan can inspect thousands of
    // deferred rows, so running it on every recovery turn whenever the pool
    // happens to have no waiter still steals a primary YSQL seat from live
    // publication. Re-admit quickly while idle, but rate-limit it during an
    // active wave. The bounded max-gap keeps deferred sources progressing
    // without permanent starvation under sustained chapter traffic.
    const sourceCooldownIntervalMs = chapterOrPoolBusy
      ? SOURCE_COOLDOWN_RECOVERY_MAX_GAP_MS
      : SOURCE_COOLDOWN_RECOVERY_IDLE_INTERVAL_MS;
    let recoverSourceCooldowns = now - this.lastSourceCooldownRecoveryAt >= sourceCooldownIntervalMs;
    if (recoverSourceCooldowns) {
      this.lastSourceCooldownRecoveryAt = now;
      if (typeof (this.queue as any).hasDueSourceCooldowns === 'function') {
        try {
          recoverSourceCooldowns = await this.queue.hasDueSourceCooldowns();
        } catch {
          recoverSourceCooldowns = false;
        }
      }
    }
    const coldMaintenanceIntervalMs = chapterOrPoolBusy ? 20 * 60_000 : 10 * 60_000;
    const recoverLegacyMaintenance = now - this.lastColdReconcileMaintenanceAt >= coldMaintenanceIntervalMs;
    if (recoverLegacyMaintenance) this.lastColdReconcileMaintenanceAt = now;
    try {
      const recovery = await this.queue.reconcile(50, { recoverUploadOutcomes, recoverSourceCooldowns, recoverLegacyMaintenance });
      if (recoverUploadOutcomes && Number(recovery.uploadReceiptRecoveries || 0) === 0 && newestUnknownUploadAt > 0) {
        this.lastUnknownUploadRecoverySeenAt = newestUnknownUploadAt;
      }
    } catch { /* bounded recovery retries next turn */ }
    try { await this.probeDueSourceRecovery(); } catch (error: any) {
      this.logger.warn('Source cooldown recovery probe failed', { error: error?.message });
    }
    if (!chapterOrPoolBusy && now - this.lastLegacyBlockedRepairAt >= 30 * 60_000) {
      this.lastLegacyBlockedRepairAt = now;
      try { await this.queue.recoverDuplicateCanonicalCatalogFailures(); } catch { /* bounded recovery retries next turn */ }
      try {
        if (typeof (this.queue as any).reopenP0MonitorsBlockedByDiscoveryPolicy === 'function') {
          await this.queue.reopenP0MonitorsBlockedByDiscoveryPolicy();
        }
      } catch { /* bounded recovery retries next turn */ }
    }
    if (this.nextDiscoverySeedRetryAt > 0 && now >= this.nextDiscoverySeedRetryAt && this.pool.waitingCount === 0) {
      await this.seedDiscovery();
    }
    await this.refreshSourceFairness();
    await this.refillHistoricalP1Catalog();
    await this.refillManifestProvenP1Imports();
    await this.refreshP1FocusGroupNow();
    void this.runHistoricalP1CatalogTurn();
    await this.refillStoredMetadataEnrichment();
    // Metadata replay has its own bounded, local lane.  Feeding its durable
    // task is insufficient while P1 remains busy: execute one saved-mapping
    // merge on each eligible recovery turn as well.
    await this.runStoredMetadataReplay();
  }

  /**
   * Probe one expired source cooldown per recovery turn using the same
   * six-stage production admission gate as the legacy runtime. It never
   * bypasses a challenge or rate limit: failures remain parked with a new
   * durable cooldown, while a full PASS explicitly releases the source.
   */
  private async probeDueSourceRecovery(): Promise<void> {
    if (!this.isRuntimeLeader || Date.now() - this.lastSourceRecoveryProbeAt < SOURCE_RECOVERY_PROBE_INTERVAL_MS) return;
    this.lastSourceRecoveryProbeAt = Date.now();
    const selected = await this.pool.query<{ id: string; name: string; status: string; base_url: string }>(
      `SELECT s.id,s.name,s.status,s.base_url
       FROM importer_sources s
       LEFT JOIN importer_lite_source_health health ON health.source=s.id
       WHERE s.enabled=true AND s.status IN ('ACTIVE','COOLDOWN','DEGRADED','PROBING')
         AND ((s.status='ACTIVE' AND s.blocked_reason IS NOT NULL)
           OR (s.status IN ('COOLDOWN','DEGRADED','PROBING')
             AND (s.cooldown_until IS NULL OR s.cooldown_until<=now())))
         AND COALESCE(health.health,'HEALTHY') NOT IN ('BROKEN','DISABLED')
         AND (health.cooldown_until IS NULL OR health.cooldown_until<=now())
         AND (s.last_health_check_at IS NULL OR s.last_health_check_at<=now()-interval '5 minutes')
       ORDER BY s.last_health_check_at NULLS FIRST,s.id LIMIT 1`,
    );
    const source = selected.rows[0];
    if (!source) return;
    const adapter = this.registry.get(source.id);
    if (!adapter) {
      await this.pool.query(`UPDATE importer_sources SET status='COOLDOWN',cooldown_until=now()+interval '6 hours',
        blocked_reason='SOURCE_ADAPTER_MISSING',last_health_check_at=now(),updated_at=now() WHERE id=$1`, [source.id]);
      return;
    }
    const claimed = await this.pool.query<{ id: string }>(`UPDATE importer_sources
      SET status='PROBING',last_health_check_at=now(),updated_at=now()
      WHERE id=$1 AND enabled=true AND status IN ('ACTIVE','COOLDOWN','DEGRADED','PROBING')
        AND (cooldown_until IS NULL OR cooldown_until<=now())
        AND (last_health_check_at IS NULL OR last_health_check_at<=now()-interval '5 minutes')
      RETURNING id`, [source.id]);
    if (claimed.rowCount !== 1) return;

    try {
      const report = await this.sourceAdmissionGate.executeProdProbe(adapter);
      const checkedAt = new Date().toISOString();
      const stages = report.stages.map((stage) => ({ stage: stage.stage, status: stage.status,
        httpStatus: stage.httpStatus ?? null, durationMs: stage.durationMs,
        classification: stage.classification ?? null }));
      if (report.overallStatus === 'PASS') {
        await this.pool.query(`UPDATE importer_sources SET status='ACTIVE',cooldown_until=NULL,
          blocked_reason=NULL,blocked_details=$2::jsonb,last_health_check_at=$3,updated_at=now() WHERE id=$1`,
        [source.id,JSON.stringify({ recovered_at: checkedAt,probe_success: true,stages }),checkedAt]);
        if (typeof (this.queue as any).invalidateSourcePolicyCache === 'function') {
          (this.queue as any).invalidateSourcePolicyCache(source.id);
        }
        await this.queue.recordSourceSuccess(source.id);
        this.logger.info('Source passed bounded recovery admission probe', { source: source.id });
        return;
      }
      const classification = report.classification || 'UNKNOWN_CLOUDFLARE';
      const longBlock = ['TURNSTILE','JS_CHALLENGE','DATACENTER_ASN_BLOCK','CLOUDFLARE_DATACENTER_BLOCK'].includes(classification);
      const limited = ['RATE_LIMITED','BURST_LIMITED'].includes(classification);
      const cooldownMs = longBlock ? 6 * 60 * 60_000 : limited ? 60 * 60_000 : 30 * 60_000;
      const nextProbe = new Date(Date.now() + cooldownMs).toISOString();
      const health = limited ? 'RATE_LIMITED' : 'DEGRADED';
      await this.pool.query(`UPDATE importer_sources SET status='COOLDOWN',cooldown_until=$2,
        blocked_reason=$3,blocked_details=$4::jsonb,last_health_check_at=$5,updated_at=now() WHERE id=$1`,
      [source.id,nextProbe,classification,JSON.stringify({ stages,cf_ray: report.cfRay,last_checked_at: checkedAt }),checkedAt]);
      await this.pool.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
        VALUES($1,$2,$3,1,$4) ON CONFLICT(source) DO UPDATE SET health=excluded.health,
        cooldown_until=excluded.cooldown_until,consecutive_failures=importer_lite_source_health.consecutive_failures+1,
        last_error=excluded.last_error,updated_at=now()`,
      [source.id,health,nextProbe,classification]);
      if (typeof (this.queue as any).invalidateSourcePolicyCache === 'function') {
        (this.queue as any).invalidateSourcePolicyCache(source.id);
      }
      this.logger.info('Source remains parked after bounded recovery probe', { source: source.id,classification,cooldownMs });
    } catch (error: any) {
      const checkedAt = new Date().toISOString();
      const nextProbe = new Date(Date.now() + 30 * 60_000).toISOString();
      await this.pool.query(`UPDATE importer_sources SET status='COOLDOWN',cooldown_until=$2,
        blocked_reason='SOURCE_RECOVERY_PROBE_ERROR',blocked_details=$3::jsonb,last_health_check_at=$4,updated_at=now() WHERE id=$1`,
      [source.id,nextProbe,JSON.stringify({ last_checked_at: checkedAt }),checkedAt]);
      await this.pool.query(`INSERT INTO importer_lite_source_health(source,health,cooldown_until,consecutive_failures,last_error)
        VALUES($1,'DEGRADED',$2,1,$3) ON CONFLICT(source) DO UPDATE SET health='DEGRADED',
        cooldown_until=excluded.cooldown_until,consecutive_failures=importer_lite_source_health.consecutive_failures+1,
        last_error=excluded.last_error,updated_at=now()`,
      [source.id,nextProbe,String(error?.message || 'SOURCE_RECOVERY_PROBE_ERROR').slice(0,300)]);
      if (typeof (this.queue as any).invalidateSourcePolicyCache === 'function') {
        (this.queue as any).invalidateSourcePolicyCache(source.id);
      }
    }
  }

  /** Existing positive manifests may outlive an older completed catalog task. */
  private async refillManifestProvenP1Imports(): Promise<void> {
    const interval = this.manifestP1CatchupActive
      ? MANIFEST_P1_CATCHUP_INTERVAL_MS
      : MANIFEST_P1_STEADY_INTERVAL_MS;
    if (!this.isRuntimeLeader || Date.now() - this.lastManifestP1RepairAt < interval) return;
    this.lastManifestP1RepairAt = Date.now();
    try {
      // This producer does not fetch a source or hold a transaction across
      // I/O. `materialize…` inserts at most two 128-row transactions here;
      // its task key is idempotent and it only reads positive manifest rows.
      // A full batch keeps catch-up active for another bounded turn.  The
      // first short batch below the cap proves the orphan reserve is empty and
      // returns us to the inexpensive steady-state sweep.
      const catchingUp = this.manifestP1CatchupActive;
      const admitted = catchingUp
        ? await this.queue.materializeConfirmedMissingP1Tasks(MANIFEST_P1_CATCHUP_BATCH)
        : await this.queue.refillManifestProvenP1Imports();
      if (catchingUp && admitted < MANIFEST_P1_CATCHUP_BATCH) this.manifestP1CatchupActive = false;
      if (admitted) this.logger.info('Materialized manifest-proven historical P1 repairs', {
        admitted,
        mode: catchingUp ? 'catchup' : 'steady',
      });
    } catch (error: any) {
      this.logger.warn('Manifest-proven P1 refill failed', { error: error?.message });
    }
  }

  /**
   * Historical aliases/tags already present in a source mapping are cheap,
   * useful evidence. Feed at most one P2 catalog task per second into the
   * bounded replay lane; normal chapter claims retain strict priority.
   */
  private async refillStoredMetadataEnrichment(): Promise<void> {
    // Outside the campaign, retain the fifteen-second producer throttle. In
    // exclusive mode a no-candidate probe advances only a bounded durable
    // cursor slice, so the two-second timer may immediately continue that
    // scan instead of leaving both local merge seats empty for fifteen
    // seconds.
    if (!this.isRuntimeLeader || this.shouldPauseLowPriorityMetadata()
      || (!this.metadataReplayV4Exclusive.enabled
        && Date.now() - this.lastMetadataEnrichmentAt < STORED_METADATA_ENRICHMENT_REFILL_INTERVAL_MS)) return;
    this.lastMetadataEnrichmentAt = Date.now();
    try {
      if (this.metadataReplayV4Exclusive.enabled) {
        const admittedV4 = await this.refillStoredMetadataEnrichmentV4();
        if (admittedV4) this.logger.info('Admitted exclusive V4 metadata enrichment', { admitted: admittedV4 });
        return;
      }
      // Legacy v2/v3 uses the chapter pool, so yield it under real pressure.
      // v4 uses the dedicated metadata seat and remains independently bounded.
      const admitted = this.pool.waitingCount >= 2 ? 0 : await this.queue.refillStoredMetadataEnrichment();
      // v4 is deliberately opt-in through a durable production setting. Its
      // single local producer must be allowed to wait behind an isolated P2
      // operation: otherwise the durable cursor is starved whenever that
      // one-seat pool has a waiter. The repository still permits only one
      // outstanding v4 task and applies its 20-second due-time fence.
      const admittedV4 = await this.refillStoredMetadataEnrichmentV4();
      if (admitted || admittedV4) this.logger.info('Admitted bounded stored metadata enrichment', {
        admitted: admitted + admittedV4, version: admittedV4 ? 4 : undefined,
      });
    } catch (error: any) {
      this.logger.warn('Stored metadata enrichment refill failed', { error: error?.message });
    }
  }

  /** Keep V4's isolated-seat ownership correct across boot, retries and direct successors. */
  private async refillStoredMetadataEnrichmentV4(): Promise<number> {
    if (!this.metadataReplayV4RefillGate.tryAcquire()) return 0;
    try {
    // V4 remains single-flight. With no chapter-pool queue, the bounded
    // five-second floor removes remaining idle time between local merges;
    // any waiter restores the established conservative twenty-second fence.
    const intervalMs = this.metadataReplayV4Exclusive.enabled
      ? this.metadataReplayV4Exclusive.intervalMs
      : Number(this.pool.waitingCount || 0) === 0
        ? STORED_METADATA_REPLAY_V4_IDLE_INTERVAL_MS
        : STORED_METADATA_REPLAY_V4_PRESSURED_INTERVAL_MS;
    const capacity = this.metadataReplayV4Exclusive.enabled ? 4 : 1;
    let admitted = 0;
    // The repository may fill one bounded, distinct-work source burst per
    // transaction. Repeat only until the exclusive capacity is full; every
    // call remains behind the single producer gate and preserves per-work
    // serialization.
    for (let slot = 0; slot < capacity; slot++) {
      const inserted = this.metadataReplayV4Exclusive.enabled
        ? await this.queue.refillStoredMetadataEnrichmentV4(
          capacity, intervalMs, STORED_METADATA_REPLAY_V4_EXCLUSIVE_SOURCE_WINDOWS_PER_TURN,
        )
        : await this.queue.refillStoredMetadataEnrichmentV4(capacity, intervalMs);
      admitted += inserted;
      if (!inserted) break;
    }
    if (admitted) {
      this.sourceMetadataRefreshDeferredForV4 = true;
      return admitted;
    }
    // A restart can find a V4 RETRY_WAIT task already outstanding, so no new
    // row is admitted even though the campaign still owns the isolated seat.
    // This same check releases optional source refreshes after the producer
    // has durably marked the campaign complete.
    this.sourceMetadataRefreshDeferredForV4 = await this.queue.isStoredMetadataReplayV4Enabled();
    return 0;
    } finally {
      this.metadataReplayV4RefillGate.release();
    }
  }

  /** Refresh the durable campaign switch without relying on process-local state. */
  private async refreshMetadataReplayV4ExclusiveMode(force = false): Promise<void> {
    if (!this.isRuntimeLeader) return;
    const now = Date.now();
    if (!force && now - this.lastMetadataReplayV4ExclusiveRefreshAt < STORED_METADATA_REPLAY_V4_EXCLUSIVE_REFRESH_MS) return;
    this.lastMetadataReplayV4ExclusiveRefreshAt = now;
    let next = await this.queue.storedMetadataReplayV4ExclusiveMode();
    if (next.enabled && !(await this.queue.isStoredMetadataReplayV4Enabled())) {
      await this.queue.disableStoredMetadataReplayV4ExclusiveMode();
      next = { enabled: false, intervalMs: STORED_METADATA_REPLAY_V4_EXCLUSIVE_DEFAULT_INTERVAL_MS };
      this.logger.info('Exclusive V4 metadata mode completed; normal importer admission restored');
    }
    if (next.enabled !== this.metadataReplayV4Exclusive.enabled || next.intervalMs !== this.metadataReplayV4Exclusive.intervalMs) {
      this.logger.warn('Updated exclusive V4 metadata mode', next);
    }
    this.metadataReplayV4Exclusive = next;
    this.sourceMetadataRefreshDeferredForV4 = next.enabled
      || ((force || this.sourceMetadataRefreshDeferredForV4) ? await this.queue.isStoredMetadataReplayV4Enabled() : false);
  }

  /** Refresh the durable turbo campaign switch without relying on process-local state. */
  private async refreshMetadataTurboMode(force = false): Promise<void> {
    if (!this.isRuntimeLeader) return;
    const now = Date.now();
    if (!force && now - this.lastMetadataTurboModeRefreshAt < 30_000) return;
    this.lastMetadataTurboModeRefreshAt = now;
    const next = await this.queue.metadataTurboExclusiveMode();
    const hasConfigChanged = force
      || next.enabled !== this.metadataTurboMode.enabled
      || next.discoveryConcurrency !== this.metadataTurboMode.discoveryConcurrency
      || next.searchConcurrency !== this.metadataTurboMode.searchConcurrency
      || next.metadataGateCapacity !== this.metadataTurboMode.metadataGateCapacity
      || next.persistenceCapacity !== this.metadataTurboMode.persistenceCapacity;

    if (hasConfigChanged) {
      this.logger.warn('Updated metadata turbo mode', next);
      if (next.enabled) {
        this.sitePausedUntil = 0;
        this.lastSiteProbe = { state: 'HEALTHY', checkedAt: new Date().toISOString(), latencyMs: 0 };
      }
      const discoveryCap = next.enabled ? (next.discoveryConcurrency ?? 24) : 2;
      const searchCap = next.enabled ? (next.searchConcurrency ?? 128) : 4;
      const metadataCap = next.enabled ? (next.metadataGateCapacity ?? Math.max(discoveryCap + 8, 48)) : 3;
      const persistenceCap = next.enabled ? Math.min(next.persistenceCapacity ?? 4, 4) : 1;

      // Normal operation owns one physical P2 YSQL backend. An explicit
      // campaign may increase network fan-out, but not the normal lane.
      this.sourceMetadataLaneGate.setCapacity(next.enabled ? 6 : 1);
      this.sourceMetadataGate.setCapacity(searchCap);
      this.sourceMetadataPreparationGate.setCapacity(next.enabled ? 4 : 1);
      this.sourceMetadataPersistenceGate.setCapacity(persistenceCap);
      this.crossSourceDiscoveryLaneGate.setCapacity(discoveryCap);
      this.crossSourceMetadataGate.setCapacity(metadataCap);
    }
    this.metadataTurboMode = next;
  }

  /** One timer turn keeps normal metadata paths out of an exclusive replay. */
  private async runMetadataTurn(): Promise<void> {
    if (!this.metadataTurnGate.tryAcquire()) return;
    try {
      await this.refreshMetadataReplayV4ExclusiveMode();
      await this.refreshMetadataTurboMode();
      if (this.metadataTurboMode.enabled) {
        const concurrency = Math.max(16, this.metadataTurboMode.discoveryConcurrency ?? 24);
        const claimed = await Promise.all(
          Array.from({ length: concurrency }, () => this.runCrossSourceMetadataDiscovery())
        );
        const admitted = await this.refillCrossSourceMetadataDiscovery();
        void this.runAliasOnlyEnrichmentTurn();
        void this.runCoverVisionTurn();
        if (!claimed.some(Boolean) && !admitted) {
          await this.queue.disableMetadataTurboExclusiveMode();
          await this.refreshMetadataTurboMode(true);
          this.logger.info('Metadata turbo exclusive mode completed all due V7 tasks; normal metadata mode restored');
        }
      } else if (this.metadataReplayV4Exclusive.enabled) {
        await Promise.all([this.runStoredMetadataReplay(), this.runStoredMetadataReplay(), this.runStoredMetadataReplay(), this.runStoredMetadataReplay()]);
        // A source-cursor probe can take materially longer than a local
        // merge. Claim all already-due V4 rows first and let the gate-fenced
        // producer replenish in parallel; otherwise its serial scan leaves
        // newly READY mappings idle before the next claim turn.
        void this.refillStoredMetadataEnrichment();
      } else {
        if (this.sourceMetadataRefreshDeferredForV4) {
          await this.refillStoredMetadataEnrichment();
          await this.runStoredMetadataReplay();
        }
        await this.refillSourceMetadataEnrichment();
        await this.refillCrossSourceMetadataDiscovery();
        await this.runCrossSourceMetadataDiscovery();
        void this.runAliasOnlyEnrichmentTurn();
        void this.runCoverVisionTurn();
      }
    } catch (error: any) {
      this.logger.warn('Metadata turn failed', { error: error?.message });
    } finally {
      this.metadataTurnGate.release();
    }
  }

  public triggerAsyncAliasOnlyEnrichment(workId: string): void {
    if (!workId || this.stopped) return;
    this.pendingImmediateAliasWorkIds.add(workId);
    if (process.env.NODE_ENV !== 'test') {
      void this.runAliasOnlyEnrichmentTurn();
    }
  }

  /**
   * Permanent background enrichment across the 3 active ALIAS_ONLY sources
   * (MangaDex, MangaFire, MangaBall) for newly published works,
   * bounded transient retries, and lightweight incomplete-alias rechecks.
   * Never blocks chapter publication and updates ONLY `works.aliases`.
   */
  public async runAliasOnlyEnrichmentTurn(forceInTest = false): Promise<number> {
    if (this.stopped || (!forceInTest && process.env.NODE_ENV === 'test')) return 0;
    if (!forceInTest && !this.isRuntimeLeader) return 0;
    if (this.metadataReplayV4Exclusive.enabled || this.shouldPauseLowPriorityMetadata()) return 0;
    if (this.hasSustainedSourceMetadataPoolPressure()) return 0;

    const hasImmediate = this.pendingImmediateAliasWorkIds.size > 0;
    if (!forceInTest && !hasImmediate && Date.now() - this.lastAliasOnlyTurnAt < 20_000) {
      return 0;
    }

    if (!this.aliasOnlyGate.tryAcquire()) return 0;
    try {
      this.lastAliasOnlyTurnAt = Date.now();
      const immediateIds = [...this.pendingImmediateAliasWorkIds];
      const due = await this.aliasOnlyRepo.findDueWorksForRuntimeEnrichment(3, immediateIds);
      for (const item of due) {
        this.pendingImmediateAliasWorkIds.delete(item.target.id);
      }
      if (!due.length) {
        this.pendingImmediateAliasWorkIds.clear();
        this.aliasOnlyTelemetry.lastRunAt = new Date().toISOString();
        this.aliasOnlyTelemetry.lastOutcome = 'NO_DUE_WORKS';
        return 0;
      }

      const batchResults = [];
      for (const item of due) {
        if (this.stopped || this.shouldPauseLowPriorityMetadata()) break;
        const enriched = await this.aliasOnlyService.enrichWork(item.target, item.onlySources);
        batchResults.push({
          target: item.target,
          priorityBucket: item.priorityBucket,
          result: enriched,
        });
      }

      if (!batchResults.length) return 0;
      const persisted = await this.aliasOnlyRepo.persistBatchResults('runtime_permanent_v1', batchResults);
      this.aliasOnlyTelemetry.processedWorks += batchResults.length;
      this.aliasOnlyTelemetry.aliasesAdded += persisted.aliasesPersisted;
      this.aliasOnlyTelemetry.lastRunAt = new Date().toISOString();
      this.aliasOnlyTelemetry.lastOutcome = `PROCESSED_${batchResults.length}_ADDED_${persisted.aliasesPersisted}`;
      if (persisted.aliasesPersisted > 0) {
        this.logger.info('Enriched work aliases from ALIAS_ONLY sources', {
          processedWorks: batchResults.length,
          worksUpdated: persisted.worksUpdated,
          aliasesPersisted: persisted.aliasesPersisted,
          ptBrPersisted: persisted.ptBrPersisted,
        });
      }
      return batchResults.length;
    } catch (error: any) {
      this.aliasOnlyTelemetry.lastRunAt = new Date().toISOString();
      this.aliasOnlyTelemetry.lastOutcome = `ERROR:${error?.message || 'unknown'}`;
      this.logger.warn('ALIAS_ONLY background turn skipped due to error', { error: error?.message });
      return 0;
    } finally {
      this.aliasOnlyGate.release();
    }
  }

  public triggerAsyncCoverVision(workId: string): void {
    if (!workId || this.stopped) return;
    this.pendingImmediateCoverVisionWorkIds.add(workId);
    if (process.env.NODE_ENV !== 'test') {
      void this.runCoverVisionTurn();
    }
  }

  /**
   * Permanent background visual inspection of covers for PT-BR titles.
   * Concurrency is strictly bounded by coverVisionGate (capacity 1).
   * Fully decoupled from chapter download and publishing workers.
   */
  public async runCoverVisionTurn(forceInTest = false): Promise<number> {
    if (this.stopped || (!forceInTest && process.env.NODE_ENV === 'test')) return 0;
    if (!forceInTest && !this.isRuntimeLeader) return 0;
    if (this.metadataReplayV4Exclusive.enabled || this.shouldPauseLowPriorityMetadata()) return 0;
    if (this.hasSustainedSourceMetadataPoolPressure()) return 0;

    const hasImmediate = this.pendingImmediateCoverVisionWorkIds.size > 0;
    if (!forceInTest && !hasImmediate && Date.now() - this.lastCoverVisionTurnAt < 15_000) {
      return 0;
    }

    if (!this.coverVisionGate.tryAcquire()) return 0;
    try {
      this.lastCoverVisionTurnAt = Date.now();
      const tasks = await this.coverVisionQueue.findDueTasks(2);
      if (!tasks.length) {
        this.pendingImmediateCoverVisionWorkIds.clear();
        this.coverVisionTelemetry.lastRunAt = new Date().toISOString();
        this.coverVisionTelemetry.lastOutcome = 'NO_DUE_TASKS';
        return 0;
      }

      let promotedCount = 0;
      for (const task of tasks) {
        if (this.stopped || this.shouldPauseLowPriorityMetadata()) break;
        this.pendingImmediateCoverVisionWorkIds.delete(task.workId);

        await this.coverVisionQueue.markRunning(task.workId);

        const inspectedUrls = [...task.inspectedUrls];
        try {
          const visionPool = this.sourceMetadataPool || this.pool;
          const workRes = await visionPool.query<{
            id: string;
            title: string;
            slug: string;
            aliases: string[];
            metadata_provenance: any;
          }>(
            `SELECT id, title, slug, aliases, metadata_provenance FROM works WHERE id = $1::uuid LIMIT 1`,
            [task.workId],
          );
          const currentWork = workRes.rows[0];
          if (!currentWork) {
            await this.coverVisionQueue.markNoPtBrFound(task.workId, inspectedUrls);
            continue;
          }

          if (isPtBrTitle(currentWork.title) || currentWork.metadata_provenance?.title?.localized_ptbr === true) {
            await this.coverVisionQueue.markCompleted(task.workId, {
              detectedTitle: currentWork.title,
              attributingSource: currentWork.metadata_provenance?.title?.source || 'already_localized',
              confidence: 1.0,
              evidence: { reason: 'ALREADY_LOCALIZED_PTBR' },
              inspectedUrls,
            });
            continue;
          }

          let matched = false;

          for (const cand of task.candidateCovers) {
            if (this.stopped) break;
            if (inspectedUrls.includes(cand.coverUrl)) continue;
            inspectedUrls.push(cand.coverUrl);

            const candAdapter = this.registry.get(cand.source);
            let coverHeaders: Record<string, string> | undefined;
            try {
              if (candAdapter && typeof candAdapter.getImageHeaders === 'function') {
                coverHeaders = await candAdapter.getImageHeaders(cand.coverUrl);
              }
            } catch {
              coverHeaders = undefined;
            }

            const visionRes = await inspectCoverForPtBrTitle({
              workId: currentWork.id,
              slug: currentWork.slug,
              title: currentWork.title,
              aliases: currentWork.aliases || [],
              coverUrl: cand.coverUrl,
              headers: coverHeaders,
              source: cand.source,
            });

            if (visionRes.foundPtBrTitle && visionRes.detectedTitle) {
              const detectedTitle = visionRes.detectedTitle;
              const newAliases = mergeAlternativeTitles(
                (currentWork.aliases || []).filter(
                  (a) => alternativeTitleKey(a) !== alternativeTitleKey(detectedTitle),
                ),
                [currentWork.title],
                detectedTitle,
              );
              const updatedProv = {
                ...(currentWork.metadata_provenance || {}),
                title: {
                  source: cand.source,
                  method: 'PTBR_FOUND_COVER',
                  localized_ptbr: true,
                  previous_value: currentWork.title,
                  confidence: visionRes.confidence,
                  evidence: visionRes.evidence,
                  cover_url: cand.coverUrl,
                  updated_at: new Date().toISOString(),
                },
              };

              const updateRes = await visionPool.query(
                `UPDATE works
                 SET title = $1, aliases = $2, metadata_provenance = $3
                 WHERE id = $4::uuid
                   AND (metadata_provenance->'title'->>'localized_ptbr' IS NULL OR metadata_provenance->'title'->>'localized_ptbr' != 'true')`,
                [detectedTitle, newAliases, JSON.stringify(updatedProv), currentWork.id],
              );

              if ((updateRes.rowCount || 0) > 0) {
                this.logger.info('Cover Vision promoted title to PT-BR from cover artwork (async turn)', {
                  workId: currentWork.id,
                  previousTitle: currentWork.title,
                  promotedTitle: detectedTitle,
                  method: 'PTBR_FOUND_COVER',
                  source: cand.source,
                  confidence: visionRes.confidence,
                  coverUrl: cand.coverUrl,
                });
                promotedCount++;
              }

              await this.coverVisionQueue.markCompleted(task.workId, {
                detectedTitle,
                attributingSource: cand.source,
                confidence: visionRes.confidence,
                evidence: visionRes.evidence,
                inspectedUrls,
              });

              matched = true;
              break;
            }
          }

          if (!matched) {
            await this.coverVisionQueue.markNoPtBrFound(task.workId, inspectedUrls);
          }
        } catch (taskErr: any) {
          this.logger.warn('Error processing cover vision task', {
            workId: task.workId,
            error: taskErr?.message || taskErr,
          });
          await this.coverVisionQueue.markFailed(
            task.workId,
            taskErr?.message || String(taskErr),
            inspectedUrls,
            task.maxAttempts,
          );
        }
      }

      this.coverVisionTelemetry.processedWorks += tasks.length;
      this.coverVisionTelemetry.promotedTitles += promotedCount;
      this.coverVisionTelemetry.lastRunAt = new Date().toISOString();
      this.coverVisionTelemetry.lastOutcome = `PROCESSED_${tasks.length}_PROMOTED_${promotedCount}`;
      return promotedCount;
    } finally {
      this.coverVisionGate.release();
    }
  }

  /** Source-backed historical metadata remains a P2 catalog task and shares
   * normal priority claims; it never steals a chapter worker or bypasses a
   * STAFF/P0/P1 reserve. At most four network-only tasks are admitted across
   * sources; YSQL preparation and persistence remain gated below.
   */
  private async refillSourceMetadataEnrichment(
    replaceCompletedTask = false,
    emptyProbeTurn = 0,
    preferredSource?: string,
  ): Promise<void> {
    const minInterval = this.metadataTurboMode.enabled ? 2_000 : SOURCE_METADATA_REFILL_INTERVAL_MS;
    if (!this.isRuntimeLeader || this.metadataReplayV4Exclusive.enabled || this.metadataTurboMode.enabled
      || (!replaceCompletedTask && Date.now() - this.lastSourceMetadataRefillAt < minInterval)) return;
    if (!replaceCompletedTask && Date.now() < this.sourceMetadataNoCandidateUntil) {
      this.noteSourceMetadataProducer('NO_CANDIDATE_BACKOFF');
      return;
    }
    if (this.sourceMetadataRefreshDeferredForV4) {
      this.noteSourceMetadataProducer('DEFERRED_V4_BACKLOG');
      return;
    }
    // V5 uses its own direct YSQL pool. Main-pool claim waiters are therefore
    // not evidence that a metadata fetch would contend with a chapter write;
    // using them here left every external seat idle while workers merely
    // waited to claim. Only pressure on the isolated metadata pool throttles
    // its producer.
    const sustainedPoolPressure = this.hasSustainedSourceMetadataPoolPressure();
    // Under real metadata-pool contention retain exactly one bounded source
    // seat instead of zero or four.
    const constrainedPool = sustainedPoolPressure;
    if (!this.metadataTurboMode.enabled && (Date.now() < this.sitePausedUntil || this.lastSiteProbe.state === 'PAUSED')) {
      this.noteSourceMetadataProducer('SITE_PAUSED');
      return;
    }
    if (this.shouldPauseLowPriorityMetadata()) {
      this.noteSourceMetadataProducer('MEMORY_PAUSED');
      return;
    }
    if (!this.sourceMetadataRefillGate.tryAcquire()) {
      this.noteSourceMetadataProducer('REFILL_IN_FLIGHT');
      return;
    }
    this.lastSourceMetadataRefillAt = Date.now();
    let advanceEmptyHorizon = false;
    try {
      // A main-pool waiter is normal while chapter receipts or claims drain.
      // It is not evidence that this isolated source-metadata lane is under
      // pressure, and must not collapse its fetch capacity to one.
      // The persistence gate below still permits just one YSQL merge/commit.
      // Under a real local queue, keep one small source request progressing
      // instead of dropping the lane to zero. It has no YSQL connection while
      // fetching, its merge is still serialised below, and this avoids a
      // feedback loop where a paused metadata lane repeatedly builds READY
      // work that only wakes on the next timer tick.
      // Preserve the one active background fetch under pressure, but let the
      // isolated producer commit a bounded successor in the same transaction.
      // A YSQL cursor turn can take seconds while the public lane is busy;
      // without this one-item buffer, the worker sits idle between every
      // historical task even though no additional HTTP or merge is allowed.
      const capacity = !this.metadataTurboMode.enabled && constrainedPool ? SOURCE_METADATA_CONSTRAINED_PREFETCH_DEPTH : (this.metadataTurboMode.enabled ? 48 : 4);
      const probeLimit = !this.metadataTurboMode.enabled && constrainedPool ? SOURCE_METADATA_CONSTRAINED_PROBE_LIMIT : (this.metadataTurboMode.enabled ? 32 : undefined);
      // Preserve the ordinary two-argument call shape when no source was
      // recently productive. Besides keeping the path minimal, this makes
      // the preference opt-in rather than part of every producer tick.
      const admitted = preferredSource
        ? await this.queue.refillSourceMetadataEnrichment(capacity, probeLimit, preferredSource)
        : await this.queue.refillSourceMetadataEnrichment(capacity, probeLimit);
      this.noteSourceMetadataProducer(admitted ? 'ADMITTED' : 'NO_CANDIDATE', admitted);
      // Do not leave a healthy P2 seat idle for 15s merely because this
      // bounded source horizon found fewer candidates than its available
      // seats. The durable cursor advances transactionally; this bounded
      // continuation only examines the next small horizon and never raises
      // execution concurrency. In particular, a partially productive
      // horizon must not strand the remaining V5 slots.
      advanceEmptyHorizon = admitted < capacity && emptyProbeTurn < SOURCE_METADATA_EMPTY_PROBE_TURNS;
      if (admitted) this.sourceMetadataNoCandidateUntil = 0;
      else if (emptyProbeTurn >= SOURCE_METADATA_EMPTY_PROBE_TURNS) {
        this.sourceMetadataNoCandidateUntil = Date.now() + SOURCE_METADATA_NO_CANDIDATE_BACKOFF_MS;
      }
      if (admitted) {
        this.logger.info('Admitted source-backed historical metadata refresh', { admitted });
        // A producer tick that creates executable work must wake the dedicated
        // lane itself. Waiting for its 5 s safety timer needlessly leaves a
        // durable P2 task READY while no source request is in flight. The
        // runner still applies the same site/pool checks and bounded cap.
        void this.runSourceMetadataRefresh();
        if (this.metadataTurboMode.enabled) {
          void this.runSourceMetadataRefresh();
          void this.runSourceMetadataRefresh();
        }
      }
    } catch (error: any) {
      this.noteSourceMetadataProducer('ERROR', 0, error?.message);
      this.logger.warn('Source metadata refill failed', { error: error?.message });
    } finally {
      this.sourceMetadataRefillGate.release();
      if (advanceEmptyHorizon && this.isRuntimeLeader) {
        void Promise.resolve().then(() => this.refillSourceMetadataEnrichment(true, emptyProbeTurn + 1));
      }
    }
  }

  /** Materialise one durable cross-source coverage task only after V4 releases its exclusive lane. */
  private async refillCrossSourceMetadataDiscovery(): Promise<number> {
    const minInterval = this.metadataTurboMode.enabled ? 1_000 : CROSS_SOURCE_DISCOVERY_REFILL_INTERVAL_MS;
    if (!this.isRuntimeLeader || this.metadataReplayV4Exclusive.enabled || this.sourceMetadataRefreshDeferredForV4
      || (!this.metadataTurboMode.enabled && (Date.now() < this.sitePausedUntil || this.lastSiteProbe.state === 'PAUSED'))
      || this.shouldPauseLowPriorityMetadata()
      || Date.now() - this.lastCrossSourceDiscoveryRefillAt < minInterval) return 0;
    if (!this.crossSourceMetadataRefillGate.tryAcquire()) return 0;
    this.lastCrossSourceDiscoveryRefillAt = Date.now();
    try {
      const admitted = await this.queue.refillCrossSourceMetadataDiscovery(this.metadataTurboMode.enabled ? 32 : 2);
      if (admitted) this.logger.info('Admitted cross-source metadata coverage task', { admitted });
      return admitted;
    } catch (error: any) {
      this.logger.warn('Cross-source metadata coverage refill failed', { error: error?.message });
      return 0;
    } finally {
      this.crossSourceMetadataRefillGate.release();
    }
  }

  private noteSourceMetadataProducer(outcome: string, admitted = 0, error: unknown = null): void {
    this.sourceMetadataProducer = {
      observedAt: new Date().toISOString(), outcome, admitted: Math.max(0, admitted),
      error: error ? String(error).slice(0, 240) : null,
    };
  }

  /**
   * Keep at most a few consecutive selections on a proven productive source.
   * This is deliberately process-local: the durable cursor still guarantees
   * recovery and fairness after a restart, while a live runtime avoids an
   * expensive lap over exhausted providers between every useful refetch.
   */
  private nextPreferredSourceMetadataSource(source: unknown): string | undefined {
    const normalized = typeof source === 'string' ? source.trim() : '';
    if (!normalized) return undefined;
    if (this.sourceMetadataPreferredSource === normalized) {
      this.sourceMetadataPreferredSourceBurst += 1;
    } else {
      this.sourceMetadataPreferredSource = normalized;
      this.sourceMetadataPreferredSourceBurst = 1;
    }
    if (this.sourceMetadataPreferredSourceBurst > SOURCE_METADATA_PREFERRED_SOURCE_BURST) {
      this.sourceMetadataPreferredSource = null;
      this.sourceMetadataPreferredSourceBurst = 0;
      return undefined;
    }
    return normalized;
  }

  /** Dedicated bounded workers give source refetch continuous progress without
   * consuming chapter workers or competing with P0/P1 task selection. */
  private async runSourceMetadataRefresh(overrideWorkerId?: string): Promise<boolean> {
    // Let in-flight requests finish, but do not start more background source
    // fetches under sustained *metadata-pool* pressure, while the site is
    // paused, or when RSS nears the container's 512 MiB boundary.
    if (this.metadataReplayV4Exclusive.enabled || this.metadataTurboMode.enabled || this.sourceMetadataRefreshDeferredForV4) return false;
    const sustainedPoolPressure = this.hasSustainedSourceMetadataPoolPressure();
    // See refillSourceMetadataEnrichment: only an intermittent isolated-pool
    // queue constrains this optional lane to one external request.
    const constrainedPool = sustainedPoolPressure;
    if (this.manualProtectiveStop.active
      || (!this.metadataTurboMode.enabled && (Date.now() < this.sitePausedUntil || this.lastSiteProbe.state === 'PAUSED'))
      || this.shouldPauseLowPriorityMetadata()) return false;
    // Keep one serial P2 lane during normal operation. It is enough to avoid
    // starvation, while a 13-connection cluster retains capacity for the
    // reader and primary chapter pool. An explicit metadata campaign remains
    // opt-in and is still reduced as soon as this pool shows pressure.
    const maxActive = !this.metadataTurboMode.enabled || constrainedPool ? 1 : 6;
    if (this.sourceMetadataLaneGate.active >= maxActive) return false;
    if (!this.isRuntimeLeader) return false;
    const releaseBackgroundBudget = this.tryAcquireBackgroundMetadataBudget();
    if (!releaseBackgroundBudget) return false;
    if (!this.sourceMetadataLaneGate.tryAcquire()) {
      releaseBackgroundBudget();
      return false;
    }
    let claimed = false;
    let completedSource: string | undefined;
    try {
      const workerId = overrideWorkerId || `${this.runtimeInstanceId}:source-metadata`;
      const claimStartedAt = performance.now();
      const task = await this.queue.claimSourceMetadataRefresh(workerId, 90);
      if (task) {
        claimed = true;
        completedSource = typeof task.payload?.source === 'string' ? task.payload.source : undefined;
        // Do not wait for this request's HTTP phase before filling the other
        // approved seat. The lane and fetch gates make this a hard maximum of
        // four network-only operations; it only removes timer-created idle time.
        if (!this.metadataTurboMode.enabled) {
          void this.runSourceMetadataRefresh();
        }
        await this.catalog(task, workerId, {
        claimSqlMs: Math.round(performance.now() - claimStartedAt),
        queueDwellMs: task.createdAt ? Math.max(0, Date.now() - task.createdAt.getTime()) : null,
        });
      }
    } catch (error: any) {
      this.logger.warn('Source metadata lane failed', { error: error?.message });
    } finally {
      this.sourceMetadataLaneGate.release();
      releaseBackgroundBudget();
      // Refill only after actual progress. This keeps an empty catalogue from
      // polling its full candidate set, while a completed task immediately
      // frees its durable replacement instead of waiting for the 15 s timer.
      if (claimed && this.isRuntimeLeader) {
        const preferredSource = this.nextPreferredSourceMetadataSource(completedSource);
        const refillPromise = this.refillSourceMetadataEnrichment(true, 0, preferredSource);
        if (!this.metadataTurboMode.enabled) {
          void refillPromise
            .then(() => this.runSourceMetadataRefresh())
            .catch((error: any) => this.logger.warn('Source metadata replacement failed', { error: error?.message }));
        }
      }
    }
    return claimed;
  }

  /**
   * A single durable cross-source probe advances independently of generic P2
   * chapter/catalog claims.  It still shares the provider gate, per-host rate
   * limiter and one-work identity lock with V5, so this removes starvation
   * without increasing unsafe external or YSQL concurrency.
   */
  private async runCrossSourceMetadataDiscovery(overrideWorkerId?: string): Promise<boolean> {
    if (this.metadataReplayV4Exclusive.enabled || this.sourceMetadataRefreshDeferredForV4
      || !this.isRuntimeLeader
      || this.manualProtectiveStop.active
      || (!this.metadataTurboMode.enabled && (Date.now() < this.sitePausedUntil || this.lastSiteProbe.state === 'PAUSED'))
      || this.shouldPauseLowPriorityMetadata()) return false;
    const releaseBackgroundBudget = this.tryAcquireBackgroundMetadataBudget();
    if (!releaseBackgroundBudget) return false;
    if (!this.crossSourceDiscoveryLaneGate.tryAcquire()) {
      releaseBackgroundBudget();
      return false;
    }
    let claimed = false;
    try {
      const workerId = overrideWorkerId || `${this.runtimeInstanceId}:cross-source-metadata`;
      const task = await this.queue.claimCrossSourceMetadataDiscovery(workerId, 90);
      if (!task) return false;
      claimed = true;
      await this.withCatalogLeaseHeartbeat(task, workerId, () => this.catalog(task, workerId));
    } catch (error: any) {
      this.logger.warn('Cross-source metadata lane failed', { error: error?.message });
    } finally {
      this.crossSourceDiscoveryLaneGate.release();
      releaseBackgroundBudget();
      // A coverage task advances one provider per durable checkpoint.  Wake
      // its successor promptly, but never spin when no due task exists.
      if (claimed && this.isRuntimeLeader) {
        void this.refillCrossSourceMetadataDiscovery();
        const delay = this.metadataTurboMode.enabled ? 25 : 100;
        const timer = setTimeout(() => void this.runCrossSourceMetadataDiscovery(), delay);
        timer.unref();
      }
    }
    return claimed;
  }

  /**
   * Catalog HTTP intentionally happens outside YSQL transactions.  A first
   * catalog pass can outlive its initial lease while it checks bounded,
   * complementary sources, so retain the normal owner fence with a small
   * periodic lease renewal.  If ownership has already been lost, finalizers
   * remain fenced by the repository and the reconciler owns recovery.
   */
  private async withCatalogLeaseHeartbeat<T>(task: LiteTask, workerId: string, operation: () => Promise<T>): Promise<T> {
    let stopped = false;
    let renewal: Promise<void> | undefined;
    const renew = (): void => {
      if (stopped || renewal) return;
      renewal = this.queue.renewLease(task, workerId, CATALOG_TASK_LEASE_SECONDS)
        .then((renewed) => {
          if (!renewed) this.logger.warn('Catalog task lease renewal lost ownership', { taskId: task.id, workerId });
        })
        .catch((error: any) => {
          // A transient YSQL error leaves the existing lease intact. The next
          // heartbeat retries before that lease expires; no network runs in a
          // transaction and final persistence remains owner-fenced.
          this.logger.warn('Catalog task lease renewal failed', { taskId: task.id, error: error?.message });
        })
        .finally(() => { renewal = undefined; });
    };
    const timer = setInterval(renew, CATALOG_TASK_LEASE_HEARTBEAT_MS);
    timer.unref();
    try {
      return await operation();
    } finally {
      stopped = true;
      clearInterval(timer);
      await renewal;
    }
  }

  /**
   * This is intentionally a one-seat onboarding lane, not a second chapter
   * scheduler.  It only claims unmapped CATALOG rows produced by discovery;
   * once a work exists, every manifest chapter returns to the ordinary
   * STAFF/P0/P1/P2 queue.  The catalog path itself holds no transaction while
   * fetching the source, and memory/site/pool guardrails are shared with the
   * low-priority metadata lanes.
   */
  private async runNewWorkCatalog(): Promise<void> {
    // A chapter receipt can leave a short-lived pool waiter on every busy
    // wave.  Treating that instantaneous sample as a hard stop recreated the
    // exact starvation this lane exists to prevent.  The lane is one task
    // wide, performs no network inside a transaction, and uses the isolated
    // metadata pool for claims/lookups; site, memory and sustained metadata
    // pool pressure remain the admission guardrails.
    if (!this.isRuntimeLeader || this.manualProtectiveStop.active || this.metadataReplayV4Exclusive.enabled || Date.now() < this.sitePausedUntil || this.shouldPauseLowPriorityMetadata()) return;
    const pressured = this.hasSustainedSourceMetadataPoolPressure();
    // One single-flight P2 turn every two minutes prevents permanent
    // discovery starvation without competing in every chapter wave.
    if (pressured && Date.now() - this.lastNewWorkPressureTurnAt < 120_000) return;
    if (!pressured && typeof (this.queue as any).hasExecutableOrRecoverableP1Backlog === 'function') {
      try {
        const p1Backlog = await this.queue.hasExecutableOrRecoverableP1Backlog();
        this.p2SubordinationState = {
          subordinated: p1Backlog.pending,
          reason: p1Backlog.reason,
          checkedAt: new Date().toISOString(),
        };
      } catch (error: any) {
        this.logger.warn('P1 backlog check for P2 subordination failed', { error: error?.message });
      }
    }
    if (!this.newWorkCatalogGate.tryAcquire()) {
      return;
    }
    try {
      const workerId = `${this.runtimeInstanceId}:new-work-catalog`;
      if (pressured) {
        this.lastNewWorkPressureTurnAt = Date.now();
        const preferDiscovery = this.pressureDiscoveryTurn;
        this.pressureDiscoveryTurn = !this.pressureDiscoveryTurn;
        if (preferDiscovery) {
          this.lastNewWorkDiscoveryAt = Date.now();
          const discovery = await this.queue.claimDueDiscovery(workerId, this.config.QUEUE_LEASE_DURATION_SECONDS || CATALOG_TASK_LEASE_SECONDS);
          if (discovery) await this.discover(discovery, workerId);
          return;
        }
      }
      const task = await this.queue.claimNewWorkCatalog(workerId, CATALOG_TASK_LEASE_SECONDS);
      if (task) {
        await this.withCatalogLeaseHeartbeat(task, workerId, () => this.catalog(task, workerId));
        return;
      }
      if (
        typeof (this.queue as any).claimDueDiscovery === 'function' &&
        Date.now() - this.lastNewWorkDiscoveryAt >= 10_000
      ) {
        this.lastNewWorkDiscoveryAt = Date.now();
        const discoveryTask = await (this.queue as any).claimDueDiscovery(
          workerId,
          this.config.QUEUE_LEASE_DURATION_SECONDS || CATALOG_TASK_LEASE_SECONDS,
        );
        if (discoveryTask) {
          await this.discover(discoveryTask, workerId);
        }
      }
    } catch (error: any) {
      this.logger.warn('New-work catalog lane failed', { error: error?.message });
    } finally {
      this.newWorkCatalogGate.release();
    }
  }

  /**
   * A new work is not limited to the aliases exposed by its first source.
   * Search only operational providers, require an exact/known-alias identity
   * match twice (summary and detail), then serialize the short YSQL attach
   * phase.  Failed/slow providers are deliberately non-blocking: their
   * ordinary discovery can still attach them later, and no synthetic
   * metadata-backfill task is created here.
   */
  private async enrichNewWorkFromComplementarySources(input: {
    workId: string;
    originSource: string;
    title: string;
    slug: string;
    aliases: string[];
    author?: string;
    artist?: string;
    synopsis?: string;
    kind?: string;
    year?: number;
    /** Supplying sources is used by the durable whole-catalog campaign. */
    sourceIds?: string[];
    /** Historical discovery supplies a small, durable checkpointed batch. */
    concurrency?: number;
    /**
     * A provider search may return a translated primary title while the
     * identity proof is present only in its detail-page aliases.  The global
     * coverage pass may fetch one ranked fallback result; onboarding keeps
     * its existing no-extra-probe behaviour.
     */
    allowFallbackDetailProbe?: boolean;
    /** Known confirmed mappings for this work so mapped sources can be fetched directly by ID. */
    knownMappings?: Map<string, { sourceWorkId: string; sourceTitle?: string; sourceSlug?: string }>;
  }): Promise<CrossSourceMetadataResult> {
    const result: CrossSourceMetadataResult = {
      busy: false,
      attemptedSources: [],
      deferredSources: [],
      attached: [],
      discoveredAliases: [],
      matchedCovers: [],
      sourceProbes: {},
    };
    if (!this.crossSourceMetadataGate.tryAcquire()) return { ...result, busy: true };
    try {
      const candidates = input.sourceIds || await this.queue.complementaryCatalogSources(input.workId, input.originSource);
      const offset = !input.sourceIds && candidates.length
        ? (parseInt(input.workId.replace(/-/g, '').slice(0, 4), 16) || 0) % candidates.length : 0;
      const rotated = offset ? [...candidates.slice(offset), ...candidates.slice(0, offset)] : candidates;
      const sourceIds = input.sourceIds ? rotated : rotated.slice(0, CROSS_SOURCE_DISCOVERY_SOURCES_PER_TURN);
      result.incomplete = !input.sourceIds && candidates.length > sourceIds.length;
      if (!sourceIds.length) return result;
      const target: MatchCandidate = {
        title: input.title, slug: input.slug, aliases: [...(input.aliases || [])],
        author: input.author, artist: input.artist, synopsis: input.synopsis,
        kind: input.kind, year: input.year,
      };
      let cursor = 0;
      const probe = async (): Promise<void> => {
        while (true) {
          const source = sourceIds[cursor++];
          if (!source) return;
          const adapter = this.registry.get(source);
          if (!adapter) {
            result.deferredSources.push(source);
            result.sourceProbes![source] = {
              outcome: 'ADAPTER_MISSING',
              queries: [],
              candidateCount: 0,
            };
            continue;
          }
          const queriesExecuted: string[] = [];
          let candidateCount = 0;
          let topCandidateTitle: string | null = null;
          try {
            let host = source;
            try { host = new URL(adapter.baseUrl).host || source; } catch { /* test doubles and invalid adapters are isolated by source id */ }
            const knownMapping = input.knownMappings?.get(source);
            let selected: { sourceWorkId: string; title: string; slug: string } | null = knownMapping
              ? {
                  sourceWorkId: knownMapping.sourceWorkId,
                  title: knownMapping.sourceTitle || target.title,
                  slug: knownMapping.sourceSlug || target.slug || '',
                }
              : null;
            let summaryMatch: ReturnType<typeof matchWorkCandidate> | null = null;
            const isKnownWorkMapping = Boolean(knownMapping);

            if (!selected) {
              const queryCandidates = buildDiscoveryQueryCandidates(target.title, target.aliases);
              let fallback: { sourceWorkId: string; title: string; slug: string } | null = null;
              const seenSummaryIds = new Set<string>();
              for (const query of queryCandidates) {
                queriesExecuted.push(query);
                const rateLimitTimeout = this.metadataTurboMode.enabled ? 15_000 : 60_000;
                await this.sourceRateLimiter.acquire(host, undefined, rateLimitTimeout);
                const summaries = await this.sourceMetadataGate.runExclusive(async () => {
                  return withTimeout(adapter.searchWorks(query), CROSS_SOURCE_METADATA_TIMEOUT_MS, 'CROSS_SOURCE_SEARCH_TIMEOUT');
                });
                candidateCount += summaries.length;
                if (!topCandidateTitle && summaries[0]?.title) {
                  topCandidateTitle = summaries[0].title;
                }
                for (const summary of summaries) {
                  const summaryId = String(summary.sourceWorkId || '').trim();
                  if (!summaryId || summaryId === 'feed' || summary.slug === 'feed' || seenSummaryIds.has(summaryId)) continue;
                  seenSummaryIds.add(summaryId);
                  const match = matchWorkCandidate(target, { title: summary.title, slug: summary.slug });
                  if (match.matched && (match.confidenceScore >= CROSS_SOURCE_METADATA_MIN_CONFIDENCE || (match.matchMethod === 'FUZZY_HIGH' && match.confidenceScore >= 0.85))) {
                    selected = summary;
                    summaryMatch = match;
                    break;
                  }
                  // Search ranking is only a discovery hint, never an identity
                  // decision.  A single fallback detail fetch lets an explicit
                  // original/English/Portuguese alias prove the match later;
                  // the strict detail gate below rejects every other result.
                  // Only probe fallback if candidate has positive confidence score, token overlap,
                  // or is an alias-indexed catalog (e.g. mangadex, anilist, mangaupdates).
                  if (input.allowFallbackDetailProbe && !fallback) {
                    const isIndexedSource = source === 'mangadex' || source === 'mangaupdates' || source === 'anilist'
                      || source === 'manhastro' || source === 'nexus' || source === 'imperiodabritannia' || source === 'hipercool'
                      || source === 'mangaflix' || source === 'mangotoons' || source === 'tankouhentai' || source === 'mangalivreto'
                      || source === 'mangaonlinetv';
                    const hasTokenOverlap = hasQueryTokenOverlap(summary.title, summary.slug, query);
                    if (isIndexedSource || match.confidenceScore > 0 || hasTokenOverlap || summaries.length <= 3) {
                      fallback = summary;
                    }
                  }
                }
                if (selected) break;
              }
              selected ||= fallback;
            }
            result.attemptedSources.push(source);
            if (!selected) {
              result.sourceProbes![source] = {
                outcome: 'NOT_FOUND',
                queries: queriesExecuted,
                candidateCount,
                topCandidateTitle,
              };
              continue;
            }

            const rateLimitTimeout = this.metadataTurboMode.enabled ? 15_000 : 60_000;
            await this.sourceRateLimiter.acquire(host, undefined, rateLimitTimeout);
            const details = await this.sourceMetadataGate.runExclusive(async () => {
              return withTimeout(adapter.fetchWorkDetails(selected!.sourceWorkId), CROSS_SOURCE_METADATA_TIMEOUT_MS,
                'CROSS_SOURCE_DETAILS_TIMEOUT');
            });
            const rawDetails = details.raw && typeof details.raw === 'object' ? details.raw : {};
            const sourceMetadata: Record<string, unknown> = {
              ...rawDetails,
              ...(details.alternativeTitles !== undefined ? { alternativeTitles: details.alternativeTitles } : {}),
              ...(details.genres !== undefined ? { genres: details.genres } : {}),
            };
            const extracted = classifyExplicitSourceMetadata(sourceMetadata, details.title);
            const detailMatch = matchWorkCandidate(target, {
              title: details.title, slug: details.slug, aliases: extracted.aliases,
              author: details.author, artist: details.artist, synopsis: sanitizeSynopsis(details.synopsis) || undefined,
              kind: details.kind, year: details.year,
            });
            const detailEvidence = evaluateMetadataMatchEvidence(target, {
              title: details.title, slug: details.slug, aliases: extracted.aliases,
              author: details.author, artist: details.artist, synopsis: sanitizeSynopsis(details.synopsis) || undefined,
              kind: details.kind, year: details.year,
            });
            const targetTitle = normalizeTitle(target.title);
            const detailTitle = normalizeTitle(details.title);
            const isGenericWord = /^(manga|manhwa|manhua|webtoon|comic|novel|feed|scan)$/i.test(targetTitle);
            const explicitCrossAlias = Boolean(targetTitle && extracted.aliases.some((alias) => {
              const normAlias = normalizeTitle(alias);
              return Boolean(normAlias && normAlias === targetTitle);
            })) || Boolean(detailTitle && (target.aliases || []).some((alias) => {
              const normAlias = normalizeTitle(alias);
              return Boolean(normAlias && normAlias === detailTitle);
            })) || extracted.aliases.some((alias) => (target.aliases || []).some((ta) => {
              const na = normalizeTitle(alias);
              const nta = normalizeTitle(ta);
              return Boolean(na && nta && na === nta && !/^(manga|manhwa|manhua|webtoon|comic|novel|feed|scan)$/i.test(na));
            }));
            const titleIsSpecific = (targetTitle.length >= 1 && !isGenericWord);
            const exactTitleOrSlug = (summaryMatch ? ['EXACT_TITLE', 'EXACT_SLUG'].includes(summaryMatch.matchMethod) : true)
              && ['EXACT_TITLE', 'EXACT_SLUG'].includes(detailMatch.matchMethod);
            // Fuzzy similarity is useful for finding candidates, but it is
            // not independent identity evidence for a new source mapping.
            const independentlyCorroborated = isKnownWorkMapping || detailEvidence.autoMatch || explicitCrossAlias
              || (titleIsSpecific && exactTitleOrSlug);
            if ((!isKnownWorkMapping && (!detailMatch.matched || (detailMatch.confidenceScore < CROSS_SOURCE_METADATA_MIN_CONFIDENCE && (detailMatch.matchMethod !== 'FUZZY_HIGH' || detailMatch.confidenceScore < 0.85))))
              || !independentlyCorroborated) {
              this.logger.info('Rejected complementary metadata candidate after detail verification', {
                workId: input.workId, source, sourceWorkId: selected.sourceWorkId,
                confidence: detailMatch.confidenceScore, method: detailMatch.matchMethod,
              });
              result.sourceProbes![source] = {
                outcome: 'REJECTED_AMBIGUOUS',
                queries: queriesExecuted,
                candidateCount,
                sourceWorkId: details.sourceWorkId,
                primaryTitle: details.title,
                alternativeTitles: extracted.aliases.slice(0, 12),
                confidence: detailMatch.confidenceScore,
                matchMethod: detailMatch.matchMethod,
              };
              continue;
            }

            await this.sourceMetadataPersistenceGate.runExclusive(async () => {
              const rawContentRating = String((rawDetails as any).contentRating || (rawDetails as any).content_rating || '').toUpperCase();
              const alreadyInTargetAliases = (target.aliases || []).some(
                (a) => normalizeTitle(a) === normalizeTitle(details.title)
              );
              const providerAliases = (!alreadyInTargetAliases && isProviderDisplayTitleAlias(details.title, details.slug, target.title))
                ? Array.from(new Set([details.title, ...extracted.aliases]))
                : extracted.aliases;
              const dedupeEngine = typeof (this.dedupe as any)?.attachVerifiedMapping === 'function'
                ? this.dedupe
                : (this.historicalMetadataDedupe || this.dedupe);
              const attached = await dedupeEngine.attachVerifiedMapping({
                source, sourceWorkId: details.sourceWorkId, title: details.title, slug: details.slug,
                aliases: providerAliases, genres: extracted.genres, author: details.author, artist: details.artist,
                synopsis: sanitizeSynopsis(details.synopsis) || undefined, kind: details.kind, status: details.status,
                year: details.year, ageRating: details.ageRating,
                contentRating: rawContentRating === 'ADULT_18' || Number(details.ageRating || 0) >= 18 ? 'ADULT_18' : undefined,
                rawMetadata: sourceMetadata,
              }, input.workId, source);
              if (attached.workId !== input.workId && attached.reason !== 'CROSS_SOURCE_MAPPING_OWNED_BY_ANOTHER_WORK_ALIASED') return;
              target.aliases ||= [];
              const freshAliases = providerAliases.filter((a) => !target.aliases!.includes(a));
              if (freshAliases.length) {
                target.aliases.push(...freshAliases);
                for (const fa of freshAliases) {
                  if (!result.discoveredAliases!.includes(fa)) result.discoveredAliases!.push(fa);
                }
              }
              const aliasesAdded = Math.max(0, Number(attached.metadataDelta?.aliasesAdded || 0));
              const tagsAdded = Math.max(0, Number(attached.metadataDelta?.tagsAdded || 0));
              await this.queue.recordSourceMetadataEnrichment({
                source, sourceWorkId: details.sourceWorkId, workId: attached.workId || input.workId,
                aliasesState: (extracted.aliasesState === 'NO_ALIASES' && aliasesAdded > 0) ? 'ENRICHED' : extracted.aliasesState,
                tagsState: extracted.tagsState,
                aliasesAdded, tagsAdded,
                error: extracted.parserError ? 'EXPLICIT_SOURCE_METADATA_NOT_PARSED' : null,
                rawMetadata: sourceMetadata, version: 7,
              });
              result.attached.push({ source, sourceWorkId: details.sourceWorkId, aliasesAdded, tagsAdded });
              if (details.coverUrl && typeof details.coverUrl === 'string' && details.coverUrl.startsWith('http')) {
                result.matchedCovers?.push({
                  source,
                  sourceWorkId: details.sourceWorkId,
                  coverUrl: details.coverUrl,
                  isBrazilianSource: !['nexus', 'mangadex', 'mangafire', 'mangaball'].includes(source),
                });
              }
              result.sourceProbes![source] = {
                outcome: aliasesAdded > 0 ? 'MATCHED_ENRICHED' : 'MATCHED_NO_NEW_ALIASES',
                queries: queriesExecuted,
                candidateCount,
                sourceWorkId: details.sourceWorkId,
                primaryTitle: details.title,
                alternativeTitles: providerAliases.slice(0, 15),
                aliasesAdded,
                tagsAdded,
                confidence: detailMatch.confidenceScore,
                matchMethod: detailMatch.matchMethod,
              };
            });
            await this.queue.recordSourceSuccess(source);
            this.logger.info('Attached complementary source metadata during new-work onboarding', {
              workId: input.workId, source, sourceWorkId: details.sourceWorkId,
              aliases: extracted.aliases.length, tags: extracted.genres.length,
            });
          } catch (error: any) {
            // Provider failures are external and must never turn an otherwise
            // complete new work into a permanently blocked onboarding task.
            this.logger.warn('Complementary source metadata probe skipped', {
              workId: input.workId, source, error: error?.message,
            });
            result.deferredSources.push(source);
            result.sourceProbes![source] = {
              outcome: 'DEFERRED_EXTERNAL_ERROR',
              queries: queriesExecuted,
              candidateCount,
              error: String(error?.message || 'UNKNOWN_ERROR'),
            };
          }
        }
      };
      await Promise.all(Array.from({ length: Math.min(Math.max(1, input.concurrency || CROSS_SOURCE_METADATA_CONCURRENCY), sourceIds.length) }, () => probe()));
      return result;
    } finally {
      this.crossSourceMetadataGate.release();
    }
  }

  private hasSustainedSourceMetadataPoolPressure(): boolean {
    const waiters = this.sourceMetadataPool.waitingCount;
    if (waiters <= 0) {
      this.sourceMetadataPoolWaitStartedAt = 0;
      return false;
    }
    if (waiters >= 2) {
      this.sourceMetadataPoolWaitStartedAt ||= Date.now();
      return Date.now() - this.sourceMetadataPoolWaitStartedAt >= SOURCE_METADATA_POOL_PRESSURE_SUSTAIN_MS;
    }
    // A lone metadata-pool waiter is transient and the serialized persistence
    // gate still bounds write pressure.
    this.sourceMetadataPoolWaitStartedAt = 0;
    return false;
  }

  /**
   * One local metadata merge at a time prevents P1's legitimate chapter
   * reserve from starving aliases/tags. It reads only a saved mapping and
   * runs through the ordinary durable task/lease/finalize path; no network,
   * source slot, page buffer or chapter worker is admitted here.
   */
  private async runStoredMetadataReplay(): Promise<void> {
    // The isolated pool is single-seat.  A strict `waitingCount > 0` guard
    // here starves an already-durable v4 task whenever source-backed P2 work
    // has a waiter, even though both paths remain serialized by that pool.
    // Queueing one local merge preserves the single-flight limit and lets the
    // longer v4 lease cover the bounded YSQL wait.
    if (!this.isRuntimeLeader || this.manualProtectiveStop.active || this.shouldPauseLowPriorityMetadata()
      || (!this.metadataReplayV4Exclusive.enabled
        && Date.now() - this.lastMetadataReplayAt < STORED_METADATA_REPLAY_INTERVAL_MS)) return;
    const replayGate = this.metadataReplayV4Exclusive.enabled
      ? this.metadataReplayExclusiveGate : this.metadataReplayGate;
    if (!replayGate.tryAcquire()) return;
    const releaseBackgroundBudget = this.tryAcquireBackgroundMetadataBudget();
    if (!releaseBackgroundBudget) {
      replayGate.release();
      return;
    }
    try {
      this.lastMetadataReplayAt = Date.now();
      const workerId = `${this.runtimeInstanceId}:metadata-replay`;
      await this.queue.reclaimExpiredStoredMetadataReplayV4();
      const task = await this.queue.claimStoredMetadataReplay(workerId, STORED_METADATA_REPLAY_V4_LEASE_SECONDS, 4)
        || (!this.metadataReplayV4Exclusive.enabled && Number(this.pool.waitingCount || 0) < 2
          ? await this.queue.claimStoredMetadataReplay(workerId, 45) : null);
      if (task) {
        await this.catalog(task, workerId);
        // Outside the exclusive campaign, a V4 item still needs an immediate
        // successor: its normal producer poll is deliberately slower. The
        // campaign's one-second turn refills after claiming all ready rows so
        // a serial cursor probe cannot hold those rows idle.
        if (task.payload?.metadataEnrichmentVersion === 4
          && !this.metadataReplayV4Exclusive.enabled && !this.shouldPauseLowPriorityMetadata()) {
          await this.refillStoredMetadataEnrichmentV4();
        }
      }
    } catch (error: any) {
      this.logger.warn('Stored metadata replay failed', { error: error?.message });
    } finally {
      replayGate.release();
      releaseBackgroundBudget();
    }
  }

  /**
   * A canonical work can have several source mappings. Exclusive V4 workers
   * may run in parallel only when their work ids differ; this in-process tail
   * lock makes the additive aliases array and tag union deterministic even if
   * an older queued task bypasses the producer's active-work exclusion.
   */
  private async serializeV4Work<T>(workId: string, operation: () => Promise<T>): Promise<T> {
    const previous = this.metadataReplayV4WorkTails.get(workId) || Promise.resolve();
    let release!: () => void;
    const tail = new Promise<void>((resolve) => { release = resolve; });
    this.metadataReplayV4WorkTails.set(workId, tail);
    await previous;
    try {
      return await operation();
    } finally {
      release();
      if (this.metadataReplayV4WorkTails.get(workId) === tail) this.metadataReplayV4WorkTails.delete(workId);
    }
  }

  /** A bounded source view refreshes on recovery and at import-priority boundaries. */
  private async refreshSourceFairness(force = false): Promise<boolean> {
    const isTestEnv = Boolean(process.env.NODE_ENV === 'test' || process.env.VITEST);
    if (
      !isTestEnv
      && this.lastSourceFairnessRefreshAt > 0
      && Date.now() - this.lastSourceFairnessRefreshAt < (force ? 12_000 : 25_000)
    ) {
      return true;
    }
    if (this.sourceFairnessRefresh) return this.sourceFairnessRefresh;
    // Claims, checkpoints and public publication use the small primary pool.
    // A stale source view during a sustained chapter wave can leave only the
    // sources visible at the last idle instant with usable slots.  The
    // existing isolated metadata connection is safe for this read only while
    // it has no work of its own; otherwise retain the old bounded view rather
    // than queue a fourth class of work behind either pool.
    const primaryBusy = this.pool.waitingCount > 0;
    const isolatedPoolAvailable = this.sourceMetadataPool !== this.pool
      && this.sourceMetadataPool.waitingCount === 0
      && (this.sourceMetadataPool.idleCount > 0 || this.sourceMetadataPool.totalCount === 0);
    if (primaryBusy && !isolatedPoolAvailable) return false;
    const fairnessPool = primaryBusy ? this.sourceMetadataPool : this.pool;
    const refresh = (async (): Promise<boolean> => {
      try {
        this.executor.setFairSourceDemandCounts(await this.queue.readyImportSourceDemand(128, fairnessPool));
        this.lastSourceFairnessRefreshAt = Date.now();
        return true;
      } catch (error: any) {
        this.logger.warn('Source fairness sample failed', { error: error?.message });
        return false;
      }
    })();
    this.sourceFairnessRefresh = refresh;
    try {
      return await refresh;
    } finally {
      if (this.sourceFairnessRefresh === refresh) this.sourceFairnessRefresh = null;
    }
  }

  /**
   * P2 background lanes normally run independently of chapter workers. Once
   * a P0/P1 chapter is admitted, or a ready chapter route is already known,
   * they share the same Yugabyte service even when their network work is
   * separate. Admit one such lane at a time and start none while the primary
   * pool already has a waiter. The ready-source view closes the short startup
   * race where P2 could take several seats before the first chapter acquired
   * its admission permit. With no chapter demand, releases remain no-ops and
   * normal P2 recovery retains its full capacity.
   */
  private tryAcquireBackgroundMetadataBudget(): (() => void) | null {
    const chapterDemandActive = this.chapterAdmissionGate.active > 0 || this.executor.getFairSourceDemand().length > 0;
    if (!chapterDemandActive) return () => {};
    if (this.pool.waitingCount > 0 || !this.backgroundMetadataUnderChapterLoadGate.tryAcquire()) return null;
    return () => this.backgroundMetadataUnderChapterLoadGate.release();
  }

  private async upsertChapterSources(rows: Array<{
    workId: string; chapterKey: string; source: string; sourceWorkId: string; sourceChapterId: string;
    language: string; edition: string; orderKey: string; sourceRank: number;
  }>): Promise<void> {
    for (let offset = 0; offset < rows.length; offset += 500) {
      const batch = rows.slice(offset, offset + 500);
      const values: unknown[] = [];
      const tuples = batch.map((row, index) => {
        const start = index * 9;
        values.push(row.workId, row.chapterKey, row.source, row.sourceWorkId, row.sourceChapterId,
          row.language, row.edition, row.orderKey, row.sourceRank);
        return `($${start + 1}::uuid,$${start + 2},$${start + 3},$${start + 4},$${start + 5},$${start + 6},$${start + 7},$${start + 8},$${start + 9},true,true,now())`;
      });
      await this.getAvailablePool().query(`INSERT INTO importer_lite_chapter_sources
        (work_id,canonical_chapter_key,source,source_work_id,source_chapter_id,language,edition,order_key,source_rank,confirmed,has_chapter,last_verified_at)
        VALUES ${tuples.join(',')}
        ON CONFLICT(source,source_chapter_id) DO UPDATE SET canonical_chapter_key=excluded.canonical_chapter_key,
          source_work_id=excluded.source_work_id,language=excluded.language,edition=excluded.edition,order_key=excluded.order_key,source_rank=excluded.source_rank,
          confirmed=true,has_chapter=true,last_verified_at=now(),updated_at=now()
        WHERE importer_lite_chapter_sources.work_id IS DISTINCT FROM excluded.work_id
           OR importer_lite_chapter_sources.canonical_chapter_key IS DISTINCT FROM excluded.canonical_chapter_key
           OR importer_lite_chapter_sources.source_work_id IS DISTINCT FROM excluded.source_work_id
           OR importer_lite_chapter_sources.language IS DISTINCT FROM excluded.language
           OR importer_lite_chapter_sources.edition IS DISTINCT FROM excluded.edition
           OR importer_lite_chapter_sources.order_key IS DISTINCT FROM excluded.order_key
           OR importer_lite_chapter_sources.source_rank IS DISTINCT FROM excluded.source_rank
           OR importer_lite_chapter_sources.confirmed IS NOT TRUE
           OR importer_lite_chapter_sources.has_chapter IS NOT TRUE`, values);
    }
  }

  /**
   * Catalog passes can rediscover a whole work that the legacy importer has
   * already published.  A source/id match plus the canonical work id is
   * sufficient evidence to reuse it; similarity or a numeric display value
   * is deliberately never used here.  Returning this map before admission
   * keeps hundreds of no-op IMPORT_CHAPTER claims out of the P1 lane.
   */
  private async legacyPublishedChapterMap(source: string, workId: string, sourceChapterIds: string[]): Promise<Map<string, string>> {
    const mapped = new Map<string, string>();
    for (let offset = 0; offset < sourceChapterIds.length; offset += 500) {
      const result = await this.getAvailablePool().query<{ source_chapter_id: string; chapter_id: string }>(
        `SELECT m.source_chapter_id,m.chapter_id
         FROM importer_chapter_mappings m
         JOIN chapters c ON c.id=m.chapter_id
         WHERE m.source=$1 AND m.source_chapter_id=ANY($2::text[])
           AND c.work_id=$3::uuid AND c.published_at IS NOT NULL`,
        [source, sourceChapterIds.slice(offset, offset + 500), workId],
      );
      for (const row of result.rows) mapped.set(String(row.source_chapter_id), String(row.chapter_id));
    }
    return mapped;
  }

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
  private async legacyPublishedWholeNumberMap(workId: string, chapterKeys: string[]): Promise<Map<string, string>> {
    const requested = [...new Set(chapterKeys)].filter((chapterKey) => {
      const parts = chapterKey.split('|');
      return parts.length === 5
        && parts[0] === 'pt-br'
        && parts[1] === 'default'
        && parts[2] === 'chapter'
        && /^\d+$/.test(parts[3] || '')
        && parts[4] === '-';
    });
    const mapped = new Map<string, string>();
    for (let offset = 0; offset < requested.length; offset += 500) {
      const result = await this.getAvailablePool().query<{ chapter_key: string; chapter_id: string }>(`WITH requested AS (
          SELECT chapter_key,split_part(chapter_key,'|',4) AS whole_number
          FROM unnest($2::text[]) AS item(chapter_key)
        )
        SELECT DISTINCT ON (requested.chapter_key) requested.chapter_key,c.id AS chapter_id
        FROM requested
        JOIN chapters c ON c.work_id=$1::uuid
          AND c.published_at IS NOT NULL
          AND c.canonical_chapter_key LIKE 'legacy:%'
          AND c.chapter_kind='chapter'
          AND c.edition='default'
          AND c.language='und'
          AND c.display_number ~ '^\\d+(\\.0+)?$'
          AND regexp_replace(lower(c.display_number),'\\.0+$','')=requested.whole_number
        ORDER BY requested.chapter_key,c.published_at,c.id`, [workId, requested.slice(offset, offset + 500)]);
      for (const row of result.rows) mapped.set(String(row.chapter_key), String(row.chapter_id));
    }
    return mapped;
  }

  /** Persist the exact legacy identity without rewriting any Reader chapter. */
  private async upsertLegacyChapterMappings(rows: Array<{
    workId: string; chapterKey: string; chapterId: string; displayNumber: string;
    orderKey: string; language: string; edition: string;
  }>): Promise<void> {
    const seenChapterKeys = new Set<string>();
    const seenChapterIds = new Set<string>();
    const dedupedRows: typeof rows = [];
    for (const row of rows) {
      const wkKey = `${row.workId}:${row.chapterKey}`;
      if (seenChapterKeys.has(wkKey) || seenChapterIds.has(row.chapterId)) continue;
      seenChapterKeys.add(wkKey);
      seenChapterIds.add(row.chapterId);
      dedupedRows.push(row);
    }
    for (let offset = 0; offset < dedupedRows.length; offset += 500) {
      const batch = dedupedRows.slice(offset, offset + 500);
      const values: unknown[] = [];
      const tuples = batch.map((row, index) => {
        const start = index * 7;
        values.push(row.workId, row.chapterKey, row.chapterId, row.displayNumber, row.orderKey, row.edition, row.language);
        return `($${start + 1}::uuid,$${start + 2},$${start + 3}::uuid,$${start + 4},$${start + 5},$${start + 6},$${start + 7})`;
      });
      await this.getAvailablePool().query(`INSERT INTO importer_lite_chapters
        (work_id,canonical_chapter_key,chapter_id,display_number,order_key,edition,language)
        SELECT v.work_id,v.canonical_chapter_key,v.chapter_id,v.display_number,v.order_key,v.edition,v.language
        FROM (VALUES ${tuples.join(',')})
          AS v(work_id,canonical_chapter_key,chapter_id,display_number,order_key,edition,language)
        WHERE NOT EXISTS (
          SELECT 1 FROM importer_lite_chapters existing
          WHERE existing.chapter_id = v.chapter_id
        )
        ON CONFLICT(work_id,canonical_chapter_key) DO NOTHING`, values);
    }
  }

  private async worker(workerId: string, slotIndex: number): Promise<void> {
    telemetryCollector.registerSlot(slotIndex);
    while (!this.stopped) {
      try {
        this.setWorkerLiveness(workerId, 'CLAIM');
        if (!this.isRuntimeLeader) {
          telemetryCollector.setSlotState(slotIndex, 'WAITING_BARRIER', 'runtime-standby');
          await sleep(250);
          continue;
        }
        if (this.metadataTurboMode.enabled) {
          telemetryCollector.setSlotState(slotIndex, 'ACTIVE_SOURCE', 'DISCOVERY_TURBO');
          this.setWorkerLiveness(workerId, 'FETCH');
          const didWork = await this.runCrossSourceMetadataDiscovery(workerId);
          await sleep(didWork ? 15 : 50);
          continue;
        }
        if (this.shouldPauseChapterAdmission()) {
          telemetryCollector.setSlotState(slotIndex, 'WAITING_BARRIER', 'site-protection');
          await sleep(250);
          continue;
        }
        // Claiming is intentionally single-flight for durable priority and
        // work-order correctness.  Do not spend one of the scarce chapter
        // permits merely waiting for that short YSQL section: at a cap of 12
        // the former order allowed eleven slots to be counted as "active"
        // while only one worker could actually reach the claim or download.
        // Serialise first, then reserve a permit only for the worker that is
        // about to issue the durable claim.  This changes no claim ordering,
        // lease, source-share or publication rule.
        //
        // A full operator cap cannot produce a claim.  Keep the surplus
        // workers out of the single-flight lane in that case: otherwise they
        // repeatedly acquire the claim mutex only to discover that every
        // chapter seat is occupied.  The acquisition below remains the
        // authoritative race-safe check after entering the lane.
        if (this.chapterAdmissionGate.available <= 0) {
          telemetryCollector.setSlotState(slotIndex, 'WAITING_BARRIER', 'operator-chapter-cap');
          await sleep(100);
          continue;
        }
        if (!this.claimGate.tryAcquire()) {
          telemetryCollector.setSlotState(slotIndex, 'WAITING_CLAIM_DB', 'claim-serialization');
          await sleep(25);
          continue;
        }
        let task: LiteTask | null = null;
        let admissionAcquired = false;
        try {
          if (!this.chapterAdmissionGate.tryAcquire()) {
            telemetryCollector.setSlotState(slotIndex, 'WAITING_BARRIER', 'operator-chapter-cap');
            await sleep(100);
            continue;
          }
          admissionAcquired = true;
          telemetryCollector.setSlotState(slotIndex, 'WAITING_CLAIM_DB');
          task = await this.claimNext(workerId, true);
        } catch (error) {
          // The admission permit protects a claimed chapter, never a failed
          // claim attempt.  Preserve the old finally guarantee when YSQL
          // rejects the short durable claim.
          if (admissionAcquired) {
            this.chapterAdmissionGate.release();
            admissionAcquired = false;
          }
          throw error;
        } finally {
          this.claimGate.release();
        }
        try {
          if (!task) {
            this.setWorkerLiveness(workerId, 'IDLE');
            telemetryCollector.setSlotState(slotIndex, 'IDLE');
            await sleep(750);
            continue;
          }
          telemetryCollector.setSlotState(slotIndex,
            task.taskType === 'IMPORT_CHAPTER' ? 'ACTIVE_DOWNLOAD' : task.taskType === 'PUBLISH' ? 'ACTIVE_DB' : 'ACTIVE_SOURCE',
            task.taskType);
          this.setWorkerLiveness(workerId, task.taskType === 'PUBLISH' ? 'PUBLISH' : task.taskType === 'IMPORT_CHAPTER' ? 'DOWNLOAD' : 'FETCH', task);
          await this.handle(task, workerId, { inlinePublish: true });
          this.setWorkerLiveness(workerId, 'FINALIZE', task);
          telemetryCollector.setSlotState(slotIndex, 'IDLE');
          this.setWorkerLiveness(workerId, 'IDLE');
        } finally {
          if (admissionAcquired) this.chapterAdmissionGate.release();
        }
      } catch {
        this.setWorkerLiveness(workerId, 'IDLE');
        telemetryCollector.setSlotState(slotIndex, 'IDLE');
        await sleep(1_000);
      }
    }
  }

  private setWorkerLiveness(workerId: string, stage: WorkerLiveness['stage'], task?: LiteTask): void {
    this.workerLiveness.set(workerId, {
      taskId: task?.id || null,
      source: task ? String(task.payload.source || '') || null : null,
      stage,
      since: new Date().toISOString(),
    });
  }

  private setWorkerSource(taskId: string, source: string): void {
    for (const value of this.workerLiveness.values()) {
      if (value.taskId === taskId) value.source = source;
    }
  }

  private livenessSnapshot() {
    const memory = process.memoryUsage();
    const memoryPressure = this.memoryPressureEnabled
      ? this.memoryPressureGuard.sample(memory)
      : this.memoryPressureGuard.snapshot();
    return {
      at: new Date().toISOString(),
      memoryMiB: { rss: Math.round(memory.rss / 1024 / 1024), heapUsed: Math.round(memory.heapUsed / 1024 / 1024),
        heapTotal: Math.round(memory.heapTotal / 1024 / 1024), external: Math.round(memory.external / 1024 / 1024),
        arrayBuffers: Math.round(memory.arrayBuffers / 1024 / 1024) },
      memoryPressure,
      ysqlPool: { total: this.pool.totalCount, idle: this.pool.idleCount, waiting: this.pool.waitingCount },
      chapterAdmission: {
        active: this.chapterAdmissionGate.active,
        capacity: this.chapterAdmissionGate.capacity,
        waiters: this.chapterAdmissionGate.queued,
      },
      claimSerialization: {
        active: this.claimGate.active,
        capacity: this.claimGate.capacity,
        waiters: this.claimGate.queued,
      },
      backgroundMetadataUnderChapterLoad: {
        active: this.backgroundMetadataUnderChapterLoadGate.active,
        capacity: this.backgroundMetadataUnderChapterLoadGate.capacity,
        waiters: this.backgroundMetadataUnderChapterLoadGate.queued,
      },
      sourceMetadataLane: {
        active: this.sourceMetadataLaneGate.capacity - this.sourceMetadataLaneGate.available,
        capacity: this.sourceMetadataLaneGate.capacity,
        waiters: this.sourceMetadataLaneGate.queued,
      },
      sourceMetadataPersistence: {
        active: this.sourceMetadataPersistenceGate.active,
        capacity: this.sourceMetadataPersistenceGate.capacity,
        waiters: this.sourceMetadataPersistenceGate.queued,
      },
      pageByteBudget: this.bufferedPageBytes.snapshot(),
      heartbeat: { startedAt: this.heartbeatStartedAt, completedAt: this.heartbeatCompletedAt },
      lease: { startedAt: this.leaseRenewStartedAt, completedAt: this.leaseRenewCompletedAt, error: this.leaseRenewError },
      workers: [...this.workerLiveness.entries()].map(([workerId, value]) => ({ workerId, ...value })),
    };
  }

  /** One empty claim probe per idle interval, shared by every worker. */
  private async claimNext(workerId: string, claimGateHeld = false): Promise<LiteTask | null> {
    const claim = async (): Promise<LiteTask | null> => {
      if (!this.isRuntimeLeader) return null;
      if (this.manualProtectiveStop.active) return null;
      if (Date.now() < this.idleClaimUntil) return null;
      void this.refreshP1FocusGroupNow();
      const focusWorkKeys = typeof (this.queue as any).getP1FocusWorkKeys === 'function'
        ? this.queue.getP1FocusWorkKeys()
        : [];
      const excludedSources = typeof (this.executor as any).getSaturatedFairSources === 'function'
        ? this.executor.getSaturatedFairSources()
        : [];
      const preferFocus = (this.p1ClaimCounter++ % 4) !== 3;
      const task = (focusWorkKeys.length > 0 || excludedSources.length > 0)
        ? await this.queue.claim(workerId, this.config.QUEUE_LEASE_DURATION_SECONDS, {
          focusWorkKeys,
          preferFocus,
          excludedSources,
          chaptersOnly: true,
        })
        : await this.queue.claim(workerId, this.config.QUEUE_LEASE_DURATION_SECONDS, {
          chaptersOnly: true,
        });
      if (task?.taskType === 'IMPORT_CHAPTER' && task.priority !== this.fairDemandImportPriority) {
        // The claim gate makes this a true priority-boundary refresh: no
        // second worker can begin a stale P0-sized source wave while the
        // first task of the new priority updates the local source caps.
        // It is one bounded read per transition, never per page or task.
        if (await this.refreshSourceFairness(true)) this.fairDemandImportPriority = task.priority;
      }
      this.idleClaimUntil = task ? 0 : Date.now() + 150;
      return task;
    };
    return claimGateHeld ? claim() : this.claimGate.runExclusive(claim);
  }

  /**
   * Claim at most one due P0 observation per short queue turn. A second
   * already-claimed observation may fetch another source concurrently, but
   * claims themselves remain serial.  A chapter-pool waiter must not turn
   * into a global P0 off switch: the one bounded monitor claim may queue
   * behind the current database work, then keeps source discovery alive while
   * P1/P2 media continues.  The two monitor permits still cap both the
   * network work and the single outstanding P0 database claim.
   */
  private async runMonitorTurn(): Promise<void> {
    // A V4 metadata campaign is P2 work; it must not globally freeze P0
    // release observation.  The monitor's own bounded network permits and serial
    // claim gate keep this higher-priority background service bounded.
    if (this.stopped || !this.isRuntimeLeader
      || this.manualProtectiveStop.active || Date.now() < this.sitePausedUntil) return;
    if (!this.monitorGate.tryAcquire()) return;
    let hadTask = false;
    let activeSource = '';
    try {
      if (Date.now() < this.idleMonitorUntil) return;
      // A second bounded source probe may wait only for the first short
      // transactional claim, then fetch independently.  Dropping it here
      // would turn the bounded network permits back into one whenever YSQL takes
      // more than a tick to return a claim.
      await this.monitorClaimGate.acquire();
      let task: LiteTask | null = null;
      try {
        // Recheck after taking the claim gate: a sibling may have found the
        // lane empty while this turn waited for its one bounded claim.
        if (Date.now() < this.idleMonitorUntil) return;
        const workerId = `${this.runtimeInstanceId}:lite-monitor`;
        // Keep at most two active checks per source. A slow provider cannot
        // occupy the whole lane; at most eight other leases wait locally.
        for (;;) {
          const index = this.monitorClaimBuffer.findIndex((candidate) => {
            const source = String(candidate.payload?.source || '');
            return !source || (this.monitorActiveBySource.get(source) || 0) < 2;
          });
          if (index >= 0) {
            const candidate = this.monitorClaimBuffer.splice(index, 1)[0];
            if (candidate.leaseUntil && candidate.leaseUntil.getTime() <= Date.now() + 30_000
              && !await this.queue.renewLease(candidate, workerId, this.config.QUEUE_LEASE_DURATION_SECONDS)) continue;
            task = candidate;
            activeSource = String(task.payload?.source || '');
            if (activeSource) this.monitorActiveBySource.set(activeSource, (this.monitorActiveBySource.get(activeSource) || 0) + 1);
            break;
          }
          if (this.monitorClaimBuffer.length >= 16) break;
          this.monitorClaimBatchCount += 1;
          const busySources: string[] = [];
          for (const [source, count] of this.monitorActiveBySource.entries()) {
            if (count >= 2) busySources.push(source);
          }
          for (const buffered of this.monitorClaimBuffer) {
            const source = String(buffered.payload?.source || '');
            if (source && !busySources.includes(source)) busySources.push(source);
          }
          const batch = await this.monitorSqlQueue().claimMonitorBatch(
            workerId,
            this.config.QUEUE_LEASE_DURATION_SECONDS,
            Math.min(2, 16 - this.monitorClaimBuffer.length),
            this.monitorClaimBatchCount % 4 === 0,
            busySources,
          );
          if (!batch.length) break;
          this.monitorClaimBuffer.push(...batch);
        }
        this.idleMonitorUntil = task || this.monitorClaimBuffer.length ? 0 : Date.now() + 2_000;
      } finally {
        this.monitorClaimGate.release();
      }
      if (task) {
        hadTask = true;
        if (
          this.monitorGate.available > 0
          && Date.now() >= this.idleMonitorUntil
          && !this.stopped
          && !(process.env.NODE_ENV === 'test' || process.env.VITEST)
        ) {
          void this.runMonitorTurn().catch(() => {});
        }
        await this.handle(task, `${this.runtimeInstanceId}:lite-monitor`);
      }
    } finally {
      if (activeSource) {
        const remaining = (this.monitorActiveBySource.get(activeSource) || 1) - 1;
        if (remaining > 0) this.monitorActiveBySource.set(activeSource, remaining);
        else this.monitorActiveBySource.delete(activeSource);
      }
      this.monitorGate.release();
      if (
        hadTask
        && !this.stopped
        && this.isRuntimeLeader
        && Date.now() >= this.idleMonitorUntil
        && !(process.env.NODE_ENV === 'test' || process.env.VITEST)
      ) {
        const nextDelayMs = (this.sourceMetadataPool.waitingCount || 0) > 1 ? 120 : 25;
        const nextTurn = setTimeout(() => void this.runMonitorTurn().catch(() => {}), nextDelayMs);
        nextTurn.unref();
      }
    }
  }

  /**
   * Probe a lightweight DB-backed public Reader endpoint (/api/releases?limit=1)
   * rather than the static Cloudflare Worker edge /api/health isolate, so real
   * YSQL or Reader latency degradation is immediately visible to protection.
   */
  private async probeSiteHealth(): Promise<{ healthy: boolean; latencyMs: number | null; status: number | null }> {
    const started = Date.now();
    try {
      const response = await fetch(`${this.config.NOX_MANGA_URL.replace(/\/$/, '')}/api/releases?limit=1`, {
        signal: AbortSignal.timeout(6_000),
      });
      await response.body?.cancel().catch(() => {});
      const latencyMs = Date.now() - started;
      return { healthy: response.ok, latencyMs, status: response.status };
    } catch {
      return { healthy: false, latencyMs: null, status: null };
    }
  }

  private scheduleSiteRecoveryProbe(): void {
    if (this.stopped || this.siteRecoveryProbeScheduled) return;
    this.siteRecoveryProbeScheduled = true;
    const timer = setTimeout(() => {
      this.siteRecoveryProbeScheduled = false;
      if (this.stopped || this.metadataTurboMode.enabled || Date.now() >= this.sitePausedUntil) return;
      void this.probeSharedResources(true).catch(() => {});
    }, 5_000);
    timer.unref();
    this.timers.push(timer);
  }

  private async probeSharedResources(recoveryProbe = false): Promise<void> {
    await this.refreshManualProtectiveStop();
    if (this.metadataTurboMode.enabled) {
      this.sitePausedUntil = 0;
      this.lastSiteProbe = { state: 'HEALTHY', checkedAt: new Date().toISOString(), latencyMs: 0 };
      return;
    }
    const first = await this.probeSiteHealth();
    if (first.healthy && (first.latencyMs === null || first.latencyMs <= 5_000)) {
      this.sitePausedUntil = 0;
      this.lastSiteProbe = { state: 'HEALTHY', checkedAt: new Date().toISOString(), latencyMs: first.latencyMs };
      this.updateAdaptiveAdmissionControl();
      return;
    }
    // A one-off DIScloud egress failure or network queueing spike can make a
    // single probe slow (>5000ms) or fail even while Reader is healthy. Confirm
    // once before pausing or reducing admission; a real outage or continuous
    // degradation still triggers protection, while a transient container-side
    // blip does not throttle healthy workers.
    const confirmation = await this.probeSiteHealth();
    if (confirmation.healthy) {
      this.sitePausedUntil = 0;
      const effectiveLatency = (confirmation.latencyMs !== null && confirmation.latencyMs <= 5_000)
        ? confirmation.latencyMs
        : (first.latencyMs !== null && first.latencyMs <= 5_000 ? first.latencyMs : Math.min(confirmation.latencyMs ?? 5_000, first.latencyMs ?? 5_000));
      this.lastSiteProbe = { state: 'HEALTHY', checkedAt: new Date().toISOString(), latencyMs: effectiveLatency };
      this.logger.warn('Transient site-health probe recovered on confirmation before throttling Lite claims', {
        firstStatus: first.status, firstLatencyMs: first.latencyMs, confirmationLatencyMs: confirmation.latencyMs,
      });
      this.updateAdaptiveAdmissionControl();
      return;
    }
    // An importer-side network failure does not establish that Reader is
    // unavailable to visitors.  Pausing every chapter after two `fetch`
    // exceptions turned a transient DISCloud egress path into a global
    // throughput brake even while the public health endpoint was healthy.
    // A real Reader response (5xx or slow) still takes the conservative
    // pause below; only two absent HTTP responses retain admissions.
    if (first.status === null && confirmation.status === null) {
      this.sitePausedUntil = 0;
      this.lastSiteProbe = { state: 'DEGRADED', checkedAt: new Date().toISOString(), latencyMs: null };
      this.logger.warn('Importer egress could not confirm Reader health; retaining Lite admissions', {
        firstStatus: first.status, confirmationStatus: confirmation.status,
      });
      this.updateAdaptiveAdmissionControl();
      return;
    }
    // A confirmed outage reduces pressure without retrying tasks or restarting
    // the process.  The next scheduled probe can release this small gate.
    // An early recovery probe must not extend a still-valid protective window
    // if Reader remains unavailable.  The regular 30-second health timer
    // will make the next bounded decision.
    if (!recoveryProbe || this.sitePausedUntil <= Date.now()) this.sitePausedUntil = Date.now() + 30_000;
    this.lastSiteProbe = { state: 'PAUSED', checkedAt: new Date().toISOString(), latencyMs: confirmation.latencyMs ?? first.latencyMs };
    this.updateAdaptiveAdmissionControl();
    this.logger.warn('Pausing new Lite claims to protect shared site resources', {
      firstStatus: first.status, firstLatencyMs: first.latencyMs,
      confirmationStatus: confirmation.status, confirmationLatencyMs: confirmation.latencyMs,
    });
    if (!recoveryProbe) this.scheduleSiteRecoveryProbe();
  }

  /** A small diagnostic snapshot; it deliberately skips contention and scans. */
  private async emitOperationalSnapshot(): Promise<void> {
    if (this.operationalSnapshotRunning) return;
    // The compact heartbeat is specifically the busy-pool path.  It writes a
    // single liveness marker, but must not hold the full-snapshot admission
    // fence while that write itself waits behind chapter persistence.  Doing
    // so turned ordinary pool contention into an avoidable all-worker pause.
    // This check and the flag assignment below are synchronous, so an idle
    // full snapshot still establishes its fence before a worker can interleave
    // and start a new claim.
    const recentFullSnapshot = Boolean(
      this.lastFullOperationalSnapshotAt
      && Date.now() - Date.parse(this.lastFullOperationalSnapshotAt) < 5 * 60_000,
    );
    if (this.pool.waitingCount > 0 || this.chapterAdmissionGate.active > 0 || recentFullSnapshot) {
      await this.emitOperationalSnapshotOnce();
      return;
    }
    this.operationalSnapshotRunning = true;
    try {
      await this.emitOperationalSnapshotOnce();
    } finally {
      this.operationalSnapshotRunning = false;
    }
  }

  /**
   * Keep per-chapter timing useful during a busy wave without serialising the
   * full collector report (or issuing another database query). All values are
   * pre-aggregated, bounded in process, and intentionally contain no task,
   * work, media, or URL identifiers.
   */
  private chapterStageTelemetry(pipelineTelemetry: any): Record<string, unknown> {
    const profile = pipelineTelemetry?.jobProfile || {};
    const stages = profile.stages || {};
    const pick = (name: string) => stages[name] || null;
    return {
      completed: Number(profile.totalCompleted || 0),
      totalDuration: profile.totalDuration || null,
      wallTimeBreakdownMs: profile.wallTimeBreakdownMs || null,
      stages: {
        sourceFetch: pick('sourceFetch'),
        pageResolution: pick('pageResolution'),
        download: pick('download'),
        telegramUpload: pick('telegramUpload'),
        dbPublish: pick('dbPublish'),
        rateLimitWait: pick('rateLimitWait'),
        semaphoreWait: pick('semaphoreWait'),
        otherWait: pick('otherWait'),
      },
    };
  }

  /**
   * Only immutable, non-secret release identifiers belong in the durable
   * heartbeat.  Keeping this adjacent to the effective limits makes an
   * operator able to distinguish an old artifact from a deliberately lower
   * environment setting without consulting process logs.
   */
  private runtimeFingerprintTelemetry(pipelineTelemetry: any): Record<string, string | null> {
    const fingerprint = pipelineTelemetry?.runtimeFingerprint || {};
    const stringValue = (key: string): string | null =>
      typeof fingerprint[key] === 'string' ? fingerprint[key] : null;
    return {
      gitSha: stringValue('gitSha'),
      artifactSha256: stringValue('artifactSha256'),
      buildId: stringValue('buildId'),
      rolloutRevision: stringValue('rolloutRevision'),
      builtAt: stringValue('builtAt'),
      bootedAt: stringValue('bootedAt'),
      nodeVersion: stringValue('nodeVersion'),
    };
  }

  /** Values after both the runtime and per-source clamps; no configuration secrets. */
  private pageWaveLimits(): Record<string, unknown> {
    const runtimeMaximum = Math.max(1, Math.min(10, this.config.BATCH_PAGE_DOWNLOAD_CONCURRENCY));
    return {
      configuredPagesPerChapter: this.config.BATCH_PAGE_DOWNLOAD_CONCURRENCY,
      runtimeMaximumPagesPerChapter: runtimeMaximum,
      pagesPerChapter: runtimeMaximum,
      sourcePagesPerChapter: {
        mangaflix: this.executor.getPageConcurrencyForSource('mangaflix'),
        manhastro: this.executor.getPageConcurrencyForSource('manhastro'),
        default: this.executor.getPageConcurrencyForSource('__default__'),
      },
    };
  }

  private async emitOperationalSnapshotOnce(): Promise<void> {
    this.heartbeatStartedAt = new Date().toISOString();
    const liveness = this.livenessSnapshot();
    const memory = process.memoryUsage();
    // This must run before any optional diagnostic query. It is a bounded
    // process log, so a future stall leaves the last local state without
    // competing for a YSQL connection or retaining per-page telemetry.
    this.logger.info('Lite liveness', {
      leader: this.isRuntimeLeader, ...liveness,
    });
    const recentFullSnapshot = Boolean(
      this.lastFullOperationalSnapshotAt
      && Date.now() - Date.parse(this.lastFullOperationalSnapshotAt) < 5 * 60_000,
    );
    const snapshotDeferred = this.pool.waitingCount > 0
      ? 'POOL_BUSY'
      : this.chapterAdmissionGate.active > 0
        ? 'CHAPTER_BUSY'
        : recentFullSnapshot
          ? 'RECENT_FULL_SNAPSHOT'
          : null;
    if (snapshotDeferred) {
      // Do not run four diagnostic scans while the small direct pool is busy,
      // nor race them against an admitted chapter between its short database
      // phases. The latter was a check-then-race: a snapshot could start with
      // no waiter and occupy every seat before the next chapter commit.
      // Keep a tiny durable liveness marker instead; a clean idle turn resumes
      // the full aggregate view without delaying P1/P2 or publication work,
      // and an old full snapshot cannot be mistaken for a stopped runtime.
      const pipelineTelemetry = telemetryCollector.getSnapshotReport() as any;
      const deferred = {
        runtime: 'lite' as const,
        runtimeInstanceId: this.runtimeInstanceId,
        runtimeFingerprint: this.runtimeFingerprintTelemetry(pipelineTelemetry),
        leader: this.isRuntimeLeader,
        chapterWorkers: this.chapterWorkerCapacity,
        chapterAdmissionCap: this.operatorChapterAdmissionCap,
        adaptiveAdmission: {
          operatorCap: this.operatorChapterAdmissionCap,
          effectiveCap: this.chapterAdmissionGate.capacity,
          healthyStreak: this.adaptiveHealthyStreak,
          reason: this.adaptiveAdmissionReason,
        },
        metadataReplayV4Exclusive: this.metadataReplayV4Exclusive,
        manualProtectiveStop: this.manualProtectiveStop,
        discoverySeed: {
          ...this.discoverySeed,
          retryAt: this.nextDiscoverySeedRetryAt ? new Date(this.nextDiscoverySeedRetryAt).toISOString() : null,
        },
        snapshotDeferred,
        lastFullOperationalSnapshotAt: this.lastFullOperationalSnapshotAt,
        ysql: { poolTotal: this.pool.totalCount, poolIdle: this.pool.idleCount, poolWaiting: this.pool.waitingCount },
        sourceMetadataYsql: {
          isolated: this.sourceMetadataPool !== this.pool,
          poolTotal: this.sourceMetadataPool.totalCount,
          poolIdle: this.sourceMetadataPool.idleCount,
          poolWaiting: this.sourceMetadataPool.waitingCount,
        },
        ysqlTelemetry: pipelineTelemetry.database,
        sourceMetadataLane: {
          active: this.sourceMetadataLaneGate.capacity - this.sourceMetadataLaneGate.available,
          capacity: this.sourceMetadataLaneGate.capacity,
          waiters: this.sourceMetadataLaneGate.queued,
        },
        backgroundMetadataUnderChapterLoad: {
          active: this.backgroundMetadataUnderChapterLoadGate.active,
          capacity: this.backgroundMetadataUnderChapterLoadGate.capacity,
          waiters: this.backgroundMetadataUnderChapterLoadGate.queued,
        },
        sourceMetadataPersistence: {
          active: this.sourceMetadataPersistenceGate.active,
          capacity: this.sourceMetadataPersistenceGate.capacity,
          waiters: this.sourceMetadataPersistenceGate.queued,
        },
        sourceMetadataProducer: this.sourceMetadataProducer,
        p0MonitorCoverage: this.p0MonitorCoverage,
        p0BorrowedSqlOperations: this.monitorSqlBorrowedOperations,
        p1FocusGroup: typeof (this.queue as any).getP1FocusGroupSnapshot === 'function'
          ? this.queue.getP1FocusGroupSnapshot()
          : null,
        p2NewWorkPolicy: this.p2SubordinationState,
        sourceFairness: {
          effectiveChapterAdmissionCap: this.executor.getEffectiveChapterAdmissionCap(),
          burst: this.executor.getSourceFairnessBurstSnapshot(),
          historicalP1Producer: this.historicalP1Producer,
          sourcePerformance: this.executor.getSourcePerformanceSnapshot(),
        },
        memoryPressure: liveness.memoryPressure,
        // In-memory stage telemetry is safe to sample while the two YSQL
        // connections are busy.  Without it, the exact periods that need
        // diagnosis lose download/upload evidence to the protective snapshot
        // deferral.
        pipeline: {
          // These are effective parsed runtime limits, not inferred load.
          // Persisting them makes a low production ceiling distinguishable
          // from a slow provider without exposing any endpoint or credential.
          limits: {
            pageDownloads: this.downloads.capacity,
            pageBuffers: this.bufferedPages.capacity,
            pageBufferBytes: this.bufferedPageBytes.capacityBytes,
            normalPageMediaProfile: this.memoryPressureGuard.getNormalMediaProfile(),
            ...this.pageWaveLimits(),
            telegramUploads: this.config.TELEGRAM_MEDIA_CONCURRENCY,
            uploadBytesPerSecond: this.config.UPLOAD_RATE_LIMIT_BYTES_PER_SEC,
          },
          downloads: pipelineTelemetry.imageDownload,
          telegram: pipelineTelemetry.telegramStorage,
          pageDownloadLimiter: pipelineTelemetry.limitersAudit?.lite_global_page_downloads,
          pageBufferLimiter: pipelineTelemetry.limitersAudit?.lite_global_page_buffers,
          pageByteBudget: this.bufferedPageBytes.snapshot(),
          uploadBatches: this.executor.getUploadBatchSnapshot(),
          chapterStages: this.chapterStageTelemetry(pipelineTelemetry),
        },
        sourceRate: this.sourceRateLimiter.getSummary(),
        liveness,
        site: this.lastSiteProbe,
        observedAt: new Date().toISOString(),
      };
      this.logger.info('Lite operational heartbeat deferred', deferred);
      await this.pool.query(
        `INSERT INTO settings(key,value) VALUES('importer_heartbeat',$1)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
        [JSON.stringify(deferred)],
      );
      this.heartbeatCompletedAt = new Date().toISOString();
      return;
    }
    const [tasks, progress, blocked, sourceHealth, metadataProduct] = await Promise.all([
      this.queue.operationalSnapshot(), this.queue.progressSnapshot(),
      this.queue.blockedReasonSnapshot(), this.queue.sourceHealthSnapshot(), this.queue.metadataProductSnapshot(),
    ]);
    // The importer intentionally keeps a small YSQL pool shared with the
    // site. Optional diversity telemetry must not add another concurrent
    // heartbeat query and manufacture pool waiters by itself.
    const diversity = await this.queue.publicationDiversitySnapshot();
    const fairSources = this.executor.getFairSourceDemand();
    const storageMetrics = this.storage.getMetricsSummary?.() as any;
    // Keep the durable heartbeat compact, but include independently measured
    // download and Telegram occupancy.  Aggregate publication rate cannot
    // distinguish a slow CDN from a slow upload/persistence path.
    const pipelineTelemetry = telemetryCollector.getSnapshotReport() as any;
    const telegram = storageMetrics ? {
      uploads: Number(storageMetrics.totalBotUploads || 0),
      shardUploads: Number(storageMetrics.totalShardUploads || 0),
      active: (storageMetrics.bots || []).reduce((sum: number, bot: any) => sum + Number(bot.active || 0), 0),
      rateLimits429: (storageMetrics.bots || []).reduce((sum: number, bot: any) => sum + Number(bot.rateLimits429 || 0), 0),
    } : null;
    const fullSnapshotAt = new Date().toISOString();
    const snapshot = {
      runtime: 'lite' as const,
      runtimeInstanceId: this.runtimeInstanceId,
      runtimeFingerprint: this.runtimeFingerprintTelemetry(pipelineTelemetry),
      leader: this.isRuntimeLeader,
      chapterWorkers: this.chapterWorkerCapacity,
      chapterAdmissionCap: this.operatorChapterAdmissionCap,
      adaptiveAdmission: {
        operatorCap: this.operatorChapterAdmissionCap,
        effectiveCap: this.chapterAdmissionGate.capacity,
        healthyStreak: this.adaptiveHealthyStreak,
        reason: this.adaptiveAdmissionReason,
      },
      lastFullOperationalSnapshotAt: fullSnapshotAt,
      metadataReplayV4Exclusive: this.metadataReplayV4Exclusive,
      manualProtectiveStop: this.manualProtectiveStop,
      discoverySeed: {
        ...this.discoverySeed,
        retryAt: this.nextDiscoverySeedRetryAt ? new Date(this.nextDiscoverySeedRetryAt).toISOString() : null,
      },
      publishedLastMinute: progress.publishedLastMinute,
      oldestReadyAt: progress.oldestReadyAt?.toISOString() || null,
      oldestStoredUnpublishedAt: progress.oldestStoredUnpublishedAt?.toISOString() || null,
      nextP0MonitorAt: progress.nextMonitorAt?.toISOString() || null,
      tasks, blocked, sourceHealth, metadataProduct, slots: telemetryCollector.getSlotProductivitySnapshot(),
      memoryMiB: {
        rss: Math.round(memory.rss / 1024 / 1024), heapUsed: Math.round(memory.heapUsed / 1024 / 1024),
        external: Math.round(memory.external / 1024 / 1024), arrayBuffers: Math.round(memory.arrayBuffers / 1024 / 1024),
      },
      ysql: { poolTotal: this.pool.totalCount, poolIdle: this.pool.idleCount, poolWaiting: this.pool.waitingCount },
      sourceMetadataYsql: {
        isolated: this.sourceMetadataPool !== this.pool,
        poolTotal: this.sourceMetadataPool.totalCount,
        poolIdle: this.sourceMetadataPool.idleCount,
        poolWaiting: this.sourceMetadataPool.waitingCount,
      },
      ysqlTelemetry: pipelineTelemetry.database,
      sourceMetadataLane: {
        active: this.sourceMetadataLaneGate.capacity - this.sourceMetadataLaneGate.available,
        capacity: this.sourceMetadataLaneGate.capacity,
        waiters: this.sourceMetadataLaneGate.queued,
      },
      backgroundMetadataUnderChapterLoad: {
        active: this.backgroundMetadataUnderChapterLoadGate.active,
        capacity: this.backgroundMetadataUnderChapterLoadGate.capacity,
        waiters: this.backgroundMetadataUnderChapterLoadGate.queued,
      },
      sourceMetadataPersistence: {
        active: this.sourceMetadataPersistenceGate.active,
        capacity: this.sourceMetadataPersistenceGate.capacity,
        waiters: this.sourceMetadataPersistenceGate.queued,
      },
      sourceMetadataProducer: this.sourceMetadataProducer,
      p0MonitorCoverage: this.p0MonitorCoverage,
      p0BorrowedSqlOperations: this.monitorSqlBorrowedOperations,
      aliasOnlyEnrichment: {
        enabled: true,
        category: 'ALIAS_ONLY',
        availableSources: [...ALIAS_ONLY_SOURCE_IDS],
        processedWorks: this.aliasOnlyTelemetry.processedWorks,
        aliasesAdded: this.aliasOnlyTelemetry.aliasesAdded,
        lastRunAt: this.aliasOnlyTelemetry.lastRunAt,
        lastOutcome: this.aliasOnlyTelemetry.lastOutcome,
        sources: this.aliasOnlyService.getHealthSnapshots(),
      },
      memoryPressure: liveness.memoryPressure,
      pipeline: {
        limits: {
          pageDownloads: this.downloads.capacity,
          pageBuffers: this.bufferedPages.capacity,
          pageBufferBytes: this.bufferedPageBytes.capacityBytes,
          normalPageMediaProfile: this.memoryPressureGuard.getNormalMediaProfile(),
          ...this.pageWaveLimits(),
          telegramUploads: this.config.TELEGRAM_MEDIA_CONCURRENCY,
          uploadBytesPerSecond: this.config.UPLOAD_RATE_LIMIT_BYTES_PER_SEC,
        },
        downloads: pipelineTelemetry.imageDownload,
        telegram: pipelineTelemetry.telegramStorage,
        pageDownloadLimiter: pipelineTelemetry.limitersAudit?.lite_global_page_downloads,
        pageBufferLimiter: pipelineTelemetry.limitersAudit?.lite_global_page_buffers,
        pageByteBudget: this.bufferedPageBytes.snapshot(),
        uploadBatches: this.executor.getUploadBatchSnapshot(),
        chapterStages: this.chapterStageTelemetry(pipelineTelemetry),
      },
      telegram, sourceRate: this.sourceRateLimiter.getSummary(), diversity,
      p1FocusGroup: typeof (this.queue as any).getP1FocusGroupSnapshot === 'function'
        ? this.queue.getP1FocusGroupSnapshot()
        : null,
      p2NewWorkPolicy: this.p2SubordinationState,
      sourceFairness: {
        readySources: fairSources,
        count: fairSources.length,
        // This is the executor input, not the configured worker count. It
        // makes a production heartbeat prove that source shares honor the
        // guarded admission cap during a controlled ramp.
        effectiveChapterAdmissionCap: this.executor.getEffectiveChapterAdmissionCap(),
        burst: this.executor.getSourceFairnessBurstSnapshot(),
        historicalP1Producer: this.historicalP1Producer,
        sourcePerformance: this.executor.getSourcePerformanceSnapshot(),
      },
      liveness,
      site: this.lastSiteProbe,
    };
    this.logger.info('Lite operational progress', snapshot);
    // A compact durable marker makes a native deploy auditable even when its
    // dashboard only exposes a rolling log window. It is diagnostic state,
    // never an authority for leadership or task eligibility.
    await this.pool.query(
      `INSERT INTO settings(key,value) VALUES('importer_heartbeat',$1)
       ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
      [JSON.stringify({ ...snapshot, observedAt: new Date().toISOString() })],
    );
    this.lastFullOperationalSnapshotAt = fullSnapshotAt;
    this.heartbeatCompletedAt = new Date().toISOString();
  }

  private async handle(task: LiteTask, workerId: string, options?: { inlinePublish?: boolean }): Promise<void> {
    if (task.taskType === 'DISCOVER') return this.discover(task, workerId);
    if (task.taskType === 'CATALOG') return this.catalog(task, workerId);
    if (task.taskType === 'IMPORT_CHAPTER') return this.executor.importChapter(task, workerId, options);
    if (task.taskType === 'PUBLISH') return this.executor.publish(task, workerId);
    await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'UNKNOWN_TASK_TYPE' });
  }

  private async discover(task: LiteTask, workerId: string): Promise<void> {
    const source = String(task.payload.source || ''); const adapter = this.registry.get(source);
    if (!adapter) return void await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'SOURCE_ADAPTER_UNAVAILABLE' });
    const policy = await this.queue.sourcePolicy(source);
    // This source-wide switch is deliberately limited to P2 DISCOVER. A
    // known-work CATALOG task is handled below and must remain available for
    // P0 release checks and P1 repairs.
    if (!policy?.runnable || !policy.catalogDiscoveryEnabled) {
      const nextRunAt = policy?.cooldownUntil && policy.cooldownUntil > new Date()
        ? policy.cooldownUntil : new Date(Date.now() + (policy?.syncIntervalMs || 60 * 60_000));
      await this.queue.finish(task, workerId, { state: 'RETRY_WAIT',
        reason: !policy?.catalogDiscoveryEnabled ? 'SOURCE_DISCOVERY_DISABLED' : `SOURCE_NOT_READY:${policy?.status || 'MISSING'}`, nextRunAt });
      return;
    }
    try {
      const cursor = String(task.checkpoint.cursor || '') || null;
      const page = await withTimeout(
        adapter.fetchUpdatedWorks(cursor, { mode: 'maintenance' }),
        this.config.SOURCE_OPERATION_TIMEOUT_MS,
        'DISCOVERY_SOURCE_TIMEOUT',
      );
      let candidateWorks = page.works.slice(0, 100);
      if (cursor) {
        try {
          const frontPage = await withTimeout(
            adapter.fetchUpdatedWorks(null, { mode: 'maintenance' }),
            this.config.SOURCE_OPERATION_TIMEOUT_MS,
            'DISCOVERY_SOURCE_TIMEOUT',
          );
          const seenIds = new Set<string>();
          candidateWorks = [...frontPage.works.slice(0, 50), ...candidateWorks].filter((w) => {
            const id = String(w.sourceWorkId || '');
            if (!id || seenIds.has(id)) return false;
            seenIds.add(id);
            return true;
          }).slice(0, 100);
        } catch {
          // Best-effort front-page check when advancing a deep cursor
        }
      }
      await this.queue.recordSourceSuccess(source);
      const existingMappings = typeof (this.queue as any).existingSourceWorkMappings === 'function'
        ? await (this.queue as any).existingSourceWorkMappings(source, candidateWorks.map((w) => w.sourceWorkId))
        : new Map<string, string>();
      const p1Backlog = typeof (this.queue as any).hasExecutableOrRecoverableP1Backlog === 'function'
        ? await this.queue.hasExecutableOrRecoverableP1Backlog()
        : { pending: false, reason: null };
      this.p2SubordinationState = {
        subordinated: p1Backlog.pending,
        reason: p1Backlog.reason,
        checkedAt: new Date().toISOString(),
      };
      const catalogInputs: EnqueueLiteTask[] = [];
      for (const work of candidateWorks) {
        const mappedWorkId = existingMappings.get(String(work.sourceWorkId));
        catalogInputs.push({
          taskType: 'CATALOG',
          workId: mappedWorkId || undefined,
          workKey: mappedWorkId ? `work:${mappedWorkId}` : `source:${source}:${work.sourceWorkId}`,
          priority: 'P2',
          idempotencyKey: `lite:catalog:${source}:${work.sourceWorkId}`,
          payload: { source, sourceWorkId: work.sourceWorkId },
        });
      }
      if (catalogInputs.length > 1 && typeof (this.queue as any).enqueueMany === 'function') {
        await this.queue.enqueueMany(catalogInputs);
      } else {
        for (const item of catalogInputs) {
          await this.queue.enqueue(item);
        }
      }
      // The durable task itself holds the cursor; no new cursor rows or sweep.
      // A cursor asks for a later small batch, not an immediate unbounded
      // sweep. This leaves a one-worker cutover free to catalog materialized
      // works and prevents one source from filling every P2 admission slot.
      const effectiveNextCursor = page.nextCursor;
      const nextDelay = effectiveNextCursor
        ? Math.max(60_000, Math.min(policy.syncIntervalMs, 5 * 60_000))
        : policy.syncIntervalMs;
      await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', priority: 'P2', nextRunAt: new Date(Date.now() + nextDelay),
        checkpoint: { cursor: effectiveNextCursor }, resetAttempts: true });
    } catch (error: any) {
      await this.queue.recordSourceFailure(source, error);
      // Discovery is one durable recurring observation per source. A long
      // upstream outage must not create an unbounded task set or make that
      // source permanently invisible; its retry cadence is capped instead.
      await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: String(error?.message || 'DISCOVERY_FAILED'),
        nextRunAt: new Date(Date.now() + this.recurringRetryDelayMs(task.attempts)) });
    }
  }

  private async catalog(task: LiteTask, workerId: string, metadataTimings?: Record<string, number | null>): Promise<void> {
    const source = String(task.payload.source || ''); const sourceWorkId = String(task.payload.sourceWorkId || '');
    if (task.payload.metadataCrossSourceDiscovery) {
      return this.withCatalogLeaseHeartbeat(task, workerId, () => this.discoverCrossSourceMappings(task, workerId));
    }
    if (task.payload.metadataEnrichment) return this.enrichStoredMappingMetadata(task, workerId, source, sourceWorkId);
    if (task.payload.metadataSourceRefresh) return this.refreshMappingMetadataFromSource(task, workerId, source, sourceWorkId, metadataTimings);
    const adapter = this.registry.get(source);
    const isInvalidNexusId = source === 'nexus' && !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(sourceWorkId);
    if (!adapter || !sourceWorkId || isInvalidNexusId) return void await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'CATALOG_INPUT_INVALID' });
    // A previously claimed monitor may use idle primary capacity only while
    // the Reader is healthy; otherwise retain the isolated metadata pool.
    const monitorQueue = task.monitoring ? this.monitorSqlQueue() : this.queue;
    const policy = await monitorQueue.sourcePolicy(source);
    // `catalogDiscoveryEnabled` controls source-wide P2 DISCOVER only. A
    // CATALOG task already has a confirmed source-work mapping: blocking it
    // because the provider opts out of new-work discovery also blocks P0
    // release checks (and P1 repairs) for existing Reader works.
    if (!policy?.runnable) {
      const policyStatus = String(policy?.status || '').toUpperCase();
      if (!task.monitoring && policy && (policyStatus === 'DISABLED'
        || (!policy.enabled && policyStatus !== 'PAUSED') || !policy.chapterIngestionEnabled)) {
        await this.queue.finish(task, workerId, {
          state: 'BLOCKED',
          reason: 'SOURCE_DISABLED',
        });
        return;
      }
      const nextRunAt = policy?.cooldownUntil && policy.cooldownUntil > new Date()
        ? policy.cooldownUntil : new Date(Date.now() + (policy?.syncIntervalMs || 60 * 60_000));
      await this.queue.finish(task, workerId, {
        state: task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
        reason: `SOURCE_NOT_READY:${policy?.status || 'MISSING'}`, nextRunAt,
        priority: task.monitoring ? 'P2' : undefined, monitoring: task.monitoring || undefined,
      });
      return;
    }
    let fetchedSnapshot = false;
    try {
      const sourceAbort = new AbortController();
      let snapshot;
      try {
        snapshot = await withTimeout(adapter.fetchWorkSnapshot
          ? adapter.fetchWorkSnapshot(sourceWorkId, sourceAbort.signal)
          : Promise.all([adapter.fetchWorkDetails(sourceWorkId), adapter.fetchChapters(sourceWorkId)])
            .then(([details, chapters]) => ({ details, chapters })),
        this.config.SOURCE_OPERATION_TIMEOUT_MS,
        'CATALOG_SOURCE_TIMEOUT');
      } finally {
        sourceAbort.abort();
      }
      fetchedSnapshot = true;

      const fpLanguage = String(snapshot.details.language || (snapshot.details.raw as any)?.language || 'pt-BR');
      const fpEdition = String(snapshot.details.edition || (snapshot.details.raw as any)?.edition || 'default');
      const fpStatus = String(snapshot.details.status || 'UNKNOWN').toUpperCase();
      const seenFpChapterIds = new Set<string>();
      const fpChapters: Array<[string, string, string]> = [];
      for (const ch of snapshot.chapters) {
        const cid = String(ch.sourceChapterId || '');
        if (!cid || seenFpChapterIds.has(cid)) continue;
        seenFpChapterIds.add(cid);
        fpChapters.push([cid, String(ch.displayNumber || ch.number), String(ch.title || '')]);
      }
      const manifestFingerprint = createHash('sha256')
        .update(JSON.stringify({
          v: 1,
          status: fpStatus,
          language: fpLanguage,
          edition: fpEdition,
          ingest: Boolean(policy.chapterIngestionEnabled),
          chapters: fpChapters,
        }))
        .digest('hex');

      const finishUnchangedMonitor = async (knownReleaseAt: string | null): Promise<void> => {
        const monitored = fpStatus !== 'CANCELLED';
        let chapterReleaseAt: string | null = knownReleaseAt || (task.checkpoint?.lastReleaseAt as string) || null;
        if (!chapterReleaseAt && Array.isArray(snapshot.chapters)) {
          for (const ch of snapshot.chapters) {
            if (ch.createdAt && (!chapterReleaseAt || ch.createdAt > chapterReleaseAt)) {
              chapterReleaseAt = ch.createdAt;
            }
          }
        }
        const checkedAt = new Date().toISOString();
        if (monitored) {
          await this.monitorFinishBatcher.submit({
            task, workerId,
            nextRunAt: new Date(Date.now() + this.monitorIntervalMs(task, fpStatus, 0, chapterReleaseAt)),
            checkpoint: {
              workId: task.workId!,
              ...this.successfulMonitorCheckpoint(task, checkedAt),
              lastNewReleaseCount: 0,
              lastReleaseAt: chapterReleaseAt,
              manifestFingerprint,
              emptyChecks: Number(task.checkpoint.emptyChecks || 0) + 1,
            },
          });
        } else {
          await this.queue.finish(task, workerId, {
            state: 'DONE', workId: task.workId!,
            checkpoint: { workId: task.workId!, ...this.successfulMonitorCheckpoint(task, checkedAt), manifestFingerprint },
            resetAttempts: true,
          });
        }
        await this.queue.recordSourceSuccess(source);
      };

      if (
        task.monitoring === true
        && Boolean(task.workId)
        && !task.checkpoint?.presentationIncomplete
        && typeof task.checkpoint?.manifestFingerprint === 'string'
        && task.checkpoint.manifestFingerprint === manifestFingerprint
      ) {
        await finishUnchangedMonitor(null);
        return;
      }

      if (
        !(process.env.NODE_ENV === 'test' || process.env.VITEST)
        && task.monitoring === true
        && Boolean(task.workId)
        && !task.checkpoint?.presentationIncomplete
        && !task.checkpoint?.manifestFingerprint
        && fpChapters.length > 0
      ) {
        try {
          const checkPool = this.mayBorrowPrimaryForMonitor(2) ? this.pool : (this.sourceMetadataPool || this.pool);
          const dbManifest = await checkPool.query<{
            work_status: string;
            has_presentation: boolean;
            matched_chapters: number;
            has_presentation_blocked_tasks: boolean;
            latest_release_at: Date | null;
          }>(`
            WITH work_known_chapters AS MATERIALIZED (
              SELECT lc.canonical_chapter_key
              FROM importer_lite_chapters lc
              JOIN chapters c
                ON c.id = lc.chapter_id
               AND c.published_at IS NOT NULL
              WHERE lc.work_id = $1::uuid
              UNION
              SELECT t.chapter_key
              FROM importer_lite_tasks t
              WHERE t.work_id = $1::uuid
                AND t.chapter_key IS NOT NULL
                AND t.task_type IN ('IMPORT_CHAPTER', 'PUBLISH')
                AND t.state IN ('READY', 'RUNNING', 'RETRY_WAIT', 'DONE')
            ),
            matched_sources AS MATERIALIZED (
              SELECT cs.source_chapter_id
              FROM importer_lite_chapter_sources cs
              JOIN work_known_chapters kc
                ON kc.canonical_chapter_key = cs.canonical_chapter_key
              WHERE cs.work_id = $1::uuid
                AND cs.source = $2
                AND cs.source_work_id = $3
                AND cs.confirmed = true
                AND COALESCE(cs.has_chapter, true) = true
              UNION
              SELECT cm.source_chapter_id
              FROM importer_chapter_mappings cm
              JOIN chapters c
                ON c.id = cm.chapter_id
               AND c.published_at IS NOT NULL
              WHERE cm.work_id = $1::uuid
                AND cm.source = $2
                AND cm.source_chapter_id = ANY($4::text[])
            )
            SELECT
              UPPER(COALESCE(w.status, 'UNKNOWN')) AS work_status,
              (
                w.title IS NOT NULL
                AND w.slug IS NOT NULL
                AND w.cover_id IS NOT NULL
                AND EXISTS (
                  SELECT 1 FROM media m
                  WHERE m.id = w.cover_id
                    AND m.storage_ready = true
                    AND COALESCE(m.bytes, 0) >= 1500
                )
              ) AS has_presentation,
              (SELECT count(*)::int
               FROM unnest($4::text[]) AS req(scid)
               WHERE EXISTS (
                 SELECT 1 FROM matched_sources ms WHERE ms.source_chapter_id = req.scid
               )) AS matched_chapters,
              EXISTS (
                SELECT 1 FROM importer_lite_tasks bt
                WHERE bt.work_id = $1::uuid
                  AND bt.state = 'BLOCKED'
                  AND bt.reason = 'WORK_PRESENTATION_INCOMPLETE'
              ) AS has_presentation_blocked_tasks,
              (SELECT max(COALESCE(c.published_at, c.created_at))
               FROM chapters c
               WHERE c.work_id = $1::uuid) AS latest_release_at
            FROM works w
            WHERE w.id = $1::uuid
          `, [task.workId, source, sourceWorkId, fpChapters.map((c) => c[0])]);
          const row = dbManifest.rows[0];
          if (
            row
            && row.has_presentation
            && !row.has_presentation_blocked_tasks
            && fpStatus !== 'CANCELLED'
            && Number(row.matched_chapters) === fpChapters.length
          ) {
            await finishUnchangedMonitor(row.latest_release_at ? row.latest_release_at.toISOString() : null);
            return;
          }
        } catch {
          // Fall through to full materialization pass on any precheck error
        }
      }
      const rawDetails = snapshot.details.raw && typeof snapshot.details.raw === 'object' ? snapshot.details.raw : {};
      // Persist precisely the provider-declared fields alongside its raw
      // payload.  Some adapters expose these normalized arrays without
      // duplicating them in `raw`; losing them here used to make a successful
      // normal onboarding look unclassified to the retroactive producer.
      const sourceMetadata: Record<string, unknown> = {
        ...rawDetails,
        ...(snapshot.details.alternativeTitles !== undefined ? { alternativeTitles: snapshot.details.alternativeTitles } : {}),
        ...(snapshot.details.genres !== undefined ? { genres: snapshot.details.genres } : {}),
      };
      const sourceMetadataClassification = classifyExplicitSourceMetadata(sourceMetadata, snapshot.details.title);
      const rawContentRating = String((rawDetails as any).contentRating || (rawDetails as any).content_rating || '').toUpperCase();
      const resolved = await this.dedupe.resolveWork({ source, sourceWorkId, title: snapshot.details.title, slug: snapshot.details.slug,
        aliases: sourceMetadataClassification.aliases, genres: sourceMetadataClassification.genres, author: snapshot.details.author,
        artist: snapshot.details.artist, synopsis: sanitizeSynopsis(snapshot.details.synopsis) || undefined, kind: snapshot.details.kind,
        status: snapshot.details.status, year: snapshot.details.year, ageRating: snapshot.details.ageRating,
        contentRating: rawContentRating === 'ADULT_18' || Number(snapshot.details.ageRating || 0) >= 18 ? 'ADULT_18' : undefined,
        rawMetadata: sourceMetadata,
        loadCoverId: snapshot.details.coverUrl ? async () => {
          const primaryId = await this.loadCover(adapter, snapshot.details.coverUrl!);
          if (primaryId) return primaryId;
          const fallbackUrl = typeof (rawDetails as any).fallbackCoverUrl === 'string'
            ? (rawDetails as any).fallbackCoverUrl
            : null;
          if (fallbackUrl && fallbackUrl !== snapshot.details.coverUrl) {
            return this.loadCover(adapter, fallbackUrl);
          }
          return null;
        } : undefined } as any);
      if (!resolved.workId || resolved.status === 'AMBIGUOUS') {
        await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: resolved.reason || 'AMBIGUOUS_CATALOG_MATCH' }); return;
      }
      // Metadata is part of normal onboarding, not a later backfill.  The
      // durable v3 classification makes this exact mapping ineligible for a
      // redundant source refetch, while preserving source-provided raw fields
      // and product deltas for observability.
      await this.queue.recordSourceMetadataEnrichment({
        source, sourceWorkId, workId: resolved.workId,
        aliasesState: sourceMetadataClassification.aliasesState,
        tagsState: sourceMetadataClassification.tagsState,
        aliasesAdded: Math.max(0, Number(resolved.metadataDelta?.aliasesAdded || 0)),
        tagsAdded: Math.max(0, Number(resolved.metadataDelta?.tagsAdded || 0)),
        error: sourceMetadataClassification.parserError ? 'EXPLICIT_SOURCE_METADATA_NOT_PARSED' : null,
        rawMetadata: sourceMetadata,
      });
      if (sourceMetadataClassification.parserError) {
        await this.queue.finish(task, workerId, {
          state: 'BLOCKED', reason: 'CATALOG_METADATA_PARSER_ERROR', workId: resolved.workId,
          checkpoint: { workId: resolved.workId, metadataParserError: true },
        });
        return;
      }
      // A first source is only the beginning of a new work's metadata.  Run
      // the bounded cross-source pass before this catalog task materializes
      // chapters/publication, so aliases and explicit tags do not wait for a
      // future retroactive sweep.  Existing mappings already take the normal
      // append-only merge path above when they arrive later.
      if (resolved.status === 'NEW_WORK') {
        const onboardingAliases = [...sourceMetadataClassification.aliases];
        let initialNotFoundAliasOnly: Array<'mangadex' | 'mangafire' | 'mangaball'> = [];
        if (process.env.NODE_ENV !== 'test' || (this as any).enableOnboardingAliasOnlyInTest) {
          try {
            const aliasOnlyTarget = {
              id: resolved.workId,
              title: snapshot.details.title,
              slug: snapshot.details.slug,
              aliases: onboardingAliases,
              author: snapshot.details.author ?? null,
              artist: snapshot.details.artist ?? null,
              kind: snapshot.details.kind ?? null,
              year: snapshot.details.year ?? null,
              ageRating: snapshot.details.ageRating ?? null,
              contentRating: rawContentRating === 'ADULT_18' || Number(snapshot.details.ageRating || 0) >= 18 ? 'ADULT_18' : null,
            };
            const aliasOnlyOutcome = await withTimeout(
              this.aliasOnlyService.enrichWork(aliasOnlyTarget),
              20_000,
              'ONBOARDING_ALIAS_ONLY_TIMEOUT',
            );
            await this.aliasOnlyRepo.persistBatchResults('runtime_onboarding_v1', [{
              target: aliasOnlyTarget,
              priorityBucket: 1,
              result: aliasOnlyOutcome,
            }]);
            for (const added of aliasOnlyOutcome.addedAliases) {
              if (!onboardingAliases.includes(added)) onboardingAliases.push(added);
            }
            initialNotFoundAliasOnly = (['mangadex', 'mangafire', 'mangaball'] as const).filter(
              (src) => aliasOnlyOutcome.perSource[src]?.status === 'NOT_FOUND'
            );
          } catch (error: any) {
            this.logger.warn('Onboarding ALIAS_ONLY pass deferred to background turn', {
              workId: resolved.workId, error: error?.message,
            });
            this.triggerAsyncAliasOnlyEnrichment(resolved.workId);
          }
        } else {
          this.triggerAsyncAliasOnlyEnrichment(resolved.workId);
        }

        const complementaryOutcome = await withTimeout(
          this.enrichNewWorkFromComplementarySources({
            workId: resolved.workId, originSource: source, title: snapshot.details.title, slug: snapshot.details.slug,
            aliases: onboardingAliases, author: snapshot.details.author, artist: snapshot.details.artist,
            synopsis: sanitizeSynopsis(snapshot.details.synopsis) || undefined, kind: snapshot.details.kind, year: snapshot.details.year,
            allowFallbackDetailProbe: true,
          }),
          45_000,
          'ONBOARDING_COMPLEMENTARY_TIMEOUT',
        ).catch((error: any) => {
          this.logger.warn('Onboarding complementary cross-source pass timed out or failed; deferring to V7 background lane', {
            workId: resolved.workId, source, error: error?.message,
          });
          return {
            busy: true,
            incomplete: true,
            attemptedSources: [] as string[],
            deferredSources: [] as string[],
            attached: [] as Array<{ source: string; sourceWorkId: string; aliasesAdded: number; tagsAdded: number }>,
            sourceProbes: [] as Array<{ source: string; outcome: string; detail?: string }>,
            discoveredAliases: [] as string[],
            matchedCovers: [] as Array<{ source: string; coverUrl: string; isBrazilianSource: boolean }>,
          };
        });

        if (typeof (this.queue as any).recordOnboardingCrossSourceDiscovery === 'function') {
          await (this.queue as any).recordOnboardingCrossSourceDiscovery({
            workId: resolved.workId,
            originSource: source,
            busy: complementaryOutcome.busy,
            incomplete: complementaryOutcome.incomplete,
            attemptedSources: complementaryOutcome.attemptedSources,
            deferredSources: complementaryOutcome.deferredSources,
            attached: complementaryOutcome.attached,
            sourceProbes: complementaryOutcome.sourceProbes,
          }).catch((error: any) => {
            this.logger.warn('Failed to persist onboarding cross-source V7 record', {
              workId: resolved.workId, error: error?.message,
            });
          });
        }

        const complementaryAddedAliases = complementaryOutcome.attached.reduce((sum, item) => sum + (item.aliasesAdded || 0), 0);
        if (complementaryOutcome.attached.length > 0 && fpStatus !== 'CANCELLED') {
          try {
            // The discovered ID may already belong to a different canonical
            // work. Enrol only mappings whose persisted owner is this work.
            await this.queue.enrollConfirmedP0Mappings(resolved.workId, complementaryOutcome.attached);
          } catch (error: any) {
            this.logger.warn('Immediate onboarding monitor enrollment deferred', { workId: resolved.workId, error: error?.message });
          }
        }
        for (const extra of complementaryOutcome.discoveredAliases || []) {
          if (!onboardingAliases.includes(extra)) onboardingAliases.push(extra);
        }
        if (
          (complementaryAddedAliases > 0 || (complementaryOutcome.discoveredAliases?.length || 0) > 0) &&
          initialNotFoundAliasOnly.length > 0 &&
          (process.env.NODE_ENV !== 'test' || (this as any).enableOnboardingAliasOnlyInTest)
        ) {
          try {
            const wave2Target = {
              id: resolved.workId,
              title: snapshot.details.title,
              slug: snapshot.details.slug,
              aliases: onboardingAliases,
              author: snapshot.details.author ?? null,
              artist: snapshot.details.artist ?? null,
              kind: snapshot.details.kind ?? null,
              year: snapshot.details.year ?? null,
              ageRating: snapshot.details.ageRating ?? null,
              contentRating: rawContentRating === 'ADULT_18' || Number(snapshot.details.ageRating || 0) >= 18 ? 'ADULT_18' : null,
            };
            const wave2Outcome = await withTimeout(
              this.aliasOnlyService.enrichWork(wave2Target, initialNotFoundAliasOnly),
              20_000,
              'ONBOARDING_WAVE2_ALIAS_ONLY_TIMEOUT',
            );
            await this.aliasOnlyRepo.persistBatchResults('runtime_onboarding_v1', [{
              target: wave2Target,
              priorityBucket: 1,
              result: wave2Outcome,
            }]);
          } catch (error: any) {
            this.logger.warn('Wave 2 onboarding ALIAS_ONLY pass deferred to background turn', {
              workId: resolved.workId, error: error?.message,
            });
            this.triggerAsyncAliasOnlyEnrichment(resolved.workId);
          }
        }

        // Permanent Visual Cover Inspection for Brazilian Scans:
        // If the work still has an international title and no PT-BR alias was found in metadata across all sources,
        // inspect cover images from confirmed Brazilian complementary sources and the origin source.
        try {
          const workTitleCheck = await (this.sourceMetadataPool || this.pool).query<{
            id: string;
            title: string;
            slug: string;
            aliases: string[];
            metadata_provenance: any;
          }>(
            `SELECT id, title, slug, aliases, metadata_provenance FROM works WHERE id = $1::uuid LIMIT 1`,
            [resolved.workId],
          );
          const currentWork = workTitleCheck.rows[0];
          if (currentWork && !isPtBrTitle(currentWork.title)) {
            const hasPtBrAlias = (currentWork.aliases || []).some((a) => isPtBrTitle(a));
            if (!hasPtBrAlias) {
              const originIsBrazilian = !['nexus', 'mangadex', 'mangafire', 'mangaball'].includes(source);
              const complementaryCovers = complementaryOutcome.matchedCovers || [];

              // Priority order:
              // 1. Confirmed Brazilian complementary sources
              // 2. Brazilian origin source
              // 3. Other confirmed complementary sources
              // 4. Foreign origin source
              const candidateCovers: Array<{ source: string; coverUrl: string }> = [];
              for (const c of complementaryCovers.filter((cov) => cov.isBrazilianSource)) {
                candidateCovers.push({ source: c.source, coverUrl: c.coverUrl });
              }
              if (originIsBrazilian && snapshot.details.coverUrl) {
                candidateCovers.push({ source, coverUrl: snapshot.details.coverUrl });
              }
              for (const c of complementaryCovers.filter((cov) => !cov.isBrazilianSource)) {
                candidateCovers.push({ source: c.source, coverUrl: c.coverUrl });
              }
              if (!originIsBrazilian && snapshot.details.coverUrl) {
                candidateCovers.push({ source, coverUrl: snapshot.details.coverUrl });
              }

              // Deduplicate candidate covers by URL
              const seenUrls = new Set<string>();
              const uniqueCovers = candidateCovers.filter((c) => {
                if (!c.coverUrl || seenUrls.has(c.coverUrl)) return false;
                seenUrls.add(c.coverUrl);
                return true;
              });

              if (uniqueCovers.length > 0) {
                await this.coverVisionQueue.enqueueTask(resolved.workId, uniqueCovers);
                this.triggerAsyncCoverVision(resolved.workId);
              }
            }
          }
        } catch (visionErr: any) {
          this.logger.warn('Cover vision enqueueing failed gracefully', {
            workId: resolved.workId,
            error: visionErr?.message || visionErr,
          });
        }
      }
      const language = String(snapshot.details.language || (snapshot.details.raw as any)?.language || 'pt-BR');
      const edition = String(snapshot.details.edition || (snapshot.details.raw as any)?.edition || 'default');
      // A recurring monitor is intentionally separate from P0 *execution*.
      // P0 only accelerates releases beyond the durable manifest frontier.
      // A source can reveal historical chapters later; those are P1 repairs,
      // not P0, without inferring that every numeric gap is a missing chapter.
      const p0Monitoring = task.monitoring || task.priority === 'P0' || task.priority === 'STAFF';
      const knownFrontier = p0Monitoring
        ? await this.getAvailablePool().query<{ order_key: string | null }>(`SELECT max(order_key) AS order_key
          FROM importer_lite_chapter_sources WHERE work_id=$1::uuid AND order_key LIKE 'n|%'`, [resolved.workId])
        : { rows: [{ order_key: null }] };
      const knownNormalOrder = knownFrontier.rows[0]?.order_key || null;
      // A manifest can reveal a long historical interval on the first pass
      // or after the source repairs an incomplete listing. Only its newest
      // normal chapter is a P0 release; earlier positively listed chapters
      // enter P1 together so the importer cannot keep moving the gap forward.
      let hasNonNormalChapter = false;
      const normalOrderKeys = snapshot.chapters
        .map((chapter) => {
          const displayNumber = chapter.displayNumber || String(chapter.number);
          const orderKey = chapterOrderKey({
            displayNumber,
            title: chapter.title || '',
            kind: chapterKind({ displayNumber, title: chapter.title || '' }),
          });
          if (!orderKey.startsWith('n|')) hasNonNormalChapter = true;
          return orderKey;
        })
        .filter((orderKey) => orderKey.startsWith('n|'))
        .sort();
      const newestManifestNormalOrder = normalOrderKeys.at(-1) || null;
      const oldestManifestNormalOrder = normalOrderKeys[0] || null;
      const hasCandidateBeyondKnownFrontier = hasNonNormalChapter
        || !knownNormalOrder
        || (newestManifestNormalOrder !== null && newestManifestNormalOrder > knownNormalOrder);
      const workHasPendingBacklog = (p0Monitoring
        && resolved.status !== 'NEW_WORK'
        && task.priority !== 'STAFF'
        && hasCandidateBeyondKnownFrontier
        && typeof (this.queue as any).hasPendingWorkChapterBacklog === 'function')
        ? await this.queue.hasPendingWorkChapterBacklog(resolved.workId)
        : false;
      const manifestSourceIds = [...new Set(snapshot.chapters
        .map((chapter) => String(chapter.sourceChapterId || ''))
        .filter(Boolean))];
      const legacyPublished = await this.legacyPublishedChapterMap(source, resolved.workId, manifestSourceIds);
      const legacyPublishedWholeNumbers = await this.legacyPublishedWholeNumberMap(resolved.workId, snapshot.chapters.map((chapter) => {
        const displayNumber = chapter.displayNumber || String(chapter.number);
        const kind = chapterKind({ displayNumber, title: chapter.title });
        return canonicalChapterKey({ displayNumber, title: chapter.title, kind, language, edition });
      }));
      const sourceRows: Array<{
        workId: string; chapterKey: string; source: string; sourceWorkId: string; sourceChapterId: string;
        language: string; edition: string; orderKey: string; sourceRank: number;
      }> = [];
      const legacyByChapterKey = new Map<string, {
        workId: string; chapterKey: string; chapterId: string; displayNumber: string;
        orderKey: string; language: string; edition: string;
      }>();
      const chapterTaskByKey = new Map<string, EnqueueLiteTask>();
      const sourceChapterIds = new Set<string>();
      for (const chapter of snapshot.chapters) {
        // A duplicate manifest entry cannot be allowed to make one batched
        // UPSERT fail and delay every other chapter in this catalog pass.
        if (sourceChapterIds.has(chapter.sourceChapterId)) continue;
        sourceChapterIds.add(chapter.sourceChapterId);
        const displayNumber = chapter.displayNumber || String(chapter.number);
        const chapterKindValue = chapterKind({ displayNumber, title: chapter.title });
        const chapterKey = canonicalChapterKey({ displayNumber, title: chapter.title, kind: chapterKindValue, language, edition });
        const orderKey = chapterOrderKey({ displayNumber, title: chapter.title, kind: chapterKindValue });
        const historicalRepair = p0Monitoring && orderKey.startsWith('n|') && Boolean(knownNormalOrder && orderKey <= knownNormalOrder);
        const isNewestManifestRelease = !orderKey.startsWith('n|') || orderKey === newestManifestNormalOrder;
        const isNewWorkOpeningRelease = resolved.status === 'NEW_WORK' && (
          oldestManifestNormalOrder
            ? orderKey === oldestManifestNormalOrder
            : sourceChapterIds.size === 1
        );
        const chapterPriority = ((p0Monitoring && !historicalRepair && isNewestManifestRelease && !workHasPendingBacklog) || isNewWorkOpeningRelease) ? 'P0' : 'P1';
        sourceRows.push({ workId: resolved.workId, chapterKey, source, sourceWorkId, sourceChapterId: chapter.sourceChapterId,
          language, edition, orderKey, sourceRank: rankForSource(source) });
        const legacyChapterId = legacyPublished.get(String(chapter.sourceChapterId))
          || legacyPublishedWholeNumbers.get(chapterKey);
        if (legacyChapterId) {
          // The existing Reader publication remains authoritative.  This only
          // gives Lite its canonical identity, so a future catalog pass cannot
          // spend a P1 slot rediscovering the same source chapter.
          // Multiple provider entries can normalize to one canonical chapter.
          // Keep the first durable legacy binding; alternatives remain in
          // importer_lite_chapter_sources and must never create a second
          // Reader chapter or a multi-row UPSERT collision.
          if (!legacyByChapterKey.has(chapterKey)) {
            legacyByChapterKey.set(chapterKey, { workId: resolved.workId, chapterKey, chapterId: legacyChapterId, displayNumber, orderKey, language, edition });
          }
        } else if (policy.chapterIngestionEnabled) {
          const candidate: EnqueueLiteTask = { taskType: 'IMPORT_CHAPTER', workId: resolved.workId, workKey: `work:${resolved.workId}`,
            chapterKey, priority: chapterPriority, idempotencyKey: `lite:chapter:${resolved.workId}:${chapterKey}`,
            payload: { displayNumber, chapterTitle: chapter.title || '', chapterKind: chapterKindValue, orderKey, language, edition,
              ...(p0Monitoring && orderKey.startsWith('n|') && orderKey < String(newestManifestNormalOrder || '')
                ? { historicalManifestRepair: true } : {}) } };
          if (!chapterTaskByKey.has(candidate.idempotencyKey)) chapterTaskByKey.set(candidate.idempotencyKey, candidate);
        }
      }
      const legacyRows = [...legacyByChapterKey.values()];
      // A mapped legacy publication is authoritative even if another source
      // entry for the same canonical identity was not mapped yet.
      const chapterTasks = [...chapterTaskByKey.values()].filter((candidate) => !legacyByChapterKey.has(String(candidate.chapterKey)));
      if (resolved.status !== 'NEW_WORK' && task.priority !== 'STAFF') {
        const hasUnpublishedFrontierPredecessor = workHasPendingBacklog || chapterTasks.some((candidate) => {
          const candidateOrder = String(candidate.payload?.orderKey || '');
          return candidateOrder.startsWith('n|')
            && Boolean(newestManifestNormalOrder)
            && candidateOrder < String(newestManifestNormalOrder)
            && (!knownNormalOrder || candidateOrder > knownNormalOrder);
        });
        if (hasUnpublishedFrontierPredecessor) {
          for (const candidate of chapterTasks) {
            if (candidate.priority === 'P0') candidate.priority = 'P1';
          }
        }
      }
      await this.upsertChapterSources(sourceRows);
      await this.upsertLegacyChapterMappings(legacyRows);
      // Do not spend download, Telegram and YSQL capacity on a chapter that
      // cannot legally become Reader-visible.  Source mappings remain durable
      // and this catalog task retries on its bounded cadence, so a later valid
      // cover (from this or another confirmed source) admits the same manifest
      // without inventing metadata or losing the historical repair.
      if (chapterTasks.length && !await this.queue.workPresentationReady(resolved.workId)) {
        await this.queue.parkUnpublishableWork(resolved.workId, `work:${resolved.workId}`);
        const status = String(snapshot.details.status || 'UNKNOWN').toUpperCase();
        const monitored = task.monitoring || task.priority === 'P0' || task.priority === 'STAFF';
        await this.queue.finish(task, workerId, monitored ? {
          state: 'RETRY_WAIT', priority: 'P2', monitoring: true,
          nextRunAt: new Date(Date.now() + this.monitorIntervalMs(task, status, 0)),
          workId: resolved.workId,
          checkpoint: { workId: resolved.workId, presentationIncomplete: true, ...this.successfulMonitorCheckpoint(task, new Date().toISOString()) },
          resetAttempts: true,
        } : {
          state: task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
          reason: 'WORK_PRESENTATION_INCOMPLETE',
          nextRunAt: new Date(Date.now() + this.recurringRetryDelayMs(task.attempts)),
          workId: resolved.workId,
          checkpoint: { workId: resolved.workId, presentationIncomplete: true, ...this.successfulMonitorCheckpoint(task, new Date().toISOString()) },
        });
        await this.queue.recordSourceSuccess(source);
        return;
      }
      const materialized = await this.queue.enqueueMany(chapterTasks);
      const newReleaseCount = chapterTasks.filter((candidate) =>
        candidate.priority === 'P0' && materialized.insertedKeys.has(candidate.idempotencyKey),
      ).length;
      // A newly confirmed mapping (or repaired presentation metadata) is
      // evidence worth re-evaluating. This indexed, per-work wake-up replaces
      // the legacy global blocked-task sweeps.
      if (policy.chapterIngestionEnabled) await this.queue.wakeCatalogBlocked(resolved.workId, `work:${resolved.workId}`);
      // Active mapped works retain their persistent P0 monitor. Only explicitly
      // CANCELLED works drop monitoring. Completed works remain monitored so post-completion chapters and side
      // stories are detected on the same cadence as other eligible works.
      const status = String(snapshot.details.status || 'UNKNOWN').toUpperCase();
      const monitored = status !== 'CANCELLED';
      let chapterReleaseAt: string | null = (task.checkpoint?.lastReleaseAt as string) || null;
      if (newReleaseCount > 0) {
        chapterReleaseAt = new Date().toISOString();
      } else if (!chapterReleaseAt && Array.isArray(snapshot.chapters)) {
        for (const ch of snapshot.chapters) {
          if (ch.createdAt && (!chapterReleaseAt || ch.createdAt > chapterReleaseAt)) {
            chapterReleaseAt = ch.createdAt;
          }
        }
      }
      if (!chapterReleaseAt && resolved.workId) {
        try {
          const latestDbRelease = await this.getAvailablePool().query<{ release_at: Date }>(
            'SELECT max(COALESCE(published_at, created_at)) AS release_at FROM chapters WHERE work_id=$1::uuid',
            [resolved.workId],
          );
          if (latestDbRelease.rows[0]?.release_at) {
            chapterReleaseAt = latestDbRelease.rows[0].release_at.toISOString();
          }
        } catch {}
      }
      const checkedAt = new Date().toISOString();
      await this.queue.finish(task, workerId, monitored ? {
        state: 'RETRY_WAIT', priority: 'P2', monitoring: true,
        nextRunAt: new Date(Date.now() + this.monitorIntervalMs(task, status, newReleaseCount, chapterReleaseAt)),
        workId: resolved.workId,
        checkpoint: {
          workId: resolved.workId, ...this.successfulMonitorCheckpoint(task, checkedAt),
          lastNewReleaseCount: newReleaseCount,
          lastReleaseAt: chapterReleaseAt,
          manifestFingerprint,
          presentationIncomplete: false,
          emptyChecks: newReleaseCount ? 0 : Number(task.checkpoint.emptyChecks || 0) + 1,
        },
        resetAttempts: true,
      } : { state: 'DONE', workId: resolved.workId, checkpoint: { workId: resolved.workId, ...this.successfulMonitorCheckpoint(task, checkedAt), manifestFingerprint }, resetAttempts: true });
      await this.queue.recordSourceSuccess(source);
    } catch (error: any) {
      const monitorRetry = task.monitoring;
      // Keep a slow health write from occupying every P0 slot after an HTTP
      // timeout. Successful observations still own their durable finish.
      if (!fetchedSnapshot) {
        if (monitorRetry) void this.queue.recordSourceFailure(source, error).catch((failure: any) =>
          this.logger.warn('Monitor source health update deferred', { source, error: failure?.message }));
        else await this.queue.recordSourceFailure(source, error);
      }
      const retryDelay = monitorRetry ? this.recurringRetryDelayMs(task.attempts) : 60_000;
      // A monitoring row is an ongoing P0 observation, not a failed chapter.
      // Keep exactly one durable row with a capped cadence; its successful
      // next observation resets the failure window. A manually-awakened
      // STAFF catalog task is no longer marked monitoring and still obtains a
      // visible terminal block after its normal bounded attempts.
      // A retry is not a fresh release. Retain the normal monitor rank so a
      // failed watch cannot repeatedly outrank older never-checked works.
      await (monitorRetry ? this.monitorSqlQueue() : this.queue).finish(task, workerId, { state: monitorRetry ? 'RETRY_WAIT' : task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
        reason: String(error?.message || 'CATALOG_FAILED').slice(0,500), nextRunAt: new Date(Date.now() + retryDelay),
        priority: monitorRetry ? 'P2' : undefined, monitoring: monitorRetry || undefined,
        checkpoint: monitorRetry ? {
          monitorFailures: Number(task.checkpoint.monitorFailures || 0) + 1,
          lastAttemptedAt: new Date().toISOString(),
          lastError: String(error?.message || 'CATALOG_FAILED').slice(0, 240),
        } : undefined });
    }
  }

  /**
   * A bounded batch of independent work/provider probes. The task keeps its
   * source coverage in its checkpoint, so a restart cannot turn catalog-wide
   * search into an unbounded in-memory sweep. A rejected or unavailable
   * provider is evidence, never a reason to attach a fuzzy match.
   */
  private async discoverCrossSourceMappings(task: LiteTask, workerId: string): Promise<void> {
    if (!task.workId) {
      await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'CROSS_SOURCE_WORK_MISSING' });
      return;
    }
    const work = await this.getAvailablePool().query<{
      id: string; title: string; slug: string; aliases: string[]; author: string | null; artist: string | null;
      synopsis: string | null; kind: string | null; year: number | null;
    }>(`SELECT id::text,title,slug,aliases,author,artist,COALESCE(synopsis,description) AS synopsis,kind,year
      FROM works
      WHERE id=$1::uuid
        AND (published=true OR EXISTS (SELECT 1 FROM importer_work_mappings m WHERE m.work_id=works.id))
      LIMIT 1`, [task.workId]);
    const target = work.rows[0];
    if (!target) {
      await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'CROSS_SOURCE_WORK_NOT_PUBLIC' });
      return;
    }
    const normalizeList = (value: unknown): string[] => Array.isArray(value)
      ? [...new Set(value.map((entry) => String(entry || '').trim()).filter(Boolean))].slice(0, 64) : [];
    const checked = new Set(normalizeList(task.checkpoint.checkedSources));
    const deferred = new Set(normalizeList(task.checkpoint.deferredSources));
    const matched = Array.isArray(task.checkpoint.matchedSources) ? task.checkpoint.matchedSources.slice(0, 64) : [];
    const sources = await this.queue.crossSourceDiscoverySources(target.id);
    const uncheckedSources = sources.filter((candidate) => !checked.has(candidate));
    const rotationOffset = uncheckedSources.length > 0
      ? parseInt(target.id.replace(/-/g, '').slice(0, 4), 16) % uncheckedSources.length
      : 0;
    const rotatedSources = rotationOffset > 0
      ? [...uncheckedSources.slice(rotationOffset), ...uncheckedSources.slice(0, rotationOffset)]
      : uncheckedSources;
    const sourceBatch = rotatedSources.slice(0, CROSS_SOURCE_DISCOVERY_SOURCES_PER_TURN);
    const aliasesAdded = Math.max(0, Number(task.checkpoint.aliasesAdded || 0));
    const tagsAdded = Math.max(0, Number(task.checkpoint.tagsAdded || 0));
    if (!sourceBatch.length) {
      const deferredRounds = Math.max(0, Number(task.checkpoint.deferredRounds || 0));
      await this.queue.finish(task, workerId, {
        state: 'DONE', resetAttempts: true,
        // This remains a partial result, not a synthetic NO_ALIASES verdict.
        // The producer can reactivate it twice at a controlled cadence after
        // external cooldowns clear; it never retries indefinitely.
        nextRunAt: deferred.size ? new Date(Date.now() + CROSS_SOURCE_DEFERRED_RECHECK_INTERVAL_MS) : undefined,
        checkpoint: {
          metadataCrossSourceDiscovery: true,
          coverage: deferred.size ? 'CROSS_SOURCE_PARTIAL_EXTERNAL_DEFERRED' : 'CROSS_SOURCE_PROBES_COMPLETE',
          checkedSources: [...checked], deferredSources: [...deferred], matchedSources: matched,
          aliasesAdded, tagsAdded, deferredRounds, completedAt: new Date().toISOString(),
        },
      });
      return;
    }
    const now = Date.now();
    let policyMap: Map<string, LiteSourcePolicy>;
    if (
      this.sourcePoliciesCache &&
      this.sourcePoliciesCache.expiresAt > now &&
      sourceBatch.every((s) => this.sourcePoliciesCache!.map.has(s))
    ) {
      policyMap = this.sourcePoliciesCache.map;
    } else {
      const allSourcesToFetch = [...new Set([...sources, ...sourceBatch])];
      policyMap = typeof (this.queue as any).sourcePolicies === 'function'
        ? await (this.queue as any).sourcePolicies(allSourcesToFetch)
        : new Map(await Promise.all(allSourcesToFetch.map(async (source) => [source, await this.queue.sourcePolicy(source)] as const)));
      this.sourcePoliciesCache = { map: policyMap, expiresAt: now + 5_000 };
    }
    const policies = sourceBatch.map((source) => [source, policyMap.get(source) || null] as const);
    const runnableSources = policies.filter(([, policy]) => policy?.runnable).map(([source]) => source);
    let outcome: CrossSourceMetadataResult = { busy: false, attemptedSources: [],
      deferredSources: policies.filter(([, policy]) => !policy?.runnable).map(([source]) => source), attached: [] };
    if (runnableSources.length) {
      const knownMappings = new Map<string, { sourceWorkId: string; sourceTitle?: string; sourceSlug?: string }>();
      let seedAliases = Array.isArray(target.aliases) ? [...target.aliases] : [];
      try {
        const existingMappings = await this.getAvailablePool().query<{
          source: string; source_work_id: string; source_title: string | null; source_slug: string | null; metadata: unknown;
        }>(`SELECT source,source_work_id,source_title,source_slug,metadata
            FROM importer_work_mappings WHERE work_id=$1::uuid`, [target.id]);
        const harvestedFromMappings: string[] = [];
        for (const m of existingMappings.rows || []) {
          if (m.source && m.source_work_id) {
            knownMappings.set(String(m.source), {
              sourceWorkId: String(m.source_work_id),
              sourceTitle: m.source_title || undefined,
              sourceSlug: m.source_slug || undefined,
            });
          }
          const stored = extractStoredMappingMetadata(m.metadata, target.title);
          harvestedFromMappings.push(...stored.aliases);
          if (isProviderDisplayTitleAlias(m.source_title, m.source_slug, target.title)) {
            harvestedFromMappings.push(String(m.source_title));
          }
        }
        if (harvestedFromMappings.length > 0) {
          const mergedSeed = mergeAlternativeTitles(seedAliases, harvestedFromMappings, target.title);
          const seedChanged = mergedSeed.length !== seedAliases.length
            || mergedSeed.some((a, i) => a !== seedAliases[i]);
          if (seedChanged) {
            await this.getAvailablePool().query(`UPDATE works SET aliases=$2::text[],updated_at=now() WHERE id=$1::uuid`, [target.id, mergedSeed]);
            seedAliases = mergedSeed;
          }
        }
      } catch {
        // Test doubles without importer_work_mappings query support fall back cleanly
      }
      // Stagger source probe order deterministically per work ID to prevent concurrent workers
      // from colliding on the exact same host in alphabetical order.
      const staggerOffset = Math.abs(
        target.id.split('').reduce((acc, c) => ((acc << 5) - acc) + c.charCodeAt(0) | 0, 0)
      ) % runnableSources.length;
      const staggeredSources = [
        ...runnableSources.slice(staggerOffset),
        ...runnableSources.slice(0, staggerOffset)
      ];
      outcome = await this.enrichNewWorkFromComplementarySources({
        workId: target.id, originSource: '', title: target.title, slug: target.slug, aliases: seedAliases,
        author: target.author || undefined, artist: target.artist || undefined, synopsis: sanitizeSynopsis(target.synopsis) || undefined,
        kind: target.kind || undefined, year: target.year ?? undefined, sourceIds: staggeredSources,
        concurrency: Math.min(this.metadataTurboMode.enabled ? 12 : 8, runnableSources.length),
        allowFallbackDetailProbe: true,
        knownMappings,
      });
      outcome.deferredSources.push(...policies.filter(([, policy]) => !policy?.runnable).map(([source]) => source));
    }
    if (outcome.busy) {
      await this.queue.finish(task, workerId, { state: 'RETRY_WAIT', reason: 'CROSS_SOURCE_LANE_BUSY',
        nextRunAt: new Date(Date.now() + 1_000) });
      return;
    }
    // The coverage cursor is a durable fallback, but a newly confirmed
    // alternate source should not wait an entire cursor rotation to enter P0.
    if (outcome.attached.length) {
      try {
        await this.queue.enrollConfirmedP0Mappings(target.id, outcome.attached);
      } catch (error: any) {
        this.logger.warn('Immediate cross-source monitor enrollment deferred', { workId: target.id, error: error?.message });
      }
    }
    for (const source of sourceBatch) checked.add(source);
    // A provider that became runnable has now received a fresh attempt.  Only
    // retain it in the durable deferred set when this attempt fails again.
    for (const source of sourceBatch) deferred.delete(source);
    for (const deferredSource of outcome.deferredSources) deferred.add(deferredSource);
    for (const attached of outcome.attached) {
      matched.push({ source: attached.source, sourceWorkId: attached.sourceWorkId,
        aliasesAdded: attached.aliasesAdded, tagsAdded: attached.tagsAdded });
    }
    const addedAliases = outcome.attached.reduce((sum, item) => sum + item.aliasesAdded, 0);
    const addedTags = outcome.attached.reduce((sum, item) => sum + item.tagsAdded, 0);
    if (addedAliases > 0) {
      this.triggerAsyncAliasOnlyEnrichment(target.id);
    }
    const aliasDiscoveryRounds = Math.max(0, Number(task.checkpoint.aliasDiscoveryRounds || 0));
    if (addedAliases > 0 && aliasDiscoveryRounds < 2) {
      const matchedSourceSet = new Set(matched.map((m: any) => m.source));
      for (const s of [...checked]) {
        if (!matchedSourceSet.has(s) && s !== 'brscans') {
          checked.delete(s);
        }
      }
    }
    const remainingSources = sources.filter((candidate) => !checked.has(candidate));
    if (!remainingSources.length) {
      const deferredRounds = Math.max(0, Number(task.checkpoint.deferredRounds || 0));
      const runnableDeferred = [...deferred].filter(
        (s) => s !== 'brscans' && policyMap.get(s)?.runnable !== false && Boolean(this.registry.get(s))
      );
      if (runnableDeferred.length > 0 && deferredRounds < 1) {
        for (const s of runnableDeferred) checked.delete(s);
        await this.queue.finish(task, workerId, {
          state: 'RETRY_WAIT', reason: 'CROSS_SOURCE_DEFERRED_OPERATIONAL_RETRY',
          nextRunAt: new Date('2026-10-22T00:00:00Z'), resetAttempts: true,
          checkpoint: {
            metadataCrossSourceDiscovery: true,
            coverage: 'CROSS_SOURCE_DEFERRED_RECHECK',
            checkedSources: [...checked], deferredSources: [...deferred], matchedSources: matched.slice(0, 64),
            aliasesAdded: aliasesAdded + addedAliases, tagsAdded: tagsAdded + addedTags,
            aliasDiscoveryRounds: aliasDiscoveryRounds + (addedAliases > 0 ? 1 : 0),
            deferredRounds: deferredRounds + 1,
            lastSource: sourceBatch.at(-1), checkedBatch: sourceBatch,
            lastOutcome: outcome.attached.length ? 'MATCHED' : 'DEFERRED',
          },
        });
        return;
      }
      await this.queue.finish(task, workerId, {
        state: 'DONE', resetAttempts: true,
        nextRunAt: deferred.size ? new Date(Date.now() + CROSS_SOURCE_DEFERRED_RECHECK_INTERVAL_MS) : undefined,
        checkpoint: {
          metadataCrossSourceDiscovery: true,
          coverage: deferred.size ? 'CROSS_SOURCE_PARTIAL_EXTERNAL_DEFERRED' : 'CROSS_SOURCE_PROBES_COMPLETE',
          checkedSources: [...checked], deferredSources: [...deferred], matchedSources: matched.slice(0, 64),
          aliasesAdded: aliasesAdded + addedAliases, tagsAdded: tagsAdded + addedTags,
          aliasDiscoveryRounds: aliasDiscoveryRounds + (addedAliases > 0 ? 1 : 0),
          deferredRounds, completedAt: new Date().toISOString(),
          lastSource: sourceBatch.at(-1), checkedBatch: sourceBatch,
          lastOutcome: outcome.attached.length ? 'MATCHED' : outcome.deferredSources.length ? 'DEFERRED' : 'NO_MATCH',
        },
      });
      return;
    }
    await this.queue.finish(task, workerId, {
      state: 'RETRY_WAIT', reason: null,
      nextRunAt: new Date(Date.now() + CROSS_SOURCE_DISCOVERY_TURN_DELAY_MS),
      resetAttempts: true,
      checkpoint: {
        metadataCrossSourceDiscovery: true, checkedSources: [...checked], deferredSources: [...deferred],
        matchedSources: matched.slice(0, 64), aliasesAdded: aliasesAdded + addedAliases, tagsAdded: tagsAdded + addedTags,
        aliasDiscoveryRounds: aliasDiscoveryRounds + (addedAliases > 0 ? 1 : 0),
        lastSource: sourceBatch.at(-1), checkedBatch: sourceBatch,
        lastOutcome: outcome.attached.length ? 'MATCHED' : outcome.deferredSources.length ? 'DEFERRED' : 'NO_MATCH',
      },
    });
  }

  /** Re-fetch explicit provider metadata for one existing mapping. The task is
   * P2, single-flight, host rate limited, and only calls fetchWorkDetails (no
   * manifest/page discovery). A retryable provider failure is durably marked
   * EXTERNAL_BLOCKED and retried on a bounded exponential cooldown.
   */
  private async refreshMappingMetadataFromSource(task: LiteTask, workerId: string, source: string, sourceWorkId: string,
    initialTimings: Record<string, number | null> = {}): Promise<void> {
    const totalStartedAt = performance.now();
    // V5 is the explicit external-coverage pass that follows V4.  Older
    // source-refresh tasks retain their v3 semantics, including their
    // existing retry policy, so this campaign cannot rewrite history.
    const metadataVersion = task.payload?.metadataEnrichmentVersion === 5 ? 5 : 3;
    const timings: Record<string, number | null> = { ...initialTimings };
    const timePhase = async <T>(name: string, operation: () => Promise<T>): Promise<T> => {
      const startedAt = performance.now();
      try { return await operation(); }
      finally { timings[name] = Math.round(performance.now() - startedAt); }
    };
    const reportTiming = (state: string, finalizeMs?: number) => {
      const totalMs = Math.round(performance.now() - totalStartedAt);
      this.logger.info('Source metadata task timing', {
        source, state, taskId: task.id, ...timings,
        ...(finalizeMs === undefined ? {} : { finalizeMs }), totalMs,
      });
      return { ...timings, ...(finalizeMs === undefined ? {} : { finalizeMs }), totalMs };
    };
    if (!source || !sourceWorkId) {
      await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'METADATA_SOURCE_INPUT_INVALID' });
      return;
    }
    const adapter = this.registry.get(source);
    if (!adapter) {
      const mapping = await this.pool.query<{ work_id: string }>(`SELECT work_id::text FROM importer_work_mappings
        WHERE source=$1 AND source_work_id=$2 AND work_id IS NOT NULL LIMIT 1`, [source, sourceWorkId]);
      if (mapping.rows[0]) await this.queue.recordSourceMetadataEnrichment({
        source, sourceWorkId, workId: String(mapping.rows[0].work_id), aliasesState: 'EXTERNAL_BLOCKED',
        tagsState: 'EXTERNAL_BLOCKED', error: 'SOURCE_ADAPTER_MISSING', nextRetryAt: new Date(Date.now() + 6 * 60 * 60_000),
        version: metadataVersion,
      });
      await this.queue.finish(task, workerId, metadataVersion >= 5
        ? { state: 'BLOCKED', reason: 'METADATA_SOURCE_ADAPTER_MISSING' }
        : { state: 'RETRY_WAIT', reason: 'SOURCE_ADAPTER_MISSING',
            nextRunAt: new Date(Date.now() + 6 * 60 * 60_000), resetAttempts: true });
      return;
    }
    let mappingWorkId = '';
    try {
      const mapping = await this.sourceMetadataPreparationGate.runExclusive(() =>
        timePhase('mappingAndPolicyQueryMs', () => this.sourceMetadataPool.query<{
          work_id: string; source_title: string; source_slug: string; title: string;
          source_enabled: boolean; source_status: string; source_cooldown: Date | null;
          source_health: string | null; health_cooldown: Date | null;
        }>(
          `SELECT m.work_id::text,m.source_title,m.source_slug,w.title,
            source_policy.enabled AS source_enabled,source_policy.status AS source_status,
            source_policy.cooldown_until AS source_cooldown,health.health AS source_health,
            health.cooldown_until AS health_cooldown
           FROM importer_work_mappings m JOIN works w ON w.id=m.work_id
           JOIN importer_sources source_policy ON source_policy.id=m.source
           LEFT JOIN importer_lite_source_health health ON health.source=m.source
           WHERE m.source=$1 AND m.source_work_id=$2 AND m.work_id IS NOT NULL LIMIT 1`, [source, sourceWorkId],
        )),
      );
      const row = mapping.rows[0];
      if (!row) {
        await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'METADATA_ENRICHMENT_MAPPING_MISSING' });
        return;
      }
      mappingWorkId = row.work_id;
      // The mapping query already joins the source and health rows. Reuse that
      // snapshot instead of issuing a second YSQL round-trip for the same
      // source policy immediately before the provider request.
      const sourceCooldown = row.source_cooldown ? new Date(row.source_cooldown) : null;
      const healthCooldown = row.health_cooldown ? new Date(row.health_cooldown) : null;
      const cooldownUntil = [sourceCooldown, healthCooldown]
        .filter((value): value is Date => Boolean(value && Number.isFinite(value.getTime())))
        .sort((left, right) => right.getTime() - left.getTime())[0] || null;
      const status = String(row.source_status || 'ACTIVE').toUpperCase();
      const health = String(row.source_health || 'HEALTHY').toUpperCase();
      const expiredAdministrativeCooldown = status === 'COOLDOWN' && Boolean(sourceCooldown && sourceCooldown <= new Date());
      const administrativelyActive = row.source_enabled === true &&
        (['ACTIVE', 'DEGRADED', 'RECOVERING'].includes(status) || expiredAdministrativeCooldown);
      const healthAllowed = !['BROKEN', 'DISABLED'].includes(health);
      const runnable = administrativelyActive && healthAllowed && (!cooldownUntil || cooldownUntil <= new Date());
      if (!runnable) {
        const nextRunAt = cooldownUntil && cooldownUntil > new Date()
          ? cooldownUntil : new Date(Date.now() + 6 * 60 * 60_000);
        const unavailableCount = Math.max(0, Number(task.checkpoint.sourceUnavailableCount) || 0) + 1;
        await this.sourceMetadataPersistenceGate.runExclusive(() => this.queue.recordSourceMetadataEnrichment({ source, sourceWorkId, workId: mappingWorkId,
          aliasesState: 'EXTERNAL_BLOCKED', tagsState: 'EXTERNAL_BLOCKED',
          error: `SOURCE_NOT_READY:${status || 'MISSING'}`, nextRetryAt: nextRunAt, version: metadataVersion }));
        await this.queue.finish(task, workerId, metadataVersion >= 5 && unavailableCount >= 4
          ? { state: 'BLOCKED', reason: `METADATA_SOURCE_UNAVAILABLE_AFTER_RECHECK:${status || 'MISSING'}` }
          : { state: 'RETRY_WAIT', reason: `SOURCE_NOT_READY:${status || 'MISSING'}`,
              nextRunAt, resetAttempts: true,
              checkpoint: { metadataSourceRefresh: true, sourceUnavailableCount: unavailableCount } });
        return;
      }
      const host = new URL(adapter.baseUrl).host;
      await timePhase('rateLimitWaitMs', () => this.sourceRateLimiter.acquire(host, undefined, 60_000));
      const details = await this.sourceMetadataGate.runExclusive(async () => {
        return timePhase('httpMs', () => withTimeout(adapter.fetchWorkDetails(sourceWorkId), this.config.SOURCE_OPERATION_TIMEOUT_MS, 'METADATA_SOURCE_TIMEOUT'));
      });
      const raw = details.raw && typeof details.raw === 'object' ? details.raw : {};
      const sourceMetadata: Record<string, unknown> = {
        ...raw,
        ...(details.alternativeTitles ? { alternativeTitles: details.alternativeTitles } : {}),
        ...(details.genres ? { genres: details.genres } : {}),
      };
      const extracted = await timePhase('parseNormalizeMs', async () =>
        classifyExplicitSourceMetadata(sourceMetadata, details.title || row.title || row.source_title || ''));
      if (extracted.parserError) {
        await timePhase('metadataPersistCommitMs', () => this.sourceMetadataPersistenceGate.runExclusive(() => this.queue.recordSourceMetadataEnrichment({ source, sourceWorkId, workId: mappingWorkId,
          aliasesState: extracted.aliasesState,
          tagsState: extracted.tagsState,
          error: 'EXPLICIT_SOURCE_METADATA_NOT_PARSED', rawMetadata: sourceMetadata, version: metadataVersion })));
        const checkpointTiming = { ...timings, totalBeforeFinalizeMs: Math.round(performance.now() - totalStartedAt) };
        const finalizeStartedAt = performance.now();
        await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'METADATA_SOURCE_PARSER_ERROR',
          checkpoint: { metadataSourceRefresh: true, parserError: true, metadataTiming: checkpointTiming } });
        timings.finishCommitMs = Math.round(performance.now() - finalizeStartedAt);
        reportTiming('PARSER_ERROR');
        return;
      }
      const persistenceQueuedAt = performance.now();
      await this.sourceMetadataPersistenceGate.runExclusive(async () => {
        timings.metadataPersistenceWaitMs = Math.round(performance.now() - persistenceQueuedAt);
        // Runtime tests deliberately substitute a tiny dedupe double. Retain
        // its older method shape while production takes the narrow historical
        // SQL merge implemented by DeduplicationEngine.
        const historicalMerge = ((this.historicalMetadataDedupe as any).mergeKnownMappedHistoricalMetadata
          || (this.historicalMetadataDedupe as any).mergeKnownMappedWork) as typeof this.dedupe.mergeKnownMappedHistoricalMetadata;
        const resolved = await timePhase('mergePersistenceMs', () => historicalMerge.call(this.historicalMetadataDedupe, {
          source, sourceWorkId, title: details.title || row.source_title || row.title,
          slug: details.slug || row.source_slug || row.title,
          aliases: extracted.aliases, genres: extracted.genres,
          author: details.author, artist: details.artist, synopsis: details.synopsis,
          kind: details.kind, status: details.status, year: details.year, ageRating: details.ageRating,
          rawMetadata: sourceMetadata as Record<string, any>,
        }, mappingWorkId, source));
        if (resolved.workId !== mappingWorkId || resolved.status === 'AMBIGUOUS' || resolved.status === 'FAILED') {
          throw new Error(`METADATA_SOURCE_MAPPING_CONFLICT:${resolved.reason || resolved.status}`);
        }
        // The merge already knows the exact newly-added aliases and tag links.
        // Avoid a second post-merge SELECT (including a work_tags count scan)
        // just to reconstruct deltas for the enrichment checkpoint.
        const aliasDelta = Math.max(0, Number(resolved.metadataDelta?.aliasesAdded || 0));
        const tagDelta = Math.max(0, Number(resolved.metadataDelta?.tagsAdded || 0));
        await timePhase('metadataFinalizeCommitMs', () => this.queue.completeSourceMetadataRefresh(task, workerId, {
          source, sourceWorkId, workId: mappingWorkId,
          aliasesState: extracted.aliases.length ? 'ENRICHED' : 'NO_ALIASES',
          tagsState: extracted.genres.length ? 'ENRICHED' : 'NO_TAGS',
          aliasesAdded: aliasDelta, tagsAdded: tagDelta, rawMetadata: sourceMetadata, version: metadataVersion,
        }, { metadataSourceRefresh: true, version: metadataVersion, aliases: extracted.aliases.length, aliasDelta,
          tags: extracted.genres.length, tagDelta,
          metadataTiming: { ...timings, totalBeforeFinalizeMs: Math.round(performance.now() - totalStartedAt) } }));
      });
      reportTiming('DONE');
    } catch (error: any) {
      const message = String(error?.message || 'METADATA_SOURCE_FETCH_FAILED').slice(0,500);
      const isInternalDbError = /timeout exceeded when trying to connect|connection slots are reserved|Client has encountered a connection error|connection terminated|ECONNREFUSED|ENOTFOUND/i.test(message);
      // Source anti-bot/login HTML can be parsed as a title by an adapter and
      // then rejected by the editorial guard. That is an external block, not
      // a parser defect; retain it with bounded retry instead of terminally
      // classifying the mapping as PARSER_ERROR.
      const externalChallengePage = /INVALID_EDITORIAL_TITLE:.*(?:authentication|login) prompt|TURNSTILE|CLOUDFLARE|HTTP[_ ]?(?:403|429)|RATE[_ ]?LIMIT/i.test(message);
      const persistenceFailure = message.startsWith('WORK_ALIAS_PERSIST_FAILED:');
      const parserFailure = message.startsWith('METADATA_SOURCE_MAPPING_CONFLICT:') && !externalChallengePage;
      // External failures reset the generic task attempt counter so a healthy
      // source observation can start cleanly. Keep a separate durable counter
      // in the task checkpoint; otherwise a 404/challenge retries every two
      // minutes forever and repeatedly occupies one of the tiny metadata
      // admission slots.
      const sourceExternalFailureCount = isInternalDbError ? 0 : Math.max(0, Number(task.checkpoint.sourceExternalFailureCount) || 0) + 1;
      const retryDelay = Math.min(6 * 60 * 60_000, 60_000 * 2 ** Math.min(sourceExternalFailureCount, 8));
      if (mappingWorkId && !persistenceFailure && !isInternalDbError) await timePhase('metadataPersistCommitMs', () => this.sourceMetadataPersistenceGate.runExclusive(() => this.queue.recordSourceMetadataEnrichment({
        source, sourceWorkId, workId: mappingWorkId,
        aliasesState: parserFailure ? 'PARSER_ERROR' : 'EXTERNAL_BLOCKED',
        tagsState: parserFailure ? 'PARSER_ERROR' : 'EXTERNAL_BLOCKED', error: message,
        nextRetryAt: parserFailure ? null : new Date(Date.now() + retryDelay), version: metadataVersion,
      })));
      if (!parserFailure && !persistenceFailure && !isInternalDbError) await timePhase('sourceHealthPersistMs', () => this.queue.recordSourceFailure(source, error));
      const checkpointTiming = { ...timings, totalBeforeFinalizeMs: Math.round(performance.now() - totalStartedAt) };
      const finalizeStartedAt = performance.now();
      await this.queue.finish(task, workerId, parserFailure
        ? { state: 'BLOCKED', reason: message, checkpoint: { metadataTiming: checkpointTiming } }
        : persistenceFailure
          ? { state: 'RETRY_WAIT', reason: 'METADATA_ALIAS_PERSISTENCE_FAILED',
              nextRunAt: new Date(Date.now() + Math.min(5 * 60_000, retryDelay)), resetAttempts: true,
              checkpoint: { metadataTiming: checkpointTiming } }
        : isInternalDbError
          ? { state: 'RETRY_WAIT', reason: `INTERNAL_DB_PRESSURE:${message}`,
              nextRunAt: new Date(Date.now() + 3_000), resetAttempts: true,
              checkpoint: { metadataTiming: checkpointTiming } }
        : metadataVersion >= 5 && sourceExternalFailureCount >= 4
          ? { state: 'BLOCKED', reason: `METADATA_SOURCE_EXTERNAL_RETRY_EXHAUSTED:${message}`,
              checkpoint: { metadataSourceRefresh: true, sourceExternalFailureCount, metadataTiming: checkpointTiming } }
          : { state: 'RETRY_WAIT', reason: `METADATA_SOURCE_EXTERNAL:${message}`,
              nextRunAt: new Date(Date.now() + retryDelay),
              checkpoint: { metadataSourceRefresh: true, sourceExternalFailureCount, metadataTiming: checkpointTiming }, resetAttempts: true });
      timings.finishCommitMs = Math.round(performance.now() - finalizeStartedAt);
      reportTiming(parserFailure ? 'PARSER_ERROR' : persistenceFailure ? 'PERSISTENCE_FAILURE' : 'EXTERNAL_BLOCKED');
    } finally {}
  }

  /** A no-network P2 pass over explicit metadata saved with a known mapping. */
  private async enrichStoredMappingMetadata(task: LiteTask, workerId: string, source: string, sourceWorkId: string): Promise<void> {
    if (!source || !sourceWorkId) {
      await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'METADATA_ENRICHMENT_INPUT_INVALID' });
      return;
    }
    try {
      const isV4 = task.payload?.metadataEnrichmentVersion === 4;
      const timings: Record<string, number> = {};
      const phase = async <T>(name: string, operation: () => Promise<T>): Promise<T> => {
        const startedAt = performance.now();
        try { return await operation(); }
        finally { timings[name] = Math.round(performance.now() - startedAt); }
      };
      const mapping = await phase('mappingLoadMs', () => this.getAvailablePool().query<{ work_id: string; source_title: string; source_slug: string; work_title: string; metadata: unknown }>(
        `SELECT m.work_id::text,m.source_title,m.source_slug,w.title AS work_title,m.metadata
         FROM importer_work_mappings m JOIN works w ON w.id=m.work_id
         WHERE m.source=$1 AND m.source_work_id=$2 AND m.work_id IS NOT NULL LIMIT 1`,
        [source, sourceWorkId],
      ));
      const row = mapping.rows[0];
      if (!row) {
        await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'METADATA_ENRICHMENT_MAPPING_MISSING' });
        return;
      }
      const primaryTitle = row.work_title || row.source_title || '';
      const extracted = extractStoredMappingMetadata(row.metadata, primaryTitle);
      // The mapping has already been attached to this canonical work. Its
      // provider-facing title is therefore first-party evidence of an alias
      // when it differs from the canonical title, even if that provider did
      // not expose a separate alternative-title field. Keep the comparison
      // and Unicode-safe de-duplication in the shared merge helper; this is
      // deliberately not a new cross-work matching signal.
      const sourceTitleAlias = isProviderDisplayTitleAlias(row.source_title, row.source_slug, primaryTitle)
        ? [row.source_title]
        : [];
      const aliases = mergeAlternativeTitles(extracted.aliases, sourceTitleAlias, primaryTitle);
      if (!aliases.length && !extracted.genres.length) {
        await this.queue.finish(task, workerId, { state: 'DONE', checkpoint: { metadataEnrichment: true, empty: true }, resetAttempts: true });
        return;
      }
      const resolve = () => (isV4 ? this.historicalMetadataDedupe : this.dedupe).resolveWork({
        source, sourceWorkId,
        // `mergeKnownMappedWork` considers its candidate title as source
        // evidence. All versions now admit that title separately through
        // isProviderDisplayTitleAlias above, so passing a machine fallback
        // here would bypass the guard and turn a slug or numeric API id into
        // a public alias. The canonical title is neutral to the additive
        // merge while all approved source-title evidence remains in aliases.
        title: primaryTitle,
        slug: row.source_slug || row.work_title,
        aliases,
        genres: extracted.genres,
      });
      // Source refresh and V4 merge both use the isolated metadata pool. In
      // exclusive mode, two different works may persist concurrently; a
      // keyed work tail above retains additive semantics for multiple source
      // mappings of the same canonical work.
      const resolved = isV4
        ? await phase('mergeAndPersistMs', () => this.serializeV4Work(row.work_id, () => (
          (this.metadataReplayV4Exclusive.enabled
            ? this.metadataReplayV4PersistenceGate
            : this.sourceMetadataPersistenceGate).runExclusive(resolve)
        )))
        : await resolve();
      if (!resolved.workId || resolved.status === 'AMBIGUOUS') {
        await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: resolved.reason || 'METADATA_ENRICHMENT_AMBIGUOUS' });
        return;
      }
      const resolvedWorkId = resolved.workId;
      const aliasesAdded = Math.max(0, Number(resolved.metadataDelta?.aliasesAdded || 0));
      const tagsAdded = Math.max(0, Number(resolved.metadataDelta?.tagsAdded || 0));
      await phase('taskFinalizeMs', () => this.queue.finish(task, workerId, {
        state: 'DONE', workId: resolvedWorkId,
        // `aliases`/`tags` describe provider evidence; the deltas are the
        // exact number actually persisted after idempotent de-duplication.
        // Keep both so production reporting never mistakes a duplicate source
        // value for a new public alias or tag.
        checkpoint: { metadataEnrichment: true, aliases: aliases.length, tags: extracted.genres.length,
          aliasesAdded, tagsAdded, metadataTiming: isV4 ? timings : undefined },
        resetAttempts: true,
      }));
    } catch (error: any) {
      const reason = String(error?.message || 'METADATA_ENRICHMENT_FAILED').slice(0, 500);
      const poolContention = /timeout exceeded when trying to connect/i.test(reason);
      const contentionCount = Math.max(0, Number(task.checkpoint.metadataDbContentionCount) || 0) + 1;
      if (poolContention && contentionCount <= 3) {
        await this.queue.finish(task, workerId, {
          state: 'RETRY_WAIT', reason: 'METADATA_ENRICHMENT_DB_CONTENTION',
          nextRunAt: new Date(Date.now() + 30_000), resetAttempts: true,
          checkpoint: { metadataDbContentionCount: contentionCount },
        });
        return;
      }
      await this.queue.finish(task, workerId, {
        state: task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT',
        reason: poolContention ? 'METADATA_ENRICHMENT_DB_CONTENTION_EXHAUSTED' : reason,
        nextRunAt: new Date(Date.now() + this.recurringRetryDelayMs(task.attempts)),
      });
    }
  }

  /** Two successful timestamps prove a revisit without an unbounded event table. */
  private successfulMonitorCheckpoint(task: LiteTask, checkedAt: string): Record<string, unknown> {
    const previous = task.checkpoint?.lastCheckedAt;
    const count = Number(task.checkpoint?.successfulChecksSinceTracking || 0);
    return {
      lastCheckedAt: checkedAt,
      previousCheckedAt: typeof previous === 'string' ? previous : null,
      successfulChecksSinceTracking: Number.isSafeInteger(count) && count >= 0 ? count + 1 : 1,
    };
  }

  /** Reschedule every eligible watch within the target window after a successful check. */
  private monitorIntervalMs(_task: LiteTask, status: string, _newReleaseCount: number, _effectiveReleaseAt?: string | null): number {
    if (status === 'CANCELLED') return 24 * 60 * 60_000;
    return 30 * 60_000;
  }

  /** Bounded health recovery for the two recurring observation task types. */
  private recurringRetryDelayMs(attempts: number): number {
    return Math.min(6 * 60 * 60_000, 60_000 * 2 ** Math.min(Math.max(0, attempts), 8));
  }

  /** Invalid/blocked covers remain absent; they never overwrite a known cover. */
  private async loadCover(adapter: NonNullable<ReturnType<SourceRegistry['get']>>, url: string): Promise<string | null> {
    try {
      const bytes = await this.downloads.runExclusive(async () => {
        let host: string | null = null;
        try { host = new URL(url).host; } catch {}
        if (host) await this.sourceRateLimiter.acquire(host);
        const response = await fetch(url, { headers: await adapter.getImageHeaders?.(url), signal: AbortSignal.timeout(30_000) });
        if (!response.ok) {
          if (response.status === 429 && host) this.sourceRateLimiter.handle429(host, response.headers.get('retry-after'));
          throw new Error(`COVER_HTTP_${response.status}`);
        }
        const bytes = await readImageBody(response, { signal: AbortSignal.timeout(30_000), maxBytes: 8 * 1024 * 1024 });
        if (host) this.sourceRateLimiter.recordSuccess(host);
        return bytes;
      });
      const stored = await processAndStoreMedia(this.catalogClient, this.storage, bytes, this.config.IMPORTER_USER_ID!, 'cover');
      return stored.mediaId;
    } catch { return null; }
  }
}
