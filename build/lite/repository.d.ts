import type { Pool } from 'pg';
import { type LitePriority, type LiteTask, type LiteTaskState, type LiteTaskType } from './types.js';
type Db = Pick<Pool, 'query' | 'connect'>;
/**
 * Older catalog passes could put two legacy mappings for one canonical chapter
 * in a single UPSERT. PostgreSQL/YSQL rejects that statement before its
 * conflict clause can decide either row. Keep the exact durable reason
 * narrowly scoped so recovery gives only those code-bug failures one clean
 * retry.
 */
export declare const DUPLICATE_CANONICAL_BATCH_REASON = "ON CONFLICT DO UPDATE command cannot affect row a second time";
export declare const METADATA_TURBO_EXCLUSIVE_MODE_KEY = "metadata_turbo_exclusive_mode";
export interface EnqueueLiteTask {
    taskType: LiteTaskType;
    workId?: string | null;
    workKey: string;
    chapterKey?: string | null;
    priority: Exclude<LitePriority, 'STAFF'>;
    idempotencyKey: string;
    nextRunAt?: Date;
    maxAttempts?: number;
    payload?: Record<string, unknown>;
    checkpoint?: Record<string, unknown>;
    /** Reserved for the one-resource P0 observation lane, never normal work. */
    monitoring?: boolean;
    /**
     * Startup's durable DISCOVER seeds are claimed through their dedicated
     * source lane and do not require a ready-work projection refresh.
     */
    skipReadyWorkRefresh?: boolean;
}
export interface LiteSourcePolicy {
    enabled: boolean;
    status: string;
    health: string;
    cooldownUntil: Date | null;
    syncIntervalMs: number;
    catalogDiscoveryEnabled: boolean;
    chapterIngestionEnabled: boolean;
    runnable: boolean;
}
export interface P0MonitorCoverageRefill {
    source: string | null;
    admitted: number;
    revived: number;
}
/** A page upload that Telegram has confirmed and now needs one durable receipt. */
export interface UploadReceiptInput {
    taskId: string;
    pagePosition: number;
    mediaId: string;
    sha256: string;
    bytes: number;
    mime: string;
    width: number;
    height: number;
    providerKey: string;
    botReference: string | null;
    shardId: string | null;
    channelId: string | null;
    messageId: number | null;
    createdBy: string;
}
/** A durable intent must exist before the matching Telegram upload starts. */
export interface UploadReservationInput {
    taskId: string;
    pagePosition: number;
    sha256: string;
}
/**
 * The final import transition is deliberately one short database transaction.
 * Media has already been durably receipted before this point; combining the
 * DONE checkpoint with its PUBLISH task removes an avoidable finish/enqueue
 * gap and several YSQL round trips without holding a connection during I/O.
 */
export interface CompleteImportInput {
    chapterId: string;
    mediaIds: string[];
    pageCount: number;
    selectedSource: string;
    mediaStoredAt: string;
}
export interface LiteClaimOptions {
    focusWorkKeys?: string[] | null;
    preferFocus?: boolean;
    excludedSources?: string[] | null;
    chaptersOnly?: boolean;
}
export interface P1FocusWorkEntry {
    workKey: string;
    workId: string;
    source: string;
    remainingChapters: number;
    enteredAt: number;
}
export interface P1FocusGroupSnapshot {
    targetSize: number;
    focusWorkCount: number;
    focusWorkKeys: string[];
    focusSources: Record<string, number>;
    primaryClaims: number;
    secondaryClaims: number;
    completedWorksSinceBoot: number;
    recentCompletedWorkIds: string[];
    refreshedAt: string | null;
}
/** Direct YSQL persistence. Network, adapters and Telegram are never invoked here. */
export declare class LiteRepository {
    private readonly db;
    private readonly sourceMetadataDb;
    private historicalP1ReserveAbundant;
    private manifestP1ReserveCache;
    private readonly coolingSources;
    private p1FocusWorkKeys;
    private p1FocusMeta;
    private p1FocusTargetSize;
    private p1FocusRefreshedAt;
    private p1CompletedWorksCount;
    private p1CompletedWorkIds;
    private p1PrimaryClaims;
    private p1SecondaryClaims;
    private lastHistoricalP1CatalogSource;
    private p1BacklogCache;
    private p0MonitorCursorEnsured;
    private readonly sourcePolicyCache;
    private readonly lastHealthyRecordAt;
    hasAbundantHistoricalP1Reserve(): boolean;
    invalidateP1BacklogCache(): void;
    invalidateSourcePolicyCache(source?: string): void;
    getActiveCoolingSources(): string[];
    private clearNonRateLimitedCoolingSource;
    /**
     * Source-backed historical metadata is deliberately allowed one tiny,
     * isolated database seat.  It cannot borrow the chapter claim pool during a
     * busy wave, while its own producer/claim/finish operations remain strictly
     * serial through the runtime's lane gates.
     */
    constructor(db: Db, sourceMetadataDb?: Db);
    /**
     * Enrol a small, source-rotated slice of existing published ongoing works
     * into the durable P0 observation lane.
     *
     * Older Lite releases only made a recurring watch as a side effect of an
     * initial P2 catalog pass.  Existing mapped works that predated Lite (or
     * whose first pass completed before that rule) therefore never received a
     * P0 check at all.  A healthy monitor executor cannot discover a release
     * for a work it was never asked to observe.
     *
     * This is deliberately a producer, not a second claim path: it creates at
     * most a small number of `monitoring=true` CATALOG rows for one operational
     * source.  Those rows remain invisible to normal P1/P2 claims and use the
     * existing two-permit P0 monitor lane.  No network operation or long-lived
     * transaction occurs here.
     */
    refillP0MonitorCoverage(limit?: number): Promise<P0MonitorCoverageRefill>;
    /**
     * Admit at most two low-priority source-backed metadata reads at a time.
     * Selection rotates across sources, honors source cooldown/eligibility, and
     * records PENDING durably before inserting an idempotent catalog task.
     */
    refillSourceMetadataEnrichment(maxPending?: number, sourceProbeLimit?: number, preferredSource?: string): Promise<number>;
    recordSourceMetadataEnrichment(input: {
        source: string;
        sourceWorkId: string;
        workId: string;
        aliasesState: 'ENRICHED' | 'NO_ALIASES' | 'EXTERNAL_BLOCKED' | 'PARSER_ERROR';
        tagsState: 'ENRICHED' | 'NO_TAGS' | 'EXTERNAL_BLOCKED' | 'PARSER_ERROR';
        aliasesAdded?: number;
        tagsAdded?: number;
        error?: string | null;
        nextRetryAt?: Date | null;
        rawMetadata?: Record<string, unknown>;
        version?: number;
    }): Promise<void>;
    /** Commit a successful source refresh, its durable classification, source
     * health and task completion together. This avoids two YSQL transactions
     * between one external metadata fetch and the next available task. */
    completeSourceMetadataRefresh(task: LiteTask, workerId: string, input: {
        source: string;
        sourceWorkId: string;
        workId: string;
        aliasesState: 'ENRICHED' | 'NO_ALIASES';
        tagsState: 'ENRICHED' | 'NO_TAGS';
        aliasesAdded: number;
        tagsAdded: number;
        rawMetadata: Record<string, unknown>;
        version?: number;
    }, checkpoint: Record<string, unknown>): Promise<boolean>;
    /**
     * Incrementally replays explicit aliases/genres already stored on legacy
     * mappings.  It is deliberately a tiny P2 producer: no chapter is claimed,
     * no source is contacted, and at most two catalog tasks may be outstanding.
     * The task itself performs the normal protected metadata merge, preserving
     * manual fields and tag provenance rules.
     */
    refillStoredMetadataEnrichment(maxPending?: number): Promise<number>;
    /**
     * Version 4 is an explicitly enabled historical campaign, separate from
     * the older parser-only replays.  It combines every provider-declared
     * alias/tag field with the already-attached secondary source title, but it
     * never uses either value to match a different work. Source-local durable
     * cursors and one outstanding P2 task preserve idempotency and source
     * rotation without competing with chapter admission or rescanning the
     * full catalog. The runtime chooses the due-time fence for normal versus
     * explicitly enabled exclusive operation.
     */
    refillStoredMetadataEnrichmentV4(maxPending?: number, minimumIntervalMs?: number, sourceWindowsPerTurn?: number): Promise<number>;
    /** The V4 campaign owns the one isolated metadata seat while it has debt. */
    isStoredMetadataReplayV4Enabled(): Promise<boolean>;
    /** A persisted, reversible operator mode for the finite V4 campaign. */
    storedMetadataReplayV4ExclusiveMode(): Promise<{
        enabled: boolean;
        intervalMs: number;
    }>;
    /** Completion is durable, idempotent and makes a restart resume normal work. */
    disableStoredMetadataReplayV4ExclusiveMode(): Promise<void>;
    /** A persisted, reversible operator mode for the finite V5 metadata & alias campaign. */
    metadataTurboExclusiveMode(): Promise<{
        enabled: boolean;
        discoveryConcurrency?: number;
        searchConcurrency?: number;
        metadataGateCapacity?: number;
        persistenceCapacity?: number;
    }>;
    setMetadataTurboExclusiveMode(enabled: boolean, options?: {
        discoveryConcurrency?: number;
        searchConcurrency?: number;
        metadataGateCapacity?: number;
        persistenceCapacity?: number;
    }): Promise<void>;
    disableMetadataTurboExclusiveMode(): Promise<void>;
    /**
     * Materialise only chapters already positively confirmed by a durable source
     * manifest.  Historical catalog tasks are useful for obtaining new
     * manifests, but a completed old catalog task must not make its confirmed
     * missing rows invisible forever.  This producer never infers a numeric
     * gap, never contacts a source, and remains bounded by both work and source
     * so one large title/provider cannot monopolise P1 admission.
     */
    refillManifestProvenP1Imports(readyFloor?: number, batchLimit?: number, forceRepair?: boolean): Promise<number>;
    /** One-shot resumable repair for a known confirmed-manifest backlog. It
     * bypasses only the ordinary reserve quota, never evidence checks or task
     * idempotency, and still inserts in bounded 128-row transactions.
     */
    materializeConfirmedMissingP1Tasks(maximum?: number): Promise<number>;
    private refreshReadyWork;
    private refreshReadyWorks;
    enqueue(input: EnqueueLiteTask): Promise<{
        inserted: boolean;
        task: LiteTask | null;
    }>;
    /**
     * Cataloguing can reveal hundreds of chapters at once.  Insert their durable
     * work in one short transaction, then refresh each affected ready-work row
     * once.  It intentionally does no network work and never claims anything.
     */
    enqueueMany(inputs: EnqueueLiteTask[]): Promise<{
        inserted: number;
        insertedKeys: Set<string>;
    }>;
    claim(workerId: string, leaseSeconds: number, options?: LiteClaimOptions): Promise<LiteTask | null>;
    /**
     * Dedicated source-rotated claim for READY historicalP1Backfill CATALOG tasks
     * across operational sources so multi-source P1 manifests materialize without
     * waiting behind hundreds of operation_rank=1 chapter imports on hot sources.
     */
    claimHistoricalP1Catalog(workerId: string, leaseSeconds: number): Promise<LiteTask | null>;
    /**
     * Dynamically maintain the Primary P1 Focus Group (~15-20 works, expanding
     * with real admission capacity) favoring near-completion works + queue-age
     * fairness + multi-source diversity, while detecting completed P1 works.
     */
    refreshP1FocusGroup(options?: {
        targetSize?: number;
        maxPerSource?: number;
        sourceTiers?: ReadonlyMap<string, 'FAST' | 'NORMAL' | 'SLOW'>;
    }): Promise<P1FocusGroupSnapshot>;
    getP1FocusWorkKeys(): string[];
    getP1FocusGroupSnapshot(): P1FocusGroupSnapshot;
    /**
     * Determines whether executable or recoverable P1 historical backlog exists
     * so P2 new-work discovery/onboarding remains subordinated to P1 without
     * ever starving permanently on terminal BLOCKED or disabled-source rows.
     */
    hasExecutableOrRecoverableP1Backlog(forceFresh?: boolean): Promise<{
        pending: boolean;
        reason: string | null;
    }>;
    /**
     * Checks whether an existing work currently has pending unpublished chapters
     * on operational routes so newly detected P0 chapters are incorporated into
     * that work's P1 canonical sequence instead of publishing out of order.
     */
    hasPendingWorkChapterBacklog(workId: string): Promise<boolean>;
    /** Source-backed metadata owns a dedicated fast lane and never waits behind chapter claims. */
    claimSourceMetadataRefresh(workerId: string, leaseSeconds: number): Promise<LiteTask | null>;
    /**
     * Whole-catalog alias discovery must not wait behind an unbounded chapter
     * P1 reserve.  It owns one explicit, low-rate source-metadata lane; the
     * normal task claim remains the authority for all chapter/catalog work.
     */
    claimCrossSourceMetadataDiscovery(workerId: string, leaseSeconds: number): Promise<LiteTask | null>;
    /**
     * One first-pass catalog claim is kept independent from the chapter queue.
     * It is limited by the runtime to a single slot and only applies before a
     * canonical work exists, so a sustained P1 repair reserve cannot make new
     * work onboarding wait forever.
     */
    claimNewWorkCatalog(workerId: string, leaseSeconds: number): Promise<LiteTask | null>;
    /**
     * Claims one due DISCOVER task independent of P1 chapter reserve so active
     * sources continuously feed the unmapped NEW_WORK onboarding lane without
     * waiting for the entire historical chapter backlog to drain.
     */
    claimDueDiscovery(workerId: string, leaseSeconds: number): Promise<LiteTask | null>;
    /**
     * Look up already-mapped source_work_id -> work_id in one indexed batch so
     * discovery only enqueues unmapped CATALOG tasks (work_id IS NULL) for
     * genuinely new works.
     */
    existingSourceWorkMappings(source: string, sourceWorkIds: string[]): Promise<Map<string, string>>;
    /**
     * Extend a live task lease without changing its owner or state.  Catalog
     * work deliberately performs provider HTTP outside a transaction and a new
     * work can also make a bounded complementary-source pass, so its wall time
     * can legitimately exceed the initial 90-second claim window.  The owner
     * and unexpired-lease predicates retain the normal fencing guarantees: a
     * stale runtime can never revive a task reclaimed by another worker.
     */
    renewLease(task: LiteTask, workerId: string, leaseSeconds: number): Promise<boolean>;
    /** Product-level transitions, counted once per work rather than per mapping. */
    metadataProductSnapshot(): Promise<{
        aliasesActivatedWorks: number;
        tagsActivatedWorks: number;
    }>;
    /**
     * P0 observation has one durable, explicitly bounded lane.  It is never
     * selected by normal claims, so unchanged watches cannot starve P1/P2.
     *
     * The old stored function selected the globally oldest watch. A provider
     * with hundreds of overdue watches could therefore hide every other source
     * for hours even while this lane itself remained healthy. This short
     * transaction locks one durable source cursor, claims the oldest due watch
     * for the next source in its round-robin, and advances only when the claim
     * succeeds. It neither changes chapter priority nor admits media work.
     */
    claimMonitor(workerId: string, leaseSeconds: number): Promise<LiteTask | null>;
    /**
     * Claims one already-durable, no-network metadata replay.  This deliberately
     * is not a second chapter-admission policy: it can only advance the bounded
     * v2 enrichment rows created from metadata we already have, and is invoked
     * at a much lower cadence than normal recovery.  Without this narrow drain,
     * a permanently healthy P1 backlog would make historical aliases and tags
     * wait forever despite requiring neither a source slot nor a chapter worker.
     */
    claimStoredMetadataReplay(workerId: string, leaseSeconds: number, metadataVersion?: 4): Promise<LiteTask | null>;
    /** Recover one expired v4 lease without waiting for the chapter-pool reconciler. */
    reclaimExpiredStoredMetadataReplayV4(): Promise<number>;
    /**
     * A bounded view of sources that can serve an actual next-priority chapter
     * import. One task is sampled per ready work, rather than the first N task
     * rows, so a single large work cannot hide other ready works. Historical P1
     * catalog work is admitted by the ready-work projection separately; it must
     * not consume a chapter slot until it has materialised a real chapter.
     *
     * The sample is outside claim; durable claim remains authority for task
     * existence, priority and work-level round-robin.
     */
    private readyImportSourceRows;
    readyImportSources(limit?: number, db?: Pick<Pool, 'query'>): Promise<string[]>;
    /**
     * A bounded source demand view for the local fairness gate.  The count is
     * deliberately one executable frontier per work, never raw chapters, so a
     * deep single-work P1 backlog cannot claim capacity reserved for unrelated
     * works or sources.
     */
    readyImportSourceDemand(limit?: number, db?: Pick<Pool, 'query'>): Promise<Map<string, number>>;
    /**
     * Recent public publication attribution. It reads the durable publication
     * event rather than task DONE, so media storage or a stale task can never
     * inflate throughput/diversity metrics.
     */
    publicationDiversitySnapshot(windowSeconds?: number): Promise<Record<string, unknown>>;
    /**
     * Source fairness is an admission yield, not a retry: no provider request
     * was started and consuming retry budget would eventually block healthy
     * work. Moving the work briefly behind its peers lets a source with an
     * available permit proceed without changing durable priority.
     */
    yieldForSourceFairness(task: LiteTask, workerId: string, delayMs?: number): Promise<boolean>;
    /**
     * Discovery is a low-cost producer, never a reason to monopolize P2.  This
     * bounded startup repair only defers due DISCOVER rows; catalog and chapter
     * tasks are untouched.  Discovery tasks are claimed directly without
     * ready-work projections.
     */
    staggerReadyDiscoveries(spacingMs: number, limit?: number): Promise<number>;
    /**
     * The v1 catalog contains durable, confirmed source/work mappings that may
     * predate Lite's chapter-manifest table.  They are evidence worth checking,
     * but are not evidence that a numeric chapter gap exists.  This bounded
     * admission turns only a few such mappings into P1 catalog passes.  A deep
     * executable reserve keeps those passes deferred, rather than suppressing
     * them entirely: that small producer lane is what lets an older, healthy
     * source eventually materialize a confirmed manifest. Cataloging remains
     * outside the claim SQL; its confirmed manifest is what may subsequently
     * create IMPORT_CHAPTER tasks.
     *
     * A durable source cursor probes only a few providers per turn.  This avoids
     * letting the newest rows of a giant mapping set hide older providers before
     * source fairness can run, without making the claim path scan the catalog.
     */
    refillHistoricalP1Catalog(readyFloor?: number, batchLimit?: number): Promise<number>;
    /**
     * A healthy P1 reserve keeps historical catalog checks deferred so they do
     * not continuously consume chapter workers. Promote one existing check on
     * the already-bounded recovery turn: this is a producer quota, not a
     * second scheduler. It keeps alternative mapped sources progressing while
     * leaving the materialised import reserve dominant.
     */
    promoteDeferredHistoricalP1Catalog(activeImportSources?: Iterable<string>): Promise<boolean>;
    /**
     * Recover only catalog tasks blocked by the pre-deduplication batch bug.
     * This is deliberately not a generic BLOCKED retry: normal permanent
     * failures retain their diagnosis and need their own safe repair path.
     */
    recoverDuplicateCanonicalCatalogFailures(limit?: number): Promise<number>;
    /**
     * Before the P0/P2 policy boundary was corrected, an operational source
     * with `catalog_discovery_enabled=false` could park its existing-work P0
     * monitor as SOURCE_NOT_READY:ACTIVE. That switch is for source-wide P2
     * DISCOVER only; reopen just those legacy monitor rows in small, idempotent
     * batches. Normal source health/cooldown eligibility remains authoritative.
     */
    reopenP0MonitorsBlockedByDiscoveryPolicy(limit?: number): Promise<number>;
    /**
     * Source administration and transient Lite health remain durable authority.
     * This indexed one-source read happens before source networking, never in a
     * normal task claim, and prevents a paused/cooling provider from consuming
     * a catalog or discovery slot.
     */
    sourcePolicy(source: string): Promise<LiteSourcePolicy | null>;
    /**
     * Batch read operational policies for multiple sources in a single query.
     * Replaces N+1 sourcePolicy roundtrips during whole-catalog cross-source discovery.
     */
    sourcePolicies(sources: string[]): Promise<Map<string, LiteSourcePolicy>>;
    /**
     * Sources that can safely be queried while a newly-created work is still in
     * its initial catalog pass.  This is intentionally one indexed set query:
     * onboarding must not turn a complementary-alias lookup into an N+1 policy
     * read, and mappings already attached to the work are not searched again.
     */
    complementaryCatalogSources(workId: string, originSource: string): Promise<string[]>;
    /**
     * A coverage pass must not assume that the mappings already attached to a
     * canonical work are exhaustive.  Return every still-operational provider
     * that has no mapping for this work, including a temporarily cooled one so
     * its outcome can be recorded as deferred rather than silently skipped.
     */
    crossSourceDiscoverySources(workId: string): Promise<string[]>;
    /**
     * Admit a tiny, durable whole-catalog cross-source discovery lane.
     * It is deliberately separate from V5's known-mapping refresh: each task
     * records every operational provider considered for its canonical work and
     * only creates a mapping after runtime identity verification.
     */
    refillCrossSourceMetadataDiscovery(maxPending?: number): Promise<number>;
    /**
     * Persist the durable cross-source V7 coverage matrix produced during NEW_WORK
     * onboarding so every new work has an auditable 43-source verification record,
     * and any deferred or gate-busy sources are automatically re-probed.
     */
    recordOnboardingCrossSourceDiscovery(input: {
        workId: string;
        originSource: string;
        busy: boolean;
        attemptedSources: string[];
        deferredSources: string[];
        attached: Array<{
            source: string;
            sourceWorkId: string;
            aliasesAdded: number;
            tagsAdded: number;
        }>;
        sourceProbes?: Record<string, unknown>;
    }): Promise<void>;
    /**
     * Chapter media is expensive and a Reader chapter cannot be exposed without
     * a minimally valid work presentation.  Catalog checks this once per work
     * snapshot, never on the page or chapter hot path, so a missing cover does
     * not turn into completed-but-unpublishable Telegram uploads.
     */
    workPresentationReady(workId: string): Promise<boolean>;
    /**
     * A catalog pass can prove that a work is not Reader-presentable before a
     * large historical manifest reaches chapter workers. Park only work that
     * has not started: in-flight lower-priority operations must still drain
     * safely, and a later valid catalog pass reopens these exact durable rows.
     */
    parkUnpublishableWork(workId: string, workKey: string): Promise<number>;
    checkpoint(task: LiteTask, workerId: string, patch: Record<string, unknown>): Promise<boolean>;
    /**
     * Atomically fence the imported chapter, acknowledge its healthy source and
     * materialize the idempotent publication task.  This runs only after every
     * page has a durable receipt.  Network and Telegram work are intentionally
     * outside this transaction.
     */
    completeImportAndEnqueuePublish(task: LiteTask, workerId: string, completion: CompleteImportInput): Promise<boolean>;
    /**
     * Inline atomic completion + Reader publication for the production worker hot
     * path.  Eliminates the intermediate PUBLISH enqueue, re-claim and second
     * transaction while preserving every lease fence, work-presentation check,
     * importer_replace_pages integrity check and importer_publication_events row.
     * If work presentation became incomplete while media was uploading, rolls
     * back and falls back to completeImportAndEnqueuePublish so durable page
     * uploads are never lost.
     */
    completeImportAndPublish(task: LiteTask, workerId: string, completion: CompleteImportInput, fresh: boolean): Promise<boolean>;
    finish(task: LiteTask, workerId: string, outcome: {
        state: Extract<LiteTaskState, 'DONE' | 'RETRY_WAIT' | 'BLOCKED'>;
        reason?: string | null;
        nextRunAt?: Date;
        checkpoint?: Record<string, unknown>;
        priority?: Exclude<LitePriority, 'STAFF'>;
        monitoring?: boolean;
        /** Catalog attaches the resolved canonical work without changing source identity. */
        workId?: string;
        /** A healthy recurring observation starts a fresh bounded failure window. */
        resetAttempts?: boolean;
    }): Promise<boolean>;
    /**
     * Catalog evidence is the only event that reopens a source-blocked chapter.
     * It is scoped by the indexed canonical work/chapter keys, so a new mapping
     * never causes a global BLOCKED sweep. Terminal integrity failures stay
     * blocked until an operator changes the underlying condition.
     */
    wakeCatalogBlocked(workId: string, workKey: string): Promise<number>;
    /**
     * Source health is durable operational evidence, shared by discovery,
     * cataloguing and chapter execution.  It is deliberately separate from
     * task priority: a cooled-down preferred source must not suppress a
     * confirmed healthy alternative.
     */
    recordSourceFailure(source: string, error: unknown): Promise<void>;
    recordSourceSuccess(source: string): Promise<void>;
    /**
     * The UNKNOWN receipt proof is deliberately expensive: it must join the
     * durable upload intent to a Telegram media row before any task can reopen.
     * Use this index-only watermark to avoid repeating that historical proof
     * when no ambiguous receipt has appeared since the last empty pass.
     */
    newestUnknownUploadAt(): Promise<Date | null>;
    hasDueSourceCooldowns(): Promise<boolean>;
    /** One bounded recovery turn; it cannot consume the claim path or scan works. */
    reconcile(limit?: number, options?: {
        recoverUploadOutcomes?: boolean;
        recoverSourceCooldowns?: boolean;
        recoverLegacyMaintenance?: boolean;
    }): Promise<{
        leases: number;
        retries: number;
        exhausted: number;
        uploads: number;
        uploadReceiptRecoveries: number;
        publishable: number;
        publishRepairs: number;
        sourceCooldownRecoveries: number;
    }>;
    operationalSnapshot(): Promise<Array<{
        priority: LitePriority;
        state: LiteTaskState;
        monitoring: boolean;
        count: number;
        oldestReadyAt: Date | null;
    }>>;
    /** Small, indexed progress probe used for a human-operable Lite heartbeat. */
    progressSnapshot(): Promise<{
        publishedLastMinute: number;
        oldestReadyAt: Date | null;
        oldestStoredUnpublishedAt: Date | null;
        nextMonitorAt: Date | null;
    }>;
    blockedReasonSnapshot(limit?: number): Promise<Array<{
        reason: string;
        count: number;
    }>>;
    sourceHealthSnapshot(): Promise<Array<{
        health: string;
        count: number;
    }>>;
    /**
     * Load a chapter's durable page receipts once before its bounded page fanout.
     * A fresh chapter normally has no rows, so probing one position at a time
     * needlessly turns every page into an extra YSQL round trip.  The task lease
     * fences concurrent writers; the receipt remains authoritative after a
     * restart and reserveUpload still rechecks the uncommon repair/race path.
     */
    uploadReceipts(taskId: string): Promise<Map<number, {
        mediaId: string;
        state: string;
        sha256: string;
        storageReady: boolean | null;
        bytes: number | null;
        mime: string | null;
        mediaSha256: string | null;
    }>>;
    /**
     * Reserve a bounded wave of pre-upload intents in one statement.  This is
     * equivalent to `reserveUpload` for a fresh page, but lets concurrently
     * completed downloads share the small direct YSQL pool.  Existing rows use
     * the single-page path below, which retains its stricter stale-intent and
     * invalid-media repair semantics.
     */
    reserveUploads(inputs: UploadReservationInput[]): Promise<Array<{
        mediaId: string;
        state: string;
    }>>;
    /** Stable ID is reserved before the non-transactional Telegram call. */
    reserveUpload(taskId: string, pagePosition: number, sha256: string): Promise<{
        mediaId: string;
        state: string;
    }>;
    recordUploadReceipt(input: UploadReceiptInput): Promise<void>;
    /**
     * Persist confirmed Telegram outcomes in one statement.  A page intent is
     * still durable before its upload; batching only removes independent YSQL
     * round trips after Telegram has replied.  On a batch error every caller
     * retains the exact same UNKNOWN-outcome path as the single-page method.
     */
    recordUploadReceipts(inputs: UploadReceiptInput[]): Promise<void>;
    markUploadUnknown(taskId: string, position: number): Promise<void>;
    /** The provider explicitly rejected the attempt, so the same reserved ID may retry safely. */
    markUploadRetryable(taskId: string, position: number): Promise<void>;
    publish(input: {
        task: LiteTask;
        workerId: string;
        chapterId: string;
        workId: string;
        mediaIds: string[];
        fresh: boolean;
    }): Promise<boolean>;
}
export {};
