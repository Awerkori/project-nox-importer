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
/** Direct YSQL persistence. Network, adapters and Telegram are never invoked here. */
export declare class LiteRepository {
    private readonly db;
    private historicalP1ReserveAbundant;
    hasAbundantHistoricalP1Reserve(): boolean;
    constructor(db: Db);
    private refreshReadyWork;
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
    claim(workerId: string, leaseSeconds: number): Promise<LiteTask | null>;
    /**
     * P0 observation has one durable, explicitly bounded lane.  It is never
     * selected by normal claims, so unchanged watches cannot starve P1/P2.
     */
    claimMonitor(workerId: string, leaseSeconds: number): Promise<LiteTask | null>;
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
    readyImportSources(limit?: number): Promise<string[]>;
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
     * tasks are untouched.  Each ready-work projection is refreshed in the
     * same short transaction so a stale projection cannot create empty claims.
     */
    staggerReadyDiscoveries(spacingMs: number, limit?: number): Promise<number>;
    /**
     * The v1 catalog contains durable, confirmed source/work mappings that may
     * predate Lite's chapter-manifest table.  They are evidence worth checking,
     * but are not evidence that a numeric chapter gap exists.  This bounded
     * admission turns only a few such mappings into P1 catalog passes when the
     * real P1 reserve becomes low.  Cataloging remains outside the claim SQL;
     * its confirmed manifest is what may subsequently create IMPORT_CHAPTER
     * tasks.
     *
     * The inner window is deliberately small and indexed by the legacy mapping
     * status.  Limiting each source to two candidates prevents a large legacy
     * mapping set from monopolising the next P1 refill.
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
     * Source administration and transient Lite health remain durable authority.
     * This indexed one-source read happens before source networking, never in a
     * normal task claim, and prevents a paused/cooling provider from consuming
     * a catalog or discovery slot.
     */
    sourcePolicy(source: string): Promise<LiteSourcePolicy | null>;
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
    /** One bounded recovery turn; it cannot consume the claim path or scan works. */
    reconcile(limit?: number): Promise<{
        leases: number;
        retries: number;
        exhausted: number;
        uploads: number;
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
