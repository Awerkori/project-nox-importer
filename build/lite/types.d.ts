/**
 * Durable vocabulary for Importer Lite.  These are deliberately smaller than
 * the legacy queue vocabulary: detail belongs in checkpoints and reasons, not
 * in ever more states.
 */
export declare const LITE_PRIORITIES: readonly ["STAFF", "P0", "P1", "P2"];
export type LitePriority = (typeof LITE_PRIORITIES)[number];
export declare const LITE_PRIORITY_RANK: Record<LitePriority, number>;
export declare const LITE_TASK_STATES: readonly ["READY", "RUNNING", "RETRY_WAIT", "BLOCKED", "DONE"];
export type LiteTaskState = (typeof LITE_TASK_STATES)[number];
export type LiteTaskType = 'DISCOVER' | 'CATALOG' | 'IMPORT_CHAPTER' | 'PUBLISH';
export interface LiteTask {
    id: string;
    taskType: LiteTaskType;
    workId: string | null;
    workKey: string;
    chapterKey: string | null;
    priority: LitePriority;
    basePriority: LitePriority;
    state: LiteTaskState;
    idempotencyKey: string;
    nextRunAt: Date;
    attempts: number;
    maxAttempts: number;
    leaseOwner: string | null;
    leaseUntil: Date | null;
    reason: string | null;
    /**
     * A bounded, query-only P0 watch. It is deliberately excluded from the
     * executable-work projection: observing an unchanged work is not P0 work.
     */
    monitoring: boolean;
    payload: Record<string, unknown>;
    checkpoint: Record<string, unknown>;
    /** Database creation time, used to measure durable queue dwell for background work. */
    createdAt?: Date;
}
export type SourceHealth = 'HEALTHY' | 'DEGRADED' | 'RATE_LIMITED' | 'BROKEN' | 'DISABLED';
export interface ChapterSourceRoute {
    source: string;
    sourceWorkId?: string | null;
    sourceChapterId: string;
    canonicalChapterKey: string;
    workId: string;
    language?: string | null;
    edition?: string | null;
    sourceRank: number;
    health: SourceHealth;
    cooldownUntil?: Date | null;
    confirmed: boolean;
    hasChapter: boolean;
}
export interface SourceSelection {
    route: ChapterSourceRoute | null;
    reason?: 'NO_CONFIRMED_ROUTE' | 'ALL_ROUTES_COOLDOWN' | 'ALL_ROUTES_UNHEALTHY';
}
