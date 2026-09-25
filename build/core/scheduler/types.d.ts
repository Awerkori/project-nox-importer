/**
 * Types and interfaces for the Project Nox Work-Oriented Scheduler.
 *
 * Defines priority lanes, canonical work lifecycle states, active set structures,
 * persistent watermarks, admission configuration, and explainability telemetry.
 */
export declare enum SchedulerLane {
    STAFF_FORCED = "STAFF_FORCED",// Priority >= 1000 / staffForced: true: Absolute Staff Priority
    P0_FRESH_RELEASE = "P0_FRESH_RELEASE",// Priority >= 100: Fresh release of tracked/existing work
    P1_CRITICAL_GAP = "P1_CRITICAL_GAP",// Priority 90-95: Missing chapter unblocking STAGED barrier cascade
    P1_BACKFILL = "P1_BACKFILL",// Priority 70-80: Existing work backfill/internal gaps
    P2_ACTIVE_NEW_WORK = "P2_ACTIVE_NEW_WORK",// Priority 40-60: Admitted new work initial fill
    P3_DISCOVERY = "P3_DISCOVERY",// Priority 20: Catalog discovery & sync
    FALLBACK = "FALLBACK"
}
export type WorkSchedulerState = 'NEW' | 'FILLING' | 'CAUGHT_UP' | 'UPDATING' | 'COMPLETE' | 'BLOCKED';
export interface ActiveWork {
    workId: string;
    workTitle: string;
    lane: 'P1' | 'P2';
    state: WorkSchedulerState;
    primarySource: string;
    admittedAt: string;
    lastActivityAt: string;
    totalChapters: number;
    publishedChapters: number;
    queuedChapters: number;
    inFlightChapters: number;
    frontierSortKey: number | null;
    criticalGapSortKey: number | null;
    criticalGapUnblockCount: number;
}
export interface WorkWatermark {
    workId: string;
    source: string;
    lastSeenChapter: number;
    lastSeenSortKey: number;
    lastSeenChapterId?: string;
    lastSourcePublishedAt?: string;
    lastDiscoveryAt: string;
}
export interface SchedulerDecision {
    jobId: string;
    workId: string;
    workTitle: string;
    chapterNumber: number;
    chapterSortKey: number;
    lane: SchedulerLane;
    reason: string;
    workState: WorkSchedulerState;
    source: string;
    waitTimeMs: number;
    decisionTime: string;
}
export interface SchedulerConfig {
    enabled: boolean;
    shadowMode: boolean;
    maxActiveNewWorks: number;
    maxActiveBackfillWorks: number;
    maxInflightPerWork: number;
    slidingWindowSize: number;
    slidingWindowMin: number;
    antiStarvationRatio: number;
}
export interface SchedulerMetrics {
    p0Queued: number;
    p1Queued: number;
    p2Queued: number;
    p3Waiting: number;
    activeNewWorksCount: number;
    activeBackfillWorksCount: number;
    fillingWorksCount: number;
    caughtUpWorksCount: number;
    blockedWorksCount: number;
    criticalGapsCount: number;
    stagedWaitingForGapCount: number;
    p0Completed1h: number;
    p1Completed1h: number;
    p2Completed1h: number;
    p0AvgWaitMs: number;
    p0P95WaitMs: number;
    lastUpdated: string;
}
export interface AcquireTelemetry {
    staffCheckMs: number;
    p0ProbeMs: number;
    criticalWorkAttempts: number;
    criticalWorkTimeMs: number;
    p1WorkAttempts: number;
    p1WorkTimeMs: number;
    p2WorkAttempts: number;
    p2WorkTimeMs: number;
    activeFallbackMs: number;
    admissionOnDemandMs: number;
    catalogFallbackMs: number;
    poolWaitTotalMs: number;
    sqlExecTotalMs: number;
    claimLockSqlMs: number;
    totalQueries: number;
    totalAcquireMs: number;
    worksTested: number;
}
