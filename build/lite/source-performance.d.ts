export type SourceSpeedTier = 'FAST' | 'NORMAL' | 'SLOW';
export type SourcePerformanceMetrics = {
    source: string;
    tier: SourceSpeedTier;
    samplePages: number;
    sampleChapters: number;
    sampleFailures: number;
    ewmaPageMs: number;
    p95PageMs: number;
    ewmaChapterMs: number;
    ewmaPagesPerChapter: number;
    ewmaBytesPerSec: number;
    ewmaSuccessRate: number;
    chaptersPerSlotHour: number;
};
export type SourcePerformanceSnapshot = {
    fastSources: string[];
    normalSources: string[];
    slowSources: string[];
    sources: SourcePerformanceMetrics[];
};
export declare class SourcePerformanceTracker {
    private readonly states;
    private readonly useWarmStartPriors;
    constructor(options?: {
        useWarmStartPriors?: boolean;
    });
    private getOrCreate;
    /**
     * Record a real origin page download. Returns true if the source's speed tier changed.
     */
    recordPageDownload(source: string, durationMs: number, bytes: number): boolean;
    /**
     * Record a completed or source-failed chapter execution. Returns true if tier changed.
     */
    recordChapterOutcome(source: string, outcome: {
        succeeded: boolean;
        durationMs: number;
        pageCount?: number;
        totalBytes?: number;
    }): boolean;
    private hasEnoughEvidence;
    private computeNextTier;
    getTier(source: string): SourceSpeedTier;
    /** Numeric rank for tie-breaking healthy multi-source routes (0 = FAST, 1 = NORMAL, 2 = SLOW). */
    getSpeedRank(source: string): number;
    /** Productivity weight multiplier for primary worker allocation. */
    getDemandWeight(source: string): number;
    getTierMap(sources?: Iterable<string>): Map<string, SourceSpeedTier>;
    getMetrics(source: string): SourcePerformanceMetrics;
    snapshot(knownSources?: Iterable<string>): SourcePerformanceSnapshot;
}
