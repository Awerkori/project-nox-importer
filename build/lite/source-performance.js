import { BoundedSamples } from '../core/bounded-samples.js';
const MIN_PAGES_FOR_CLASSIFICATION = 12;
const MIN_CHAPTERS_FOR_CLASSIFICATION = 2;
const PAGE_EWMA_ALPHA = 0.15;
const CHAPTER_EWMA_ALPHA = 0.25;
// Soft Bayesian warm-start priors (weight = 6 pseudo-pages) prevent a cold-start
// slow-source stampede right after container boot while remaining fully dynamic:
// once real traffic accumulates >=12 pages or >=2 chapters, live EWMA and P95
// measurements completely govern promotion or demotion in either direction.
const WARM_START_FAST_PRIORS = new Set([
    'nexus',
    'taimumangas',
    'tankouhentai',
    'pizzariascan',
    'nocturnesummer',
]);
const WARM_START_SLOW_PRIORS = new Set([
    'mangaflix',
    'mangotoons',
    'hipercool',
    'manhastro',
]);
function percentile(samples, q) {
    if (!samples.length)
        return 0;
    const sorted = [...samples].sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * q));
    return sorted[idx];
}
export class SourcePerformanceTracker {
    states = new Map();
    useWarmStartPriors;
    constructor(options) {
        this.useWarmStartPriors = options?.useWarmStartPriors !== false;
    }
    getOrCreate(source) {
        let state = this.states.get(source);
        if (state)
            return state;
        let initialTier = 'NORMAL';
        let ewmaPageMs = 4_200;
        let ewmaChapterMs = 65_000;
        let priorPages = 0;
        if (this.useWarmStartPriors) {
            if (WARM_START_FAST_PRIORS.has(source)) {
                initialTier = 'FAST';
                ewmaPageMs = 1_650;
                ewmaChapterMs = 22_000;
                priorPages = 6;
            }
            else if (WARM_START_SLOW_PRIORS.has(source)) {
                initialTier = 'SLOW';
                ewmaPageMs = 11_500;
                ewmaChapterMs = 155_000;
                priorPages = 6;
            }
        }
        state = {
            tier: initialTier,
            priorPages,
            samplePages: 0,
            sampleChapters: 0,
            sampleFailures: 0,
            ewmaPageMs,
            ewmaChapterMs,
            ewmaPagesPerChapter: 24,
            ewmaBytesPerSec: Math.round((512 * 1024 * 1000) / Math.max(250, ewmaPageMs)),
            ewmaSuccessRate: 0.98,
            pageLatencySamples: new BoundedSamples(32),
            lastUpdatedAt: Date.now(),
        };
        this.states.set(source, state);
        return state;
    }
    /**
     * Record a real origin page download. Returns true if the source's speed tier changed.
     */
    recordPageDownload(source, durationMs, bytes) {
        if (!source)
            return false;
        const state = this.getOrCreate(source);
        const boundedMs = Math.max(20, Math.min(120_000, durationMs));
        const boundedBytes = Math.max(0, bytes);
        const prevTier = state.tier;
        state.samplePages++;
        state.pageLatencySamples.push(boundedMs);
        const alpha = state.samplePages <= 4 && state.priorPages === 0 ? 0.35 : PAGE_EWMA_ALPHA;
        state.ewmaPageMs = state.ewmaPageMs * (1 - alpha) + boundedMs * alpha;
        const instantBps = (boundedBytes * 1000) / Math.max(50, boundedMs);
        state.ewmaBytesPerSec = state.ewmaBytesPerSec * (1 - alpha) + instantBps * alpha;
        // Healthy page downloads gently reinforce reliability without erasing chapter errors immediately.
        state.ewmaSuccessRate = Math.min(1, state.ewmaSuccessRate * 0.97 + 0.03);
        state.lastUpdatedAt = Date.now();
        state.tier = this.computeNextTier(state);
        return state.tier !== prevTier;
    }
    /**
     * Record a completed or source-failed chapter execution. Returns true if tier changed.
     */
    recordChapterOutcome(source, outcome) {
        if (!source)
            return false;
        const state = this.getOrCreate(source);
        const prevTier = state.tier;
        const boundedDurationMs = Math.max(100, Math.min(600_000, outcome.durationMs));
        if (outcome.succeeded) {
            state.sampleChapters++;
            const alpha = state.sampleChapters <= 2 && state.priorPages === 0 ? 0.45 : CHAPTER_EWMA_ALPHA;
            state.ewmaChapterMs = state.ewmaChapterMs * (1 - alpha) + boundedDurationMs * alpha;
            if (outcome.pageCount && outcome.pageCount > 0) {
                state.ewmaPagesPerChapter = state.ewmaPagesPerChapter * (1 - alpha) + outcome.pageCount * alpha;
            }
            state.ewmaSuccessRate = state.ewmaSuccessRate * (1 - CHAPTER_EWMA_ALPHA) + 1 * CHAPTER_EWMA_ALPHA;
        }
        else {
            state.sampleFailures++;
            state.ewmaSuccessRate = state.ewmaSuccessRate * (1 - CHAPTER_EWMA_ALPHA);
            // Penalize effective slot duration on upstream failures so failing sources lose priority slots cleanly.
            state.ewmaChapterMs = Math.min(300_000, state.ewmaChapterMs * 0.85 + Math.max(boundedDurationMs, 120_000) * 0.15);
        }
        state.lastUpdatedAt = Date.now();
        state.tier = this.computeNextTier(state);
        return state.tier !== prevTier;
    }
    hasEnoughEvidence(state) {
        return (state.samplePages >= MIN_PAGES_FOR_CLASSIFICATION ||
            state.sampleChapters >= MIN_CHAPTERS_FOR_CLASSIFICATION ||
            state.sampleFailures >= 3);
    }
    computeNextTier(state) {
        if (!this.hasEnoughEvidence(state)) {
            return state.tier;
        }
        const p95Ms = state.pageLatencySamples.length >= 6
            ? percentile(state.pageLatencySamples, 0.95)
            : state.ewmaPageMs * 1.35;
        const normalizedChapterMs = state.ewmaPagesPerChapter > 24
            ? state.ewmaChapterMs * (24 / state.ewmaPagesPerChapter)
            : state.ewmaChapterMs;
        const provenProductiveChapters = state.sampleChapters >= 2 &&
            normalizedChapterMs <= 75_000 &&
            state.ewmaSuccessRate >= 0.90 &&
            state.ewmaPageMs < 7_500;
        // 1. Check SLOW conditions (with recovery hysteresis if currently SLOW).
        if (state.tier === 'SLOW') {
            const stillSlow = !provenProductiveChapters && (state.ewmaPageMs >= 5_200 ||
                (state.pageLatencySamples.length >= 8 && p95Ms >= 11_500 && state.ewmaPageMs >= 3_200) ||
                (state.sampleChapters >= 2 && normalizedChapterMs >= 95_000) ||
                state.ewmaSuccessRate < 0.84);
            if (stillSlow)
                return 'SLOW';
        }
        else {
            const enterSlow = !provenProductiveChapters && (state.ewmaPageMs >= 7_000 ||
                (state.pageLatencySamples.length >= 8 && p95Ms >= 15_000 && state.ewmaPageMs >= 4_500) ||
                (state.sampleChapters >= 2 && normalizedChapterMs >= 135_000) ||
                ((state.sampleChapters + state.sampleFailures) >= 3 && state.ewmaSuccessRate < 0.72));
            if (enterSlow)
                return 'SLOW';
        }
        // 2. Check FAST conditions (with hysteresis if currently FAST).
        if (state.tier === 'FAST') {
            const retainFast = state.ewmaPageMs <= 3_500 &&
                (state.pageLatencySamples.length < 8 || p95Ms <= 7_800 || state.ewmaPageMs <= 2_000) &&
                state.ewmaSuccessRate >= 0.85 &&
                (state.sampleChapters === 0 || normalizedChapterMs <= 70_000);
            if (retainFast)
                return 'FAST';
            return 'NORMAL';
        }
        const enterFast = state.ewmaPageMs <= 2_800 &&
            (state.pageLatencySamples.length < 8 || p95Ms <= 6_500 || state.ewmaPageMs <= 1_800) &&
            state.ewmaSuccessRate >= 0.90 &&
            (state.sampleChapters === 0 || normalizedChapterMs <= 55_000);
        return enterFast ? 'FAST' : 'NORMAL';
    }
    getTier(source) {
        if (!source)
            return 'NORMAL';
        return this.getOrCreate(source).tier;
    }
    /** Numeric rank for tie-breaking healthy multi-source routes (0 = FAST, 1 = NORMAL, 2 = SLOW). */
    getSpeedRank(source) {
        const tier = this.getTier(source);
        if (tier === 'FAST')
            return 0;
        if (tier === 'NORMAL')
            return 1;
        return 2;
    }
    /** Productivity weight multiplier for primary worker allocation. */
    getDemandWeight(source) {
        const tier = this.getTier(source);
        if (tier === 'FAST')
            return 3.0;
        if (tier === 'NORMAL')
            return 1.0;
        return 0.35;
    }
    getTierMap(sources) {
        const out = new Map();
        if (sources) {
            for (const source of sources) {
                if (source)
                    out.set(source, this.getTier(source));
            }
        }
        for (const [source, state] of this.states) {
            out.set(source, state.tier);
        }
        return out;
    }
    getMetrics(source) {
        const state = this.getOrCreate(source);
        const p95Ms = state.pageLatencySamples.length > 0
            ? percentile(state.pageLatencySamples, 0.95)
            : state.ewmaPageMs;
        const chaptersPerSlotHour = Math.round(((3_600_000 / Math.max(5_000, state.ewmaChapterMs)) * state.ewmaSuccessRate) * 10) / 10;
        return {
            source,
            tier: state.tier,
            samplePages: state.samplePages,
            sampleChapters: state.sampleChapters,
            sampleFailures: state.sampleFailures,
            ewmaPageMs: Math.round(state.ewmaPageMs),
            p95PageMs: Math.round(p95Ms),
            ewmaChapterMs: Math.round(state.ewmaChapterMs),
            ewmaPagesPerChapter: Math.round(state.ewmaPagesPerChapter * 10) / 10,
            ewmaBytesPerSec: Math.round(state.ewmaBytesPerSec),
            ewmaSuccessRate: Math.round(state.ewmaSuccessRate * 1000) / 1000,
            chaptersPerSlotHour,
        };
    }
    snapshot(knownSources) {
        if (knownSources) {
            for (const s of knownSources) {
                if (s)
                    this.getOrCreate(s);
            }
        }
        const allMetrics = [...this.states.keys()]
            .sort()
            .map((source) => this.getMetrics(source));
        return {
            fastSources: allMetrics.filter((m) => m.tier === 'FAST').map((m) => m.source),
            normalSources: allMetrics.filter((m) => m.tier === 'NORMAL').map((m) => m.source),
            slowSources: allMetrics.filter((m) => m.tier === 'SLOW').map((m) => m.source),
            sources: allMetrics
                .filter((m) => m.samplePages > 0 || m.sampleChapters > 0 || m.sampleFailures > 0 || (knownSources ? [...knownSources].includes(m.source) : false))
                .sort((a, b) => this.getSpeedRank(a.source) - this.getSpeedRank(b.source) || b.samplePages - a.samplePages),
        };
    }
}
