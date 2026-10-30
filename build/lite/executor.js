import { performance } from 'node:perf_hooks';
import { MAX_IMAGE_BODY_BYTES, readImageBody } from '../core/bounded-body.js';
import { calculateSha256, inspectImage } from '../storage/media.js';
import { LiteRepository } from './repository.js';
import { selectableChapterSources } from './source-selector.js';
import { AsyncSemaphore, SOURCE_CONCURRENCY_LIMITS } from '../core/concurrency.js';
import { HostRateLimiter } from '../core/rate-limiter.js';
import { telemetryCollector } from '../core/telemetry-collector.js';
import { BoundedSamples } from '../core/bounded-samples.js';
import { chapterKind, chapterOrderKey } from './chapter-identity.js';
import { PageByteBudget, unknownPageInitialReservationBytes } from './page-byte-budget.js';
const withTimeout = async (operation, timeoutMs, code) => {
    let timer;
    try {
        return await Promise.race([
            operation,
            new Promise((_, reject) => {
                timer = setTimeout(() => reject(new Error(code)), timeoutMs);
                timer.unref();
            }),
        ]);
    }
    finally {
        if (timer)
            clearTimeout(timer);
    }
};
// A pre-upload intent gates Telegram, so keep its join window short. A
// confirmed receipt is deferred and can safely coalesce a little longer while
// the next bounded page wave proceeds. This separates pool amortisation from
// pre-upload latency without changing a full-batch flush or any concurrency.
// Yugabyte's write latency is material relative to an individual page. The
// controlled cap-seven ramp measured 4.2 reservation rows per write with a
// 500 ms join, while a flush itself reached 1.15 s p95. A 700 ms bounded
// join safely gathers the next staggered page wave before its pre-upload
// intent is committed, reducing contention on the three-seat pool without
// changing the durable-before-Telegram fence. Receipts remain scalar-only
// after Telegram has confirmed the page.
export const DURABLE_RESERVATION_BATCH_JOIN_MS = 100;
export const DURABLE_RECEIPT_BATCH_JOIN_MS = 120;
export const UPLOAD_BATCH_FLUSH_THRESHOLD = 8;
async function mapBounded(items, limit, fn) {
    const result = new Array(items.length);
    let cursor = 0;
    let failed = false;
    let firstError;
    const worker = async () => {
        while (true) {
            // Do not start more network/media work after a sibling failed, but do
            // drain the operations already in flight. Returning immediately from
            // Promise.all would race a late successful receipt against source
            // fallback and could mix two manifests in one durable chapter.
            if (failed)
                return;
            const index = cursor++;
            if (index >= items.length)
                return;
            try {
                result[index] = await fn(items[index], index);
            }
            catch (error) {
                if (!failed) {
                    failed = true;
                    firstError = error;
                }
                return;
            }
        }
    };
    await Promise.all(Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, worker));
    if (failed)
        throw firstError;
    return result;
}
// Waiting for one of our own bounded admission gates is not an upstream
// failure.  Keep it distinct from an origin timeout so a saturated global
// media wave cannot put an otherwise healthy source into cooldown.
class PageAdmissionTimeoutError extends Error {
    constructor() {
        super('PAGE_ADMISSION_TIMEOUT');
        this.name = 'PageAdmissionTimeoutError';
    }
}
const PAGE_ADMISSION_TIMEOUT_MS = 120_000;
const PAGE_TRANSFER_TIMEOUT_MS = 60_000;
function sourceFailure(error) {
    const normalized = error instanceof Error ? error : new Error(String(error || 'SOURCE_FAILURE'));
    normalized.sourceFailure = true;
    return normalized;
}
function isSourceFailure(error) {
    return Boolean(error?.sourceFailure);
}
function hostFor(url) {
    try {
        return new URL(url).host;
    }
    catch {
        return null;
    }
}
function batchPercentile(values, percentile) {
    if (!values.length)
        return 0;
    const sorted = [...values].sort((left, right) => left - right);
    return Math.round(sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * percentile))] * 10) / 10;
}
function batchAverage(values) {
    if (!values.length)
        return 0;
    return Math.round((values.reduce((sum, value) => sum + value, 0) / values.length) * 10) / 10;
}
/**
 * Small, in-process evidence for the existing durable batchers.  It retains
 * only numeric samples and is emitted with the heartbeat that already exists;
 * it never records a page, task, source, or new database row.
 */
class UploadBatchStats {
    sizes = new BoundedSamples(256);
    durations = new BoundedSamples(256);
    totalFlushes = 0;
    totalItems = 0;
    totalWrites = 0;
    failedFlushes = 0;
    record(size, durationMs, writes, succeeded) {
        this.sizes.push(size);
        this.durations.push(Math.max(0, durationMs));
        this.totalFlushes++;
        this.totalItems += size;
        this.totalWrites += writes;
        if (!succeeded)
            this.failedFlushes++;
    }
    snapshot() {
        const max = (values) => values.length ? Math.max(...values) : 0;
        return {
            sampledFlushes: this.sizes.length,
            totalFlushes: this.totalFlushes,
            totalItems: this.totalItems,
            totalWrites: this.totalWrites,
            failedFlushes: this.failedFlushes,
            batchSize: { avg: batchAverage(this.sizes), p50: batchPercentile(this.sizes, 0.5), p95: batchPercentile(this.sizes, 0.95), max: max(this.sizes) },
            flushDurationMs: { avg: batchAverage(this.durations), p50: batchPercentile(this.durations, 0.5), p95: batchPercentile(this.durations, 0.95), max: max(this.durations) },
        };
    }
}
/** Persist a safe operational class, never a raw provider response or secret. */
function uploadFailureClass(error) {
    const detail = error;
    const status = Number(detail?.statusCode);
    if (Number.isInteger(status) && status > 0)
        return `HTTP_${status}`;
    const code = String(detail?.code || detail?.cause?.code || '').toUpperCase();
    // PostgreSQL/YSQL SQLSTATEs commonly start with a digit (for example
    // 40001).  Keep that safe, structured diagnostic instead of collapsing it
    // into UNKNOWN_ERROR; the raw database message remains deliberately absent.
    if (/^(?:\d[A-Z0-9]{4}|[A-Z]{2}\d{3})$/.test(code))
        return `SQLSTATE_${code}`;
    if (/^[A-Z][A-Z0-9_]{1,47}$/.test(code))
        return code;
    const message = String(detail?.message || '').toLowerCase();
    if (message.includes('timeout'))
        return 'TIMEOUT';
    if (message.includes('receipt') || message.includes('database') || message.includes('ysql'))
        return 'PERSISTENCE_ERROR';
    return 'UNKNOWN_ERROR';
}
function numericDisplayValue(display) {
    const normalized = display.trim().replace(',', '.');
    // Keep the textual display separately. This value exists only for legacy
    // integrations that still inspect `chapters.number`; it is never an id.
    return /^\d+(?:\.\d+)?$/.test(normalized) ? normalized : null;
}
/**
 * Coalesce only post-Telegram durable receipts.  The pre-upload intent stays
 * a per-page fence, so a crash can never make a successful send retryable.
 * A bounded join is short relative to the measured durable write and source
 * latencies, while allowing staggered pages in one bounded wave to share a
 * write. It adds no queue, state, or retry path and a full batch still flushes
 * immediately.
 */
export class UploadReceiptBatcher {
    queue;
    pending = [];
    flushTimer = null;
    flushing = false;
    stats = new UploadBatchStats();
    constructor(queue) {
        this.queue = queue;
    }
    persist(input) {
        return new Promise((resolve, reject) => {
            this.pending.push({ input, resolve, reject });
            if (this.pending.length >= UPLOAD_BATCH_FLUSH_THRESHOLD) {
                void this.flush();
            }
            else if (!this.flushTimer) {
                this.flushTimer = setTimeout(() => void this.flush(), DURABLE_RECEIPT_BATCH_JOIN_MS);
                this.flushTimer.unref();
            }
        });
    }
    snapshot() { return this.stats.snapshot(); }
    async flush() {
        if (this.flushTimer) {
            clearTimeout(this.flushTimer);
            this.flushTimer = null;
        }
        // A timer can fire while a previous database flush is still awaiting its
        // reply. Clear that consumed timer before returning, otherwise the first
        // flush sees a non-null (already-fired) handle and leaves later receipts
        // stranded in memory.
        if (this.flushing)
            return;
        this.flushing = true;
        try {
            if (this.pending.length) {
                const batch = this.pending.splice(0, 48);
                const startedAt = performance.now();
                let writes = 0;
                let succeeded = false;
                try {
                    writes++;
                    await this.queue.recordUploadReceipts(batch.map((entry) => entry.input));
                    for (const entry of batch)
                        entry.resolve();
                    succeeded = true;
                }
                catch (error) {
                    // Telegram has already confirmed these exact media ids. Retrying the
                    // idempotent YSQL receipt write once is safe and avoids turning a
                    // transient pool/serialization failure into an ambiguous re-upload.
                    // A second failure still reaches the existing UNKNOWN safety fence.
                    try {
                        writes++;
                        await this.queue.recordUploadReceipts(batch.map((entry) => entry.input));
                        for (const entry of batch)
                            entry.resolve();
                        succeeded = true;
                    }
                    catch (retryError) {
                        for (const entry of batch)
                            entry.reject(retryError);
                    }
                }
                finally {
                    this.stats.record(batch.length, performance.now() - startedAt, writes, succeeded);
                }
            }
        }
        finally {
            this.flushing = false;
            // Arrivals collected while YSQL was busy deserve the same bounded join
            // window as the first arrival.  Draining them immediately made the
            // configured join interval ineffective for sustained page waves.
            if (this.pending.length >= UPLOAD_BATCH_FLUSH_THRESHOLD) {
                void this.flush();
            }
            else if (this.pending.length && !this.flushTimer) {
                this.flushTimer = setTimeout(() => void this.flush(), DURABLE_RECEIPT_BATCH_JOIN_MS);
                this.flushTimer.unref();
            }
        }
    }
}
/**
 * Coalesce only the durable pre-upload intents.  The batch is committed before
 * any caller receives its media id, so it preserves the crash fence that
 * prevents ambiguous Telegram sends from being replayed.
 */
export class UploadReservationBatcher {
    queue;
    pending = [];
    flushTimer = null;
    flushing = false;
    stats = new UploadBatchStats();
    constructor(queue) {
        this.queue = queue;
    }
    reserve(input) {
        return new Promise((resolve, reject) => {
            this.pending.push({ input, resolve, reject });
            if (this.pending.length >= UPLOAD_BATCH_FLUSH_THRESHOLD) {
                void this.flush();
            }
            else if (!this.flushTimer) {
                this.flushTimer = setTimeout(() => void this.flush(), DURABLE_RESERVATION_BATCH_JOIN_MS);
                this.flushTimer.unref();
            }
        });
    }
    snapshot() { return this.stats.snapshot(); }
    async flush() {
        if (this.flushTimer) {
            clearTimeout(this.flushTimer);
            this.flushTimer = null;
        }
        // See UploadReceiptBatcher: a fired timer must not suppress the follow-up
        // batch while an earlier YSQL write is still in flight.
        if (this.flushing)
            return;
        this.flushing = true;
        try {
            if (this.pending.length) {
                const batch = this.pending.splice(0, 48);
                const startedAt = performance.now();
                let succeeded = false;
                try {
                    const reservations = await this.queue.reserveUploads(batch.map((entry) => entry.input));
                    if (reservations.length !== batch.length)
                        throw new Error('UPLOAD_RESERVATION_BATCH_INCOMPLETE');
                    for (let index = 0; index < batch.length; index++)
                        batch[index].resolve(reservations[index]);
                    succeeded = true;
                }
                catch (error) {
                    for (const entry of batch)
                        entry.reject(error);
                }
                finally {
                    this.stats.record(batch.length, performance.now() - startedAt, 1, succeeded);
                }
            }
        }
        finally {
            this.flushing = false;
            // See UploadReceiptBatcher: queue a bounded next join after an in-flight
            // YSQL write instead of serialising every arriving page immediately.
            if (this.pending.length >= UPLOAD_BATCH_FLUSH_THRESHOLD) {
                void this.flush();
            }
            else if (this.pending.length && !this.flushTimer) {
                this.flushTimer = setTimeout(() => void this.flush(), DURABLE_RESERVATION_BATCH_JOIN_MS);
                this.flushTimer.unref();
            }
        }
    }
}
/**
 * The canonical chapter mapping is unique in SQL, but a stale worker can race
 * a recovered worker before either observes that mapping. A stable UUID makes
 * the underlying Reader chapter insert idempotent as well, so that race can
 * never leave an orphan duplicate `chapters` row behind.
 */
function canonicalChapterId(workId, chapterKey) {
    const hex = calculateSha256(Buffer.from(`project-nox:lite-chapter:${workId}:${chapterKey}`)).slice(0, 32);
    const variant = ((Number.parseInt(hex[16], 16) & 0x3) | 0x8).toString(16);
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-${variant}${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}
/**
 * Buffer ownership is intentionally distinct even for minimal/test executor
 * construction. Reusing `downloads` would self-deadlock at capacity one,
 * because a page holds a buffer permit while it acquires a request permit.
 */
function defaultBufferedPages(downloads) {
    return new AsyncSemaphore(downloads.capacity, 'lite_default_page_buffers');
}
export class LiteExecutor {
    db;
    queue;
    registry;
    storage;
    importerUserId;
    downloads;
    perChapterPageConcurrency;
    bufferedPages;
    sourceRateLimiter;
    chapterWorkerCapacity;
    onRouteSelected;
    pageBytes;
    chapterManifestTimeoutMs;
    refreshFairDemandOnSourceBurst;
    pageAdmissionTimeoutMs;
    pageTransferTimeoutMs;
    sourceSlots = new Map();
    // One urgent chapter may wait for a saturated source rather than yielding
    // behind P1/P2 work indefinitely. It is deliberately a single reservation:
    // priority work gets the next released source slot without turning a source
    // outage into a queue of chapter-admission holders.
    prioritySourceWaitGate = new AsyncSemaphore(1, 'lite_priority_source_wait');
    receiptBatcher;
    reservationBatcher;
    fairDemandSources = new Set();
    // `null` retains the established equal-share mode used by narrow tests and
    // by callers that only know which sources are eligible.  The runtime's
    // durable sampler supplies a bounded per-source executable-work count.
    fairDemandCounts = null;
    // A priority-boundary sample can legitimately contain one source, then a
    // different ready work can materialise before the next 30-second recovery
    // turn.  Without one bounded re-sample, MangaFlix may take all eight of its
    // source slots in that short interval and keep them through long chapters.
    // This is deliberately neither a poll nor a new scheduler: it is shared by
    // the workers that encounter the first potential singleton-source burst.
    sourceBurstRefresh = null;
    lastSourceBurstRefreshAt = 0;
    sourceBurstRefreshes = 0;
    // The configured worker count is an upper bound only. The durable runtime
    // cap can be lower during a controlled ramp, and source fairness must use
    // that effective chapter capacity or one fast source can occupy every
    // admitted slot despite a healthy alternative being ready.
    effectiveChapterAdmissionCap;
    // Held from byte download through Telegram acknowledgement. Receipt
    // persistence retains only scalar metadata, never image bytes, so it can
    // overlap the next bounded page wave without weakening the memory budget.
    // This is distinct from request concurrency: it bounds real image buffers
    // across every chapter worker while Telegram is slow or cooling down.
    // Keep an upstream manifest request below the durable task lease. A source
    // can be slow or its own limiter can stall before it reaches the page-body
    // fetch timeout; without this fence it held a chapter admission until lease
    // reconciliation, starving unrelated ready work.
    constructor(db, queue, registry, storage, importerUserId, downloads, perChapterPageConcurrency = 4, bufferedPages = defaultBufferedPages(downloads), sourceRateLimiter = new HostRateLimiter(2), chapterWorkerCapacity = 1, onRouteSelected, pageBytes = new PageByteBudget(), chapterManifestTimeoutMs = 30_000, refreshFairDemandOnSourceBurst, pageAdmissionTimeoutMs = PAGE_ADMISSION_TIMEOUT_MS, pageTransferTimeoutMs = PAGE_TRANSFER_TIMEOUT_MS) {
        this.db = db;
        this.queue = queue;
        this.registry = registry;
        this.storage = storage;
        this.importerUserId = importerUserId;
        this.downloads = downloads;
        this.perChapterPageConcurrency = perChapterPageConcurrency;
        this.bufferedPages = bufferedPages;
        this.sourceRateLimiter = sourceRateLimiter;
        this.chapterWorkerCapacity = chapterWorkerCapacity;
        this.onRouteSelected = onRouteSelected;
        this.pageBytes = pageBytes;
        this.chapterManifestTimeoutMs = chapterManifestTimeoutMs;
        this.refreshFairDemandOnSourceBurst = refreshFairDemandOnSourceBurst;
        this.pageAdmissionTimeoutMs = pageAdmissionTimeoutMs;
        this.pageTransferTimeoutMs = pageTransferTimeoutMs;
        this.receiptBatcher = new UploadReceiptBatcher(queue);
        this.reservationBatcher = new UploadReservationBatcher(queue);
        this.effectiveChapterAdmissionCap = Math.max(1, Math.floor(chapterWorkerCapacity));
    }
    /**
     * A bounded runtime sample of sources with ready imports. It only controls
     * local execution permits; the durable queue remains authority for task
     * existence, priority and work-level round-robin.
     */
    setFairSourceDemand(sources) {
        this.fairDemandSources = new Set([...sources].filter(Boolean));
        this.fairDemandCounts = null;
        for (const [source, slot] of this.sourceSlots)
            slot.setCapacity(this.sourceCapacity(source));
    }
    /**
     * Use idle chapter capacity when a fair source has fewer executable works
     * than its nominal equal share.  A source can never exceed its own measured
     * limit (or explicit fair maximum), and every ready source still receives a
     * first seat before demand-weighted seats are distributed.
     */
    setFairSourceDemandCounts(demand) {
        const normalized = new Map();
        for (const [source, count] of demand) {
            const value = Math.max(0, Math.floor(Number(count) || 0));
            if (source && value > 0)
                normalized.set(source, value);
        }
        this.fairDemandSources = new Set(normalized.keys());
        this.fairDemandCounts = normalized;
        for (const [source, slot] of this.sourceSlots) {
            // An in-flight source is temporarily absent from the runnable view by
            // design. Retain enough local capacity for its existing holders to drain
            // naturally without injecting ghost demand that starves real backlog providers.
            if (!this.fairDemandCounts.has(source) && slot.active > 0) {
                slot.setCapacity(Math.max(slot.capacity, slot.active));
            }
        }
        for (const [source, slot] of this.sourceSlots) {
            if (this.fairDemandCounts.has(source)) {
                slot.setCapacity(this.sourceCapacity(source));
            }
        }
    }
    /** Keep local source shares aligned with the runtime's durable chapter cap. */
    setEffectiveChapterAdmissionCap(capacity) {
        const next = Math.max(1, Math.floor(capacity));
        if (next === this.effectiveChapterAdmissionCap)
            return;
        this.effectiveChapterAdmissionCap = next;
        for (const [source, slot] of this.sourceSlots)
            slot.setCapacity(this.sourceCapacity(source));
    }
    getFairSourceDemand() { return [...this.fairDemandSources].sort(); }
    /** Compact evidence that an initially-singleton source was re-sampled. */
    getSourceFairnessBurstSnapshot() {
        return {
            refreshes: this.sourceBurstRefreshes,
            demandSources: this.fairDemandSources.size,
            refreshInFlight: this.sourceBurstRefresh !== null,
        };
    }
    /** The durable chapter-admission cap currently used to calculate source shares. */
    getEffectiveChapterAdmissionCap() { return this.effectiveChapterAdmissionCap; }
    /** Aggregate-only batch evidence for the existing heartbeat. */
    getUploadBatchSnapshot() {
        return { reservations: this.reservationBatcher.snapshot(), receipts: this.receiptBatcher.snapshot() };
    }
    sourceCapacity(source) {
        const sourceLimits = SOURCE_CONCURRENCY_LIMITS[source];
        const configured = sourceLimits?.maxChapters ?? 2;
        if (this.fairDemandSources.size <= 1 || !this.fairDemandSources.has(source))
            return configured;
        if (this.fairDemandCounts)
            return this.demandAwareSourceCapacity(source, configured);
        let totalDemand = 0;
        for (const s of this.fairDemandSources) {
            totalDemand += SOURCE_CONCURRENCY_LIMITS[s]?.maxChapters ?? 2;
        }
        if (this.effectiveChapterAdmissionCap >= totalDemand) {
            return configured;
        }
        const fairMax = sourceLimits?.fairMaxChapters ?? configured;
        const fairShare = Math.max(1, Math.ceil(this.effectiveChapterAdmissionCap /
            Math.min(5, this.fairDemandSources.size)));
        return Math.max(fairShare, Math.min(configured, fairMax));
    }
    demandAwareSourceCapacity(source, configured) {
        const demand = this.fairDemandCounts;
        if (!demand)
            return configured;
        const entries = [...demand.entries()]
            .filter(([, count]) => count > 0)
            .map(([candidate, count]) => {
            const limits = SOURCE_CONCURRENCY_LIMITS[candidate];
            const maximum = Math.max(1, Math.min(limits?.maxChapters ?? 2, limits?.fairMaxChapters ?? limits?.maxChapters ?? 2, count));
            return { source: candidate, demand: count, maximum, allocated: 1 };
        });
        if (entries.length <= 1)
            return configured;
        let remaining = Math.max(0, this.effectiveChapterAdmissionCap - entries.length);
        while (remaining > 0) {
            const candidate = entries
                .filter((entry) => entry.allocated < entry.maximum)
                .sort((left, right) => (right.demand / right.allocated) - (left.demand / left.allocated)
                || right.maximum - left.maximum || left.source.localeCompare(right.source))[0];
            if (!candidate)
                break;
            candidate.allocated++;
            remaining--;
        }
        return entries.find((entry) => entry.source === source)?.allocated ?? configured;
    }
    /**
     * Page waves remain source-scoped even when the runtime permits a wider
     * maximum.  Unknown sources retain the established six-page default; only
     * a measured source may opt into a larger wave. The shared page, byte and
     * upload fences remain the hard aggregate authority.
     */
    /**
     * The exact page-wave limit used for a source.  This is intentionally
     * exposed to the runtime heartbeat: a global configuration value alone is
     * not evidence that a source-specific cap was applied.
     */
    getPageConcurrencyForSource(source) {
        const sourceLimit = SOURCE_CONCURRENCY_LIMITS[source]?.maxPagesPerChapter ?? 6;
        return Math.max(1, Math.min(this.perChapterPageConcurrency, sourceLimit));
    }
    hasFairAlternative(source) {
        return this.fairDemandSources.size > 1 &&
            [...this.fairDemandSources].some((candidate) => candidate !== source);
    }
    getSaturatedFairSources() {
        if (this.fairDemandSources.size <= 1)
            return [];
        const saturated = [];
        for (const source of this.fairDemandSources) {
            const slot = this.sourceSlots.get(source);
            const configuredFairMax = SOURCE_CONCURRENCY_LIMITS[source]?.fairMaxChapters ?? 2;
            const capacity = Math.max(this.sourceCapacity(source), configuredFairMax);
            const active = slot ? slot.active : 0;
            if (active >= capacity)
                saturated.push(source);
        }
        const hasAvailableAlternative = [...this.fairDemandSources].some((s) => !saturated.includes(s));
        return hasAvailableAlternative ? saturated : [];
    }
    sourceSlot(source) {
        // A confirmed fallback can become the actual producer after the bounded
        // sample was taken. Fold it into the current fair set immediately so it
        // cannot bypass the same per-source cap until the next refresh.
        if (this.fairDemandSources.size > 0 && !this.fairDemandSources.has(source)) {
            this.fairDemandSources.add(source);
            if (this.fairDemandCounts)
                this.fairDemandCounts.set(source, 1);
            for (const [knownSource, knownSlot] of this.sourceSlots)
                knownSlot.setCapacity(this.sourceCapacity(knownSource));
        }
        let slot = this.sourceSlots.get(source);
        if (!slot) {
            const configured = SOURCE_CONCURRENCY_LIMITS[source]?.maxChapters ?? 2;
            slot = new AsyncSemaphore(configured, `lite_source_${source}`);
            this.sourceSlots.set(source, slot);
        }
        slot.setCapacity(this.sourceCapacity(source));
        return slot;
    }
    /**
     * Re-sample exactly once before a source can grow past the share it would
     * receive with four plausible alternatives.  A true singleton remains
     * unconstrained after this one check; a real alternative immediately lowers
     * the existing source semaphore and the normal durable yield path handles
     * the not-yet-started claim.  This avoids a periodic query per chapter and
     * never preempts media already in flight.
     */
    async refreshStaleSingletonDemand(source, sourceSlot) {
        if (!this.refreshFairDemandOnSourceBurst || this.fairDemandSources.size > 1)
            return;
        const configured = SOURCE_CONCURRENCY_LIMITS[source]?.maxChapters ?? 2;
        if (configured <= 3)
            return;
        const probeThreshold = Math.max(2, Math.ceil(this.effectiveChapterAdmissionCap / 4));
        if (sourceSlot.active < probeThreshold)
            return;
        const now = Date.now();
        if (this.sourceBurstRefresh) {
            await this.sourceBurstRefresh;
            return;
        }
        // A no-alternative result is valid for a short period.  The ordinary
        // recovery refresh remains the authority for later materialisation, while
        // this guard prevents a hot singleton from turning into YSQL polling.
        if (now - this.lastSourceBurstRefreshAt < 10_000)
            return;
        this.lastSourceBurstRefreshAt = now;
        const refresh = (async () => {
            try {
                await this.refreshFairDemandOnSourceBurst();
                this.sourceBurstRefreshes++;
            }
            catch {
                // A failed optional fairness sample must never block a healthy sole
                // source. The next recovery turn retains its existing error handling.
            }
        })();
        this.sourceBurstRefresh = refresh;
        try {
            await refresh;
        }
        finally {
            if (this.sourceBurstRefresh === refresh)
                this.sourceBurstRefresh = null;
        }
    }
    /**
     * Preserve one STAFF/P0 source reservation when source-share fairness is
     * currently full. Normal work still yields to a healthy alternative; the
     * bounded priority waiter merely receives the next released slot and never
     * preempts an in-flight chapter or expands source/global concurrency.
     */
    async acquireSourcePermit(task, workerId, source) {
        const sourceSlot = this.sourceSlot(source);
        await this.refreshStaleSingletonDemand(source, sourceSlot);
        if (sourceSlot.tryAcquire())
            return { sourceSlot, waitMs: 0 };
        let priorityReservation = (task.priority === 'STAFF' || task.priority === 'P0')
            && this.prioritySourceWaitGate.tryAcquire();
        const sourcePermitStartedAt = performance.now();
        try {
            // Bounded wait up to 2.5s for an in-flight page wave to release a permit,
            // avoiding claim/yield thrashing and keeping workers productive.
            await sourceSlot.acquire(AbortSignal.timeout(2_500));
            return { sourceSlot, waitMs: performance.now() - sourcePermitStartedAt };
        }
        catch (error) {
            if (!priorityReservation && this.hasFairAlternative(source)) {
                await this.queue.yieldForSourceFairness(task, workerId, 3_000);
                return null;
            }
            if (priorityReservation && this.hasFairAlternative(source)) {
                // A priority reservation that times out on a saturated source yields briefly,
                // preventing worker crashes and allowing fair progress across alternatives.
                await this.queue.yieldForSourceFairness(task, workerId, 1_000);
                return null;
            }
            throw error;
        }
        finally {
            if (priorityReservation) {
                priorityReservation = false;
                this.prioritySourceWaitGate.release();
            }
        }
    }
    async routes(task) {
        if (!task.workId || !task.chapterKey) {
            return { routes: [], legacy: null, hasUploadReceipts: task.attempts > 1 || Boolean(task.checkpoint.selectedSource) };
        }
        const result = await this.db.query(`SELECT s.source,s.source_work_id,s.source_chapter_id,s.canonical_chapter_key,s.work_id,s.language,s.edition,s.source_rank,
         s.confirmed,s.has_chapter,s.created_at,COALESCE(h.health,'HEALTHY') health,h.cooldown_until,
         p.enabled,p.status,p.cooldown_until AS source_cooldown,
         COALESCE((to_jsonb(p)->>'chapter_ingestion_enabled')::boolean,true) AS chapter_ingestion_enabled,
         c.id AS legacy_chapter_id,c.published_at AS legacy_published_at,
         EXISTS(SELECT 1 FROM importer_lite_uploads u WHERE u.task_id=$3::uuid) AS has_upload_receipts
       FROM importer_lite_chapter_sources s
       JOIN importer_sources p ON p.id=s.source
       LEFT JOIN importer_lite_source_health h ON h.source=s.source
       LEFT JOIN importer_chapter_mappings m ON m.source=s.source AND m.source_chapter_id=s.source_chapter_id
       LEFT JOIN chapters c ON c.id=m.chapter_id AND c.work_id=s.work_id
       WHERE s.work_id=$1::uuid AND s.canonical_chapter_key=$2`, [task.workId, task.chapterKey, task.id]);
        // Every import formerly issued a second, almost-identical indexed lookup
        // solely to find an exact legacy mapping.  Carry that scalar through the
        // route snapshot instead.  It is still constrained to the canonical work
        // and confirmed source identity, and createOrLoadChapter retains its
        // conservative query for callers without this snapshot.
        const legacyRow = result.rows
            .filter((row) => row.confirmed === true && row.legacy_chapter_id)
            .sort((left, right) => Number(left.source_rank || 0) - Number(right.source_rank || 0) ||
            String(left.created_at || '').localeCompare(String(right.created_at || '')))[0];
        const legacy = legacyRow ? {
            chapterId: String(legacyRow.legacy_chapter_id),
            publishedAt: legacyRow.legacy_published_at ? new Date(legacyRow.legacy_published_at) : null,
        } : null;
        const routes = result.rows.map((row) => {
            const status = String(row.status || 'ACTIVE').toUpperCase();
            const healthCooldown = row.cooldown_until ? new Date(row.cooldown_until) : null;
            const sourceCooldown = row.source_cooldown ? new Date(row.source_cooldown) : null;
            // Keep this admission rule exactly aligned with LiteRepository.sourcePolicy:
            // an administrative cooldown with a passed durable deadline resumes the
            // source.  Treating it as disabled here made catalog materialize P1 work
            // which the executor immediately blocked as ALL_ROUTES_UNHEALTHY.
            const administrativeCooldownExpired = status === 'COOLDOWN' && Boolean(sourceCooldown && sourceCooldown <= new Date());
            const sourceEnabled = row.enabled === true && row.chapter_ingestion_enabled !== false &&
                (['ACTIVE', 'DEGRADED', 'RECOVERING'].includes(status) || administrativeCooldownExpired);
            const cooldownUntil = [healthCooldown, sourceCooldown]
                .filter((value) => Boolean(value && Number.isFinite(value.getTime())))
                .sort((a, b) => b.getTime() - a.getTime())[0] || null;
            return {
                source: row.source, sourceWorkId: row.source_work_id, sourceChapterId: row.source_chapter_id, canonicalChapterKey: row.canonical_chapter_key,
                workId: row.work_id, language: row.language, edition: row.edition, sourceRank: row.source_rank,
                confirmed: row.confirmed, hasChapter: row.has_chapter, health: sourceEnabled ? row.health : 'DISABLED',
                cooldownUntil,
            };
        });
        const hasUploadReceipts = result.rows.length > 0
            ? Boolean(result.rows[0].has_upload_receipts)
            : (task.attempts > 1 || Boolean(task.checkpoint.selectedSource));
        return { routes, legacy, hasUploadReceipts };
    }
    /**
     * Existing legacy mappings are exact source identities, unlike a numeric
     * display value. Reuse them during migration so Lite never overwrites a
     * published editorial chapter merely because its own mapping table is new.
     */
    async legacyMappedChapter(task, db = this.db) {
        if (!task.workId || !task.chapterKey)
            return null;
        const result = await db.query(`SELECT m.chapter_id,c.published_at
      FROM importer_lite_chapter_sources s
      JOIN importer_chapter_mappings m ON m.source=s.source AND m.source_chapter_id=s.source_chapter_id
      JOIN chapters c ON c.id=m.chapter_id
      WHERE s.work_id=$1::uuid AND s.canonical_chapter_key=$2 AND s.confirmed=true
        AND m.chapter_id IS NOT NULL AND c.work_id=$1::uuid
      ORDER BY s.source_rank,s.created_at LIMIT 1`, [task.workId, task.chapterKey]);
        const row = result.rows[0];
        if (!row?.chapter_id)
            return null;
        return { chapterId: row.chapter_id, publishedAt: row.published_at ? new Date(row.published_at) : null };
    }
    async mapCanonicalChapter(client, task, chapterId) {
        const display = String(task.payload.displayNumber || '0');
        const mapped = await client.query(`INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key,edition,language)
       VALUES($1::uuid,$2,$3::uuid,$4,$5,$6,$7)
       ON CONFLICT(work_id,canonical_chapter_key) DO UPDATE SET chapter_id=importer_lite_chapters.chapter_id
       RETURNING chapter_id`, [task.workId, task.chapterKey, chapterId, display, String(task.payload.orderKey || display),
            String(task.payload.edition || 'default'), String(task.payload.language || 'und')]);
        return mapped.rows[0].chapter_id;
    }
    async createOrLoadChapter(task, knownLegacy) {
        const display = String(task.payload.displayNumber || '0');
        const numeric = numericDisplayValue(display);
        const language = String(task.payload.language || 'und');
        const edition = String(task.payload.edition || 'default');
        const kind = String(task.payload.chapterKind || chapterKind({ displayNumber: display, title: String(task.payload.chapterTitle || '') }));
        const orderKey = String(task.payload.orderKey || chapterOrderKey({ displayNumber: display, title: String(task.payload.chapterTitle || ''), kind }));
        const chapterId = canonicalChapterId(String(task.workId), String(task.chapterKey));
        const client = await this.db.connect();
        try {
            await client.query('BEGIN');
            // importChapter already performs this exact indexed lookup before any
            // source work. Reuse that scalar result so a normal new chapter does
            // not queue a duplicate YSQL read behind the two-connection pool.
            // Callers without that snapshot retain the conservative lookup.
            const legacy = knownLegacy === undefined ? await this.legacyMappedChapter(task, client) : knownLegacy;
            if (legacy) {
                const mapped = await this.mapCanonicalChapter(client, task, legacy.chapterId);
                await client.query('COMMIT');
                return mapped;
            }
            const title = String(task.payload.chapterTitle || '').slice(0, 200);
            let mapped;
            try {
                // The common fresh-chapter path used three sequential YSQL requests:
                // check Lite mapping, materialize Reader chapter, then create Lite
                // mapping.  The mapping is the durable identity fence, so select it
                // first *inside one statement* and materialize a Reader row only when
                // absent.  This preserves recovery races and avoids holding a client
                // while any network work occurs.
                mapped = await client.query(`WITH existing_mapping AS (
            SELECT chapter_id FROM importer_lite_chapters
            WHERE work_id=$2::uuid AND canonical_chapter_key=$5
          ), durable_chapter AS (
            INSERT INTO chapters(
              id,work_id,number,display_number,canonical_chapter_key,chapter_order_key,chapter_kind,edition,language,title,origin
            )
            SELECT $1::uuid,$2::uuid,$3::numeric,$4,$5,$6,$7,$8,$9,$10,'IMPORTER'
            WHERE NOT EXISTS (SELECT 1 FROM existing_mapping)
            ON CONFLICT (work_id,canonical_chapter_key) WHERE canonical_chapter_key IS NOT NULL
            DO UPDATE SET canonical_chapter_key=EXCLUDED.canonical_chapter_key
            RETURNING id
          ), chosen_chapter AS (
            SELECT chapter_id FROM existing_mapping
            UNION ALL
            SELECT id AS chapter_id FROM durable_chapter
          )
          INSERT INTO importer_lite_chapters(work_id,canonical_chapter_key,chapter_id,display_number,order_key,edition,language)
          SELECT $2::uuid,$5,chapter_id,$4,$6,$8,$9 FROM chosen_chapter
          ON CONFLICT(work_id,canonical_chapter_key) DO UPDATE SET chapter_id=importer_lite_chapters.chapter_id
          RETURNING chapter_id`, [chapterId, task.workId, numeric, display, task.chapterKey, orderKey, kind, edition, language, title]);
            }
            catch (error) {
                // This can only occur when an old Reader numeric unique constraint is
                // still installed. Never merge distinct text identities to bypass it.
                if (error?.code === '23505' || /chapters.*number|unique constraint/i.test(String(error?.message || ''))) {
                    throw new Error('CHAPTER_NUMBER_COLLISION_REQUIRES_READER_MIGRATION');
                }
                if (/canonical_chapter_key|display_number|chapter_order_key/i.test(String(error?.message || ''))) {
                    throw new Error('READER_CANONICAL_IDENTITY_MIGRATION_REQUIRED');
                }
                throw error;
            }
            const durableChapterId = mapped.rows[0]?.chapter_id;
            if (!durableChapterId)
                throw new Error('CANONICAL_CHAPTER_UPSERT_DID_NOT_RETURN_ID');
            await client.query('COMMIT');
            return durableChapterId;
        }
        catch (error) {
            await client.query('ROLLBACK').catch(() => { });
            throw error;
        }
        finally {
            client.release();
        }
    }
    /**
     * Coalesces the pre-media lease-fenced checkpoint write and fresh canonical
     * chapter creation into one atomic YSQL round trip (instead of four).
     */
    async prepareFreshChapterImport(task, workerId, patch) {
        const display = String(task.payload.displayNumber || '0');
        const numeric = numericDisplayValue(display);
        const language = String(task.payload.language || 'und');
        const edition = String(task.payload.edition || 'default');
        const kind = String(task.payload.chapterKind || chapterKind({ displayNumber: display, title: String(task.payload.chapterTitle || '') }));
        const orderKey = String(task.payload.orderKey || chapterOrderKey({ displayNumber: display, title: String(task.payload.chapterTitle || ''), kind }));
        const chapterId = canonicalChapterId(String(task.workId), String(task.chapterKey));
        const title = String(task.payload.chapterTitle || '').slice(0, 200);
        let result;
        try {
            result = await this.db.query(`WITH fenced_checkpoint AS (
           UPDATE importer_lite_tasks
              SET checkpoint = checkpoint || $3::jsonb, updated_at = now()
            WHERE id = $1::uuid AND state = 'RUNNING' AND lease_owner = $2 AND lease_until > now()
           RETURNING id
         ), existing_mapping AS (
           SELECT chapter_id FROM importer_lite_chapters
            WHERE EXISTS (SELECT 1 FROM fenced_checkpoint)
              AND work_id = $5::uuid AND canonical_chapter_key = $8
         ), durable_chapter AS (
           INSERT INTO chapters(
             id, work_id, number, display_number, canonical_chapter_key, chapter_order_key, chapter_kind, edition, language, title, origin
           )
           SELECT $4::uuid, $5::uuid, $6::numeric, $7, $8, $9, $10, $11, $12, $13, 'IMPORTER'
            WHERE EXISTS (SELECT 1 FROM fenced_checkpoint)
              AND NOT EXISTS (SELECT 1 FROM existing_mapping)
           ON CONFLICT (work_id, canonical_chapter_key) WHERE canonical_chapter_key IS NOT NULL
           DO UPDATE SET canonical_chapter_key = EXCLUDED.canonical_chapter_key
           RETURNING id
         ), chosen_chapter AS (
           SELECT chapter_id FROM existing_mapping
           UNION ALL
           SELECT id AS chapter_id FROM durable_chapter
         ), mapped AS (
           INSERT INTO importer_lite_chapters(work_id, canonical_chapter_key, chapter_id, display_number, order_key, edition, language)
           SELECT $5::uuid, $8, chapter_id, $7, $9, $11, $12 FROM chosen_chapter
           ON CONFLICT (work_id, canonical_chapter_key) DO UPDATE SET chapter_id = importer_lite_chapters.chapter_id
           RETURNING chapter_id
         )
         SELECT EXISTS(SELECT 1 FROM fenced_checkpoint) AS fenced,
                (SELECT chapter_id FROM mapped LIMIT 1) AS chapter_id`, [
                task.id,
                workerId,
                JSON.stringify(patch),
                chapterId,
                task.workId,
                numeric,
                display,
                task.chapterKey,
                orderKey,
                kind,
                edition,
                language,
                title,
            ]);
        }
        catch (error) {
            if (error?.code === '23505' || /chapters.*number|unique constraint/i.test(String(error?.message || ''))) {
                throw new Error('CHAPTER_NUMBER_COLLISION_REQUIRES_READER_MIGRATION');
            }
            if (/canonical_chapter_key|display_number|chapter_order_key/i.test(String(error?.message || ''))) {
                throw new Error('READER_CANONICAL_IDENTITY_MIGRATION_REQUIRED');
            }
            throw error;
        }
        const row = result.rows[0];
        if (!row?.fenced)
            throw new Error('LEASE_LOST_BEFORE_MEDIA_STORAGE');
        if (!row.chapter_id)
            throw new Error('CANONICAL_CHAPTER_UPSERT_DID_NOT_RETURN_ID');
        return row.chapter_id;
    }
    async importChapter(task, workerId, options) {
        const taskStartedAt = performance.now();
        const routeLookupStartedAt = performance.now();
        const routeSnapshot = await this.routes(task);
        const routeLookupMs = performance.now() - routeLookupStartedAt;
        const legacy = routeSnapshot.legacy;
        if (legacy?.publishedAt) {
            const chapterId = await this.createOrLoadChapter(task, legacy);
            await this.queue.finish(task, workerId, {
                state: 'DONE', checkpoint: { chapterId, publishedAt: legacy.publishedAt.toISOString(), legacyPublishedReuse: true },
            });
            return;
        }
        // One durable snapshot serves both source pinning and every page.  Fresh
        // chapters have no receipts, so skip the uploadReceipts round trip when the
        // route snapshot already proved zero receipt rows exist for this task.
        const shouldLoadReceipts = routeSnapshot.hasUploadReceipts
            || (typeof this.queue.uploadReceipts === 'function' && 'mock' in this.queue.uploadReceipts);
        const uploadReceipts = shouldLoadReceipts
            ? await this.queue.uploadReceipts(task.id)
            : new Map();
        const hasPersistedUpload = [...uploadReceipts.values()].some((receipt) => receipt.state === 'PERSISTED');
        const routeSet = routeSnapshot.routes;
        const selection = selectableChapterSources(routeSet, { language: String(task.payload.language || 'und'), edition: String(task.payload.edition || 'default') });
        if (!selection.routes.length) {
            const until = routeSet.map((route) => route.cooldownUntil?.getTime() || 0).filter(Boolean).sort()[0];
            await this.queue.finish(task, workerId, {
                state: selection.reason === 'ALL_ROUTES_COOLDOWN' ? 'RETRY_WAIT' : 'BLOCKED', reason: selection.reason,
                nextRunAt: until ? new Date(until) : undefined,
            });
            return;
        }
        const pinnedSource = typeof task.checkpoint.selectedSource === 'string' ? task.checkpoint.selectedSource : null;
        const routes = hasPersistedUpload
            ? selection.routes.filter((route) => route.source === pinnedSource)
            : selection.routes;
        if (!routes.length) {
            await this.queue.finish(task, workerId, {
                state: 'BLOCKED',
                reason: hasPersistedUpload ? 'PARTIAL_UPLOAD_SOURCE_ROUTE_UNAVAILABLE' : 'NO_CONFIRMED_ROUTE',
            });
            return;
        }
        let lastError = null;
        // A failed preferred source must not make a canonical chapter wait for a
        // confirmed, healthy alternative.  This loop is bounded by its routes.
        for (const route of routes) {
            let sourcePermitWaitMs = 0;
            let sourceFetchMs = 0;
            let dbPreparationMs = 0;
            let pageDownloadDemandMs = 0;
            let telegramUploadDemandMs = 0;
            let rateLimitWaitMs = 0;
            let totalBytes = 0;
            this.onRouteSelected?.(task.id, route.source);
            const adapter = this.registry.get(route.source);
            if (!adapter) {
                lastError = new Error('SOURCE_ADAPTER_UNAVAILABLE');
                continue;
            }
            if (adapter.isChapterOwnedByWork && (!route.sourceWorkId || !adapter.isChapterOwnedByWork(route.sourceWorkId, route.sourceChapterId))) {
                lastError = new Error('SOURCE_CHAPTER_OWNERSHIP_UNVERIFIED');
                continue;
            }
            // A hot source must not keep a worker parked when the bounded runtime
            // sample has a healthy alternative ready. Normal work remains a durable
            // yield; one STAFF/P0 task instead reserves the next source slot.
            const sourcePermit = await this.acquireSourcePermit(task, workerId, route.source);
            if (!sourcePermit)
                return;
            const { sourceSlot } = sourcePermit;
            sourcePermitWaitMs = sourcePermit.waitMs;
            try {
                // Telegram has accepted these pages, but their durable receipts still
                // need the small YSQL pool. Do not make every page worker idle behind
                // that write: queue the scalar receipt, continue the bounded wave,
                // and drain every receipt before the chapter can advance. A crash in
                // this interval leaves its pre-Telegram intent durable and recovery
                // treats the result as UNKNOWN rather than sending the page again.
                const deferredReceipts = [];
                const drainDeferredReceipts = async () => {
                    const settled = await Promise.allSettled(deferredReceipts);
                    const rejected = settled.find((result) => result.status === 'rejected');
                    if (rejected)
                        throw rejected.reason;
                };
                let urls;
                try {
                    const sourceFetchStartedAt = performance.now();
                    urls = await withTimeout(adapter.fetchChapterPages(route.sourceChapterId, Number(task.payload.displayNumber)), this.chapterManifestTimeoutMs, 'CHAPTER_MANIFEST_TIMEOUT');
                    sourceFetchMs = performance.now() - sourceFetchStartedAt;
                    if (!urls.length || urls.length > 1000)
                        throw new Error('INVALID_PAGE_MANIFEST');
                }
                catch (error) {
                    throw sourceFailure(error);
                }
                const pageManifestHash = calculateSha256(Buffer.from(JSON.stringify(urls)));
                const previousManifestHash = typeof task.checkpoint.pageManifestHash === 'string' ? task.checkpoint.pageManifestHash : null;
                if (hasPersistedUpload && previousManifestHash && previousManifestHash !== pageManifestHash) {
                    throw new Error('SOURCE_PAGE_MANIFEST_CHANGED_AFTER_PARTIAL_STORAGE');
                }
                // Persist the chosen source and manifest before storing page bytes.
                // From this point a partial chapter can only resume from the exact
                // same route; otherwise a valid alternative could create a mixed
                // edition without any detectable corruption.
                const dbPreparationStartedAt = performance.now();
                const checkpointPatch = {
                    selectedSource: route.source,
                    sourceChapterId: route.sourceChapterId,
                    pageManifestHash,
                    plannedPageCount: urls.length,
                };
                const canCoalescePrepare = !legacy
                    && this.queue.checkpoint === LiteRepository.prototype.checkpoint
                    && !('mock' in this.queue.checkpoint);
                let chapterId;
                if (canCoalescePrepare) {
                    chapterId = await this.prepareFreshChapterImport(task, workerId, checkpointPatch);
                }
                else {
                    if (!await this.queue.checkpoint(task, workerId, checkpointPatch)) {
                        throw new Error('LEASE_LOST_BEFORE_MEDIA_STORAGE');
                    }
                    chapterId = await this.createOrLoadChapter(task, legacy);
                }
                dbPreparationMs = performance.now() - dbPreparationStartedAt;
                let mediaIds;
                const mediaPipelineStartedAt = performance.now();
                try {
                    mediaIds = await mapBounded(urls, this.getPageConcurrencyForSource(route.source), async (url, index) => {
                        const position = index + 1;
                        const receipt = uploadReceipts.get(position);
                        if (receipt?.state === 'PERSISTED' && receipt.storageReady === true && Number(receipt.bytes) > 0 &&
                            String(receipt.mime || '').startsWith('image/') && receipt.mediaSha256 === receipt.sha256)
                            return receipt.mediaId;
                        if (receipt?.state === 'UNKNOWN')
                            throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
                        const host = hostFor(url);
                        // A count permit owns only the active HTTP body/validation phase.
                        // Reserve bounded room before taking the global request slot. A
                        // provider can omit Content-Length (MangaFlix does in production),
                        // and waiting for bytes after its headers had arrived previously
                        // stranded global request permits on non-reading responses. The
                        // one-MiB initial reservation is upgraded by readImageBody before
                        // an oversized chunk is accepted, with the same 20-MiB terminal
                        // headroom as the former header-first admission.
                        // Do not spend the origin transfer timeout while this page is
                        // waiting for our own rate, byte, body, or global-download gates.
                        // Under a full media wave that made a healthy source look slow and
                        // incorrectly degraded it after the shared queue delay.
                        const admissionController = new AbortController();
                        const admissionTimer = setTimeout(() => {
                            admissionController.abort(new PageAdmissionTimeoutError());
                        }, this.pageAdmissionTimeoutMs);
                        let transferStarted = false;
                        let pageBufferHeld = false;
                        const releasePageBuffer = () => {
                            if (!pageBufferHeld)
                                return;
                            pageBufferHeld = false;
                            this.bufferedPages.release();
                        };
                        // A holder keeps the pre-admitted reservation visible to the outer
                        // cleanup through download, upload, and receipt persistence.
                        const byteReservation = { value: null };
                        try {
                            let downloaded;
                            try {
                                if (host) {
                                    const rateLimitStartedAt = performance.now();
                                    await this.sourceRateLimiter.acquire(host, admissionController.signal);
                                    rateLimitWaitMs += performance.now() - rateLimitStartedAt;
                                }
                                const initialReservationBytes = unknownPageInitialReservationBytes(this.pageBytes.capacityBytes);
                                // In production, 99.5% of pages are <= 3.68 MiB (p50: 0.29 MiB, avg: 0.49 MiB).
                                // Reserving a 1 MiB virtual headroom ensures that unknown-length responses
                                // can stream safely while preventing virtual over-reservation from blocking
                                // concurrent page admissions.
                                const completionHeadroomBytes = Math.min(1 * 1024 * 1024, MAX_IMAGE_BODY_BYTES - initialReservationBytes);
                                const reservation = await this.pageBytes.reserve(initialReservationBytes, admissionController.signal, { completionHeadroomBytes });
                                byteReservation.value = reservation;
                                // A task awaiting page bytes owns neither a global HTTP slot
                                // nor a page-body count permit. Once admitted, the count gate
                                // still bounds active response bodies independently of uploads.
                                await this.bufferedPages.acquire(admissionController.signal);
                                pageBufferHeld = true;
                                downloaded = await this.downloads.runExclusive(async () => {
                                    // The shared download permit is the boundary between local
                                    // admission and the real origin operation.  Clear the local
                                    // queue deadline before starting the source transfer timer.
                                    admissionController.signal.throwIfAborted();
                                    transferStarted = true;
                                    clearTimeout(admissionTimer);
                                    const transferSignal = AbortSignal.timeout(this.pageTransferTimeoutMs);
                                    const startedAt = performance.now();
                                    telemetryCollector.trackActiveDownload(1);
                                    try {
                                        const adapterHeaders = await adapter.getImageHeaders?.(url);
                                        const imageHeaders = {
                                            Referer: adapter.baseUrl ? `${adapter.baseUrl}/` : (host ? `https://${host}/` : ''),
                                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                                            Accept: 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
                                            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                                            ...(adapterHeaders || {}),
                                        };
                                        const response = await fetch(url, { headers: imageHeaders, signal: transferSignal });
                                        if (!response.ok) {
                                            if (response.status === 429 && host)
                                                this.sourceRateLimiter.handle429(host, response.headers.get('retry-after'));
                                            const error = sourceFailure(new Error(`PAGE_HTTP_${response.status}`));
                                            error.retryAfter = Number(response.headers.get('retry-after') || 0);
                                            throw error;
                                        }
                                        const bytes = await readImageBody(response, { signal: transferSignal, reservation });
                                        // MangaFlix omits Content-Length.  Its stream must retain
                                        // the 20 MiB terminal reservation while chunks are being
                                        // assembled, but most completed pages are far smaller.
                                        // Once the exact validated body exists, retain only its
                                        // real bytes through Telegram and durable receipt storage.
                                        // `completeBody` also returns this response's terminal
                                        // headroom, so the next lengthless response keeps its own
                                        // bounded completion guarantee.
                                        reservation.completeBody(bytes.byteLength);
                                        const image = inspectImage(bytes);
                                        if (host)
                                            this.sourceRateLimiter.recordSuccess(host);
                                        telemetryCollector.recordImageDownload(route.source, performance.now() - startedAt, bytes.length);
                                        return { bytes, image };
                                    }
                                    finally {
                                        pageDownloadDemandMs += performance.now() - startedAt;
                                        telemetryCollector.trackActiveDownload(-1);
                                    }
                                }, admissionController.signal);
                            }
                            catch (error) {
                                if (!transferStarted && admissionController.signal.aborted) {
                                    const reason = admissionController.signal.reason;
                                    throw reason instanceof PageAdmissionTimeoutError ? reason : new PageAdmissionTimeoutError();
                                }
                                throw sourceFailure(error);
                            }
                            finally {
                                clearTimeout(admissionTimer);
                            }
                            let { bytes, image } = downloaded;
                            totalBytes += bytes.byteLength;
                            const sha256 = calculateSha256(bytes);
                            // The page body has finished its only count-bound phase: HTTP
                            // read and image validation.  It remains fully byte-reserved
                            // until its receipt is durable, so a slow pre-upload intent
                            // cannot raise retained media beyond PageByteBudget.  Releasing
                            // the count permit here prevents a contended YSQL reservation
                            // batch from idling unrelated page downloads behind a body that
                            // is no longer being read.
                            releasePageBuffer();
                            const reserved = await this.reservationBatcher.reserve({ taskId: task.id, pagePosition: position, sha256 });
                            if (reserved.state === 'PERSISTED')
                                return reserved.mediaId;
                            if (reserved.state === 'UNKNOWN')
                                throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
                            try {
                                const uploadStartedAt = performance.now();
                                telemetryCollector.trackActiveTelegramUpload(1);
                                let providerKey;
                                try {
                                    providerKey = await this.storage.upload(bytes, image.mime, reserved.mediaId, chapterId);
                                    telemetryCollector.recordTelegramUpload(performance.now() - uploadStartedAt, bytes.length);
                                }
                                finally {
                                    telegramUploadDemandMs += performance.now() - uploadStartedAt;
                                    telemetryCollector.trackActiveTelegramUpload(-1);
                                }
                                // Once the image payload has been durably accepted by Telegram,
                                // the raw bytes in memory are no longer needed. Release the byte
                                // reservation immediately so other awaiting pages can begin downloading
                                // without waiting for the database receipt batch to flush.
                                byteReservation.value?.release();
                                byteReservation.value = null;
                                const pageByteLength = bytes.length;
                                const pageMime = image.mime;
                                const pageWidth = image.width;
                                const pageHeight = image.height;
                                // Eagerly release memory references so V8 GC can collect the raw image buffer immediately
                                bytes = null;
                                downloaded.bytes = null;
                                image.data = null;
                                const receipt = this.receiptBatcher.persist({
                                    taskId: task.id, pagePosition: position, mediaId: reserved.mediaId, sha256,
                                    bytes: pageByteLength, mime: pageMime, width: pageWidth, height: pageHeight, providerKey,
                                    botReference: this.storage.getLastBotReference?.(reserved.mediaId) || null,
                                    shardId: this.storage.getLastShardId?.(reserved.mediaId) || null,
                                    channelId: this.storage.getLastChannelId?.(reserved.mediaId) || null, createdBy: this.importerUserId,
                                    messageId: this.storage.getLastMessageId?.(reserved.mediaId) || null,
                                }).catch(async (error) => {
                                    await this.queue.checkpoint(task, workerId, {
                                        lastUploadFailure: {
                                            position, phase: 'RECEIPT_PERSISTENCE', class: uploadFailureClass(error), at: new Date().toISOString(),
                                        },
                                    });
                                    await this.queue.markUploadUnknown(task.id, position);
                                    throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
                                });
                                // A rejected promise can happen while sibling page work is
                                // still running. Attach a no-op handler now; the durable
                                // drain below remains the authority that propagates it.
                                void receipt.catch(() => { });
                                deferredReceipts.push(receipt);
                                return reserved.mediaId;
                            }
                            catch (error) {
                                // A confirmed Telegram success followed by a DB failure is
                                // ambiguous. Network failures without an explicit Telegram HTTP
                                // rejection are ambiguous too: a response can be lost after the
                                // server accepted the upload. Only known HTTP rejections (notably
                                // 429) may retry with the reserved media ID.
                                if (!error?.statusCode) {
                                    // UNKNOWN is intentionally terminal for this page: retain a
                                    // redacted, durable stage/class so production can distinguish
                                    // Telegram transport from post-upload receipt persistence
                                    // without logging provider responses or credentials.
                                    await this.queue.checkpoint(task, workerId, {
                                        lastUploadFailure: {
                                            position,
                                            phase: 'TELEGRAM_TRANSPORT',
                                            class: uploadFailureClass(error),
                                            at: new Date().toISOString(),
                                        },
                                    });
                                    await this.queue.markUploadUnknown(task.id, position);
                                    throw new Error(`UPLOAD_OUTCOME_UNKNOWN:${position}`);
                                }
                                await this.queue.markUploadRetryable(task.id, position);
                                throw error;
                            }
                        }
                        finally {
                            byteReservation.value?.release();
                            releasePageBuffer();
                        }
                    });
                }
                catch (error) {
                    // Do not let source fallback inspect an incomplete durable receipt
                    // wave. If a receipt failed, its UNKNOWN fence is more important
                    // than the source error that happened alongside it.
                    try {
                        await drainDeferredReceipts();
                    }
                    catch (receiptError) {
                        throw receiptError;
                    }
                    throw error;
                }
                await drainDeferredReceipts();
                // A media wave is not complete until every Telegram receipt is
                // durably recorded; include that bounded drain in its wall time.
                const mediaPipelineWallMs = performance.now() - mediaPipelineStartedAt;
                const dbPublishStartedAt = performance.now();
                const completionPayload = {
                    chapterId,
                    mediaIds,
                    pageCount: urls.length,
                    selectedSource: route.source,
                    mediaStoredAt: new Date().toISOString(),
                };
                const canInlinePublish = options?.inlinePublish === true
                    && typeof this.queue.completeImportAndPublish === 'function'
                    && this.queue.completeImportAndEnqueuePublish === LiteRepository.prototype.completeImportAndEnqueuePublish
                    && !('mock' in this.queue.completeImportAndEnqueuePublish);
                const completed = canInlinePublish
                    ? await this.queue.completeImportAndPublish(task, workerId, completionPayload, task.priority === 'P0' || task.priority === 'STAFF')
                    : await this.queue.completeImportAndEnqueuePublish(task, workerId, completionPayload);
                if (!completed)
                    throw new Error('LEASE_LOST_AFTER_MEDIA_STORAGE');
                const dbPublishMs = performance.now() - dbPublishStartedAt;
                const totalDurationMs = performance.now() - taskStartedAt;
                // This stays entirely in the existing bounded in-memory telemetry
                // collector. It writes neither page data nor a new operational row,
                // but makes an importer-only chapter wave explainable by stage.
                telemetryCollector.recordChapterMetric({
                    jobId: task.id,
                    source: route.source,
                    chapterNumber: Number.isFinite(Number(task.payload.displayNumber)) ? Number(task.payload.displayNumber) : 0,
                    pageCount: urls.length,
                    totalBytes,
                    totalDurationMs: Math.round(totalDurationMs),
                    totalSlotOccupancyMs: Math.round(totalDurationMs),
                    claim_acquire_ms: 0,
                    mutex_wait_ms: 0,
                    claim_db_ms: 0,
                    metadata_load_ms: 0,
                    source_fetch_ms: Math.round(sourceFetchMs),
                    page_resolution_ms: Math.round(routeLookupMs),
                    download_ms: Math.round(pageDownloadDemandMs),
                    encode_ms: 0,
                    telegram_upload_ms: Math.round(telegramUploadDemandMs),
                    media_pipeline_wall_ms: Math.round(mediaPipelineWallMs),
                    db_wait_ms: 0,
                    db_publish_ms: Math.round(dbPreparationMs + dbPublishMs),
                    rate_limit_wait_ms: Math.round(rateLimitWaitMs),
                    semaphore_wait_ms: Math.round(sourcePermitWaitMs),
                    other_wait_ms: Math.max(0, Math.round(totalDurationMs - routeLookupMs - sourceFetchMs - mediaPipelineWallMs - dbPreparationMs - dbPublishMs)),
                    timestamp: new Date().toISOString(),
                });
                return;
            }
            catch (error) {
                lastError = error;
                if (isSourceFailure(error)) {
                    await this.queue.recordSourceFailure(route.source, error);
                    // Never blend a partially persisted chapter with an alternative
                    // source. The task will retry the pinned route after its cooldown.
                    if ([...(await this.queue.uploadReceipts(task.id)).values()].some((receipt) => receipt.state === 'PERSISTED'))
                        break;
                    continue;
                }
                break;
            }
            finally {
                sourceSlot.release();
            }
        }
        const message = String(lastError?.message || 'IMPORT_FAILED');
        const permanent = /UPLOAD_OUTCOME_UNKNOWN|CHAPTER_DISPLAY_NUMBER|CHAPTER_NUMBER_COLLISION|SOURCE_PAGE_MANIFEST_CHANGED_AFTER_PARTIAL_STORAGE|LEASE_LOST_BEFORE_MEDIA_STORAGE|SOURCE_CHAPTER_OWNERSHIP_UNVERIFIED/.test(message);
        const retryAfter = Number(lastError?.retryAfter || lastError?.parameters?.retry_after || 0);
        await this.queue.finish(task, workerId, {
            state: permanent || task.attempts >= task.maxAttempts ? 'BLOCKED' : 'RETRY_WAIT', reason: message.slice(0, 500),
            nextRunAt: permanent ? undefined : new Date(Date.now() + (retryAfter > 0
                ? (retryAfter + 2) * 1_000
                : Math.min(30 * 60_000, 30_000 * Math.max(1, task.attempts)))),
        });
    }
    async publish(task, workerId) {
        // Normal imports materialize this payload atomically with their durable
        // completion. It is safe only when every value is present and shaped as a
        // scalar reference: LiteRepository.publish still fences the task and
        // validates every media row before anything becomes Reader-visible.
        // Recovery tasks created by older deployments deliberately retain the
        // checkpoint read below, rather than assuming their payload is complete.
        let workId = task.workId;
        let chapterId = typeof task.payload.chapterId === 'string' ? task.payload.chapterId : null;
        let mediaIds = Array.isArray(task.payload.mediaIds) && task.payload.mediaIds.every((id) => typeof id === 'string')
            ? task.payload.mediaIds : null;
        if (!workId || !chapterId || !mediaIds) {
            const importTaskId = String(task.payload.importTaskId || '');
            const imported = await this.db.query('SELECT work_id,checkpoint FROM importer_lite_tasks WHERE id=$1::uuid', [importTaskId]);
            const checkpoint = imported.rows[0]?.checkpoint || {};
            workId = imported.rows[0]?.work_id || null;
            chapterId = typeof checkpoint.chapterId === 'string' ? checkpoint.chapterId : null;
            mediaIds = Array.isArray(checkpoint.mediaIds) && checkpoint.mediaIds.every((id) => typeof id === 'string')
                ? checkpoint.mediaIds : null;
        }
        if (!workId || !chapterId || !mediaIds) {
            await this.queue.finish(task, workerId, { state: 'BLOCKED', reason: 'IMPORT_CHECKPOINT_INCOMPLETE' });
            return;
        }
        try {
            await this.queue.publish({ task, workerId, chapterId, workId,
                mediaIds, fresh: task.priority === 'P0' || task.priority === 'STAFF' });
        }
        catch (error) {
            const presentation = String(error?.message || '').includes('WORK_PRESENTATION_INCOMPLETE');
            await this.queue.finish(task, workerId, { state: presentation ? 'BLOCKED' : 'RETRY_WAIT',
                reason: String(error?.message || 'PUBLISH_FAILED'), nextRunAt: presentation ? undefined : new Date(Date.now() + 60_000) });
        }
    }
}
