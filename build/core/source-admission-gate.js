import { CloudflareClassifier } from './cloudflare-classifier.js';
import { Logger } from './logger.js';
export class SourceAdmissionGate {
    stageTimeoutMs;
    logger = new Logger('SourceAdmissionGate');
    /** Bound each provider stage so one hung adapter cannot freeze recovery for
     * every other source. Provider-level fetch timeouts remain in force. */
    constructor(stageTimeoutMs = 20_000) {
        this.stageTimeoutMs = stageTimeoutMs;
    }
    async withStageTimeout(stage, operation) {
        let timer;
        try {
            return await Promise.race([
                operation,
                new Promise((_, reject) => {
                    timer = setTimeout(() => reject(new Error(`Admission probe stage ${stage} timed out after ${this.stageTimeoutMs}ms`)), this.stageTimeoutMs);
                }),
            ]);
        }
        finally {
            if (timer)
                clearTimeout(timer);
        }
    }
    /**
     * Executes a strict production probe directly from the current runtime environment.
     * Tests: Base URL -> Catalog -> Details -> Chapters -> Pages -> Image Download.
     * Differentiates ASN Datacenter block from Rate Limits.
     */
    async executeProdProbe(adapter, transport = fetch) {
        const sourceId = adapter.id;
        const stages = [];
        const nowIso = new Date().toISOString();
        let cfRay = null;
        let primaryClassification = null;
        let isAsnBlock = false;
        // Use probeUrl if adapter defines one (e.g. Kuro via CF Workers bridge)
        const targetUrl = adapter.probeUrl ?? adapter.baseUrl;
        this.logger.info(`Starting Production Admission Probe for source: ${sourceId} (${targetUrl})`);
        // Helper sleep
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
        // STAGE 1: BASE URL
        const t0 = Date.now();
        try {
            const res = await transport(targetUrl, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                    Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                },
                signal: AbortSignal.timeout(10_000),
            });
            const bodyText = res.text ? await res.text().catch(() => '') : '';
            cfRay = res.headers?.get ? res.headers.get('cf-ray') : null;
            const insp = CloudflareClassifier.inspect(res.status, res.headers, bodyText, {
                url: targetUrl,
                expectedType: 'html',
                isIsolatedRequest: true,
            });
            if (insp.isBlocked || insp.isChallenge || res.status === 403) {
                stages.push({
                    stage: 'BASE_URL',
                    status: 'FAIL',
                    httpStatus: res.status,
                    durationMs: Date.now() - t0,
                    detail: insp.reason,
                    classification: insp.classification,
                });
                primaryClassification = insp.classification || 'CLOUDFLARE_DATACENTER_BLOCK';
                if (primaryClassification === 'CLOUDFLARE_DATACENTER_BLOCK' || primaryClassification === 'DATACENTER_ASN_BLOCK') {
                    isAsnBlock = true;
                }
                return {
                    sourceId,
                    timestamp: nowIso,
                    overallStatus: 'FAIL',
                    stages,
                    recommendedState: 'UPSTREAM_BLOCKED',
                    classification: primaryClassification,
                    cfRay,
                    safeRate: 0,
                    isAsnBlock,
                };
            }
            stages.push({
                stage: 'BASE_URL',
                status: 'PASS',
                httpStatus: res.status,
                durationMs: Date.now() - t0,
            });
        }
        catch (err) {
            stages.push({
                stage: 'BASE_URL',
                status: 'FAIL',
                durationMs: Date.now() - t0,
                detail: err?.message,
            });
            return {
                sourceId,
                timestamp: nowIso,
                overallStatus: 'FAIL',
                stages,
                recommendedState: 'UPSTREAM_BLOCKED',
                classification: 'CLOUDFLARE_DATACENTER_BLOCK',
                cfRay,
                safeRate: 0,
                isAsnBlock: false,
            };
        }
        await sleep(300);
        // STAGE 2: CATALOG / SEARCH DISCOVERY
        const t1 = Date.now();
        let discoveredWorks = [];
        try {
            const isPlaceholderWork = (work) => {
                const sourceWorkId = String(work?.sourceWorkId || '').trim().toLocaleLowerCase();
                if (!sourceWorkId)
                    return true;
                const normalized = sourceWorkId.replace(/^https?:\/\/[^/]+/i, '').replace(/^\/+|\/+$/g, '');
                const terminal = normalized.split('/').pop() || normalized;
                return new Set(['feed', 'page', 'order', 'manga', 'obra', 'projeto']).has(terminal);
            };
            if (typeof adapter.searchWorks === 'function') {
                try {
                    const searchWorks = await this.withStageTimeout('CATALOG_SEARCH', adapter.searchWorks('Solo'));
                    // Some WordPress/Madara search endpoints return a feed or pagination
                    // link as the only result. Treat that as an unusable catalog result
                    // and fall back to the adapter's bootstrap listing instead of
                    // probing a fake work and parking a healthy source as API_BLOCK.
                    discoveredWorks = (searchWorks || []).filter((work) => !isPlaceholderWork(work));
                }
                catch { }
            }
            if ((!discoveredWorks || discoveredWorks.length === 0) && typeof adapter.fetchUpdatedWorks === 'function') {
                try {
                    const catRes = await this.withStageTimeout('CATALOG_BOOTSTRAP', adapter.fetchUpdatedWorks(null, { mode: 'bootstrap' }));
                    discoveredWorks = catRes?.works || [];
                }
                catch { }
            }
            if (!discoveredWorks || discoveredWorks.length === 0) {
                stages.push({
                    stage: 'CATALOG',
                    status: 'FAIL',
                    durationMs: Date.now() - t1,
                    detail: 'Catalog/Search returned 0 works',
                });
                return {
                    sourceId,
                    timestamp: nowIso,
                    overallStatus: 'FAIL',
                    stages,
                    recommendedState: 'UPSTREAM_BLOCKED',
                    classification: 'API_BLOCK',
                    cfRay,
                    safeRate: 0,
                    isAsnBlock: false,
                };
            }
            stages.push({
                stage: 'CATALOG',
                status: 'PASS',
                durationMs: Date.now() - t1,
                detail: `Found ${discoveredWorks.length} works`,
            });
        }
        catch (err) {
            stages.push({
                stage: 'CATALOG',
                status: 'FAIL',
                durationMs: Date.now() - t1,
                detail: err?.message,
            });
            return {
                sourceId,
                timestamp: nowIso,
                overallStatus: 'FAIL',
                stages,
                recommendedState: 'UPSTREAM_BLOCKED',
                classification: 'API_BLOCK',
                cfRay,
                safeRate: 0,
                isAsnBlock: false,
            };
        }
        await sleep(300);
        // STAGE 3: WORK DETAILS (using real work from catalog)
        let testWork = discoveredWorks[0];
        const t2 = Date.now();
        try {
            if (typeof adapter.fetchWorkDetails === 'function') {
                const details = await this.withStageTimeout('DETAILS', adapter.fetchWorkDetails(testWork.sourceWorkId));
                stages.push({
                    stage: 'DETAILS',
                    status: 'PASS',
                    durationMs: Date.now() - t2,
                    detail: `Title: ${details?.title || testWork.title}`,
                });
            }
            else {
                stages.push({
                    stage: 'DETAILS',
                    status: 'PASS',
                    durationMs: 0,
                    detail: `Title: ${testWork.title}`,
                });
            }
        }
        catch (err) {
            stages.push({
                stage: 'DETAILS',
                status: 'FAIL',
                durationMs: Date.now() - t2,
                detail: err?.message,
            });
            return {
                sourceId,
                timestamp: nowIso,
                overallStatus: 'FAIL',
                stages,
                recommendedState: 'UPSTREAM_BLOCKED',
                classification: 'API_BLOCK',
                cfRay,
                safeRate: 0,
                isAsnBlock: false,
            };
        }
        await sleep(600);
        // STAGE 4: CHAPTERS LIST
        const t3 = Date.now();
        let chapters = [];
        try {
            chapters = await this.withStageTimeout('CHAPTERS', adapter.fetchChapters(testWork.sourceWorkId));
            if (chapters.length === 0) {
                // A catalog search can legitimately return a placeholder, one-shot,
                // or otherwise empty work before a usable work.  Treating that first
                // result as an upstream outage permanently blocked otherwise healthy
                // sources.  Try a small bounded number of additional catalog results
                // before classifying the source; this is still a read-only probe and
                // never bypasses an upstream challenge.
                let selected = null;
                for (const candidate of discoveredWorks.slice(1, 4)) {
                    try {
                        const candidateDetails = typeof adapter.fetchWorkDetails === 'function'
                            ? await this.withStageTimeout('DETAILS_ALTERNATE', adapter.fetchWorkDetails(candidate.sourceWorkId))
                            : candidate;
                        const candidateChapters = await this.withStageTimeout('CHAPTERS_ALTERNATE', adapter.fetchChapters(candidate.sourceWorkId));
                        if (candidateChapters.length > 0) {
                            selected = { work: candidate, details: candidateDetails, chapters: candidateChapters };
                            break;
                        }
                    }
                    catch {
                        // Continue to the next bounded candidate. A single malformed or
                        // empty catalog entry is not enough evidence to block the source.
                    }
                }
                if (selected) {
                    testWork = selected.work;
                    chapters = selected.chapters;
                    const detailsStage = stages.find((stage) => stage.stage === 'DETAILS');
                    if (detailsStage)
                        detailsStage.detail = `Title: ${selected.details?.title || selected.work.title}`;
                }
                else {
                    stages.push({
                        stage: 'CHAPTERS',
                        status: 'FAIL',
                        durationMs: Date.now() - t3,
                        detail: 'No chapters found in bounded sample of catalog works',
                    });
                    return {
                        sourceId,
                        timestamp: nowIso,
                        overallStatus: 'FAIL',
                        stages,
                        recommendedState: 'UPSTREAM_BLOCKED',
                        classification: 'API_BLOCK',
                        cfRay,
                        safeRate: 0,
                        isAsnBlock: false,
                    };
                }
            }
            stages.push({
                stage: 'CHAPTERS',
                status: 'PASS',
                durationMs: Date.now() - t3,
                detail: `Found ${chapters.length} chapters`,
            });
        }
        catch (err) {
            stages.push({
                stage: 'CHAPTERS',
                status: 'FAIL',
                durationMs: Date.now() - t3,
                detail: err?.message,
            });
            return {
                sourceId,
                timestamp: nowIso,
                overallStatus: 'FAIL',
                stages,
                recommendedState: 'UPSTREAM_BLOCKED',
                classification: 'API_BLOCK',
                cfRay,
                safeRate: 0,
                isAsnBlock: false,
            };
        }
        await sleep(600);
        // STAGE 5: CHAPTER PAGES
        const testChapter = chapters[0];
        const t4 = Date.now();
        let pages = [];
        try {
            pages = await this.withStageTimeout('PAGES', adapter.fetchChapterPages(testChapter.sourceChapterId, testChapter.number));
            if (pages.length === 0) {
                stages.push({
                    stage: 'PAGES',
                    status: 'FAIL',
                    durationMs: Date.now() - t4,
                    detail: '0 pages returned for chapter',
                });
                return {
                    sourceId,
                    timestamp: nowIso,
                    overallStatus: 'FAIL',
                    stages,
                    recommendedState: 'UPSTREAM_BLOCKED',
                    classification: 'API_BLOCK',
                    cfRay,
                    safeRate: 0,
                    isAsnBlock: false,
                };
            }
            stages.push({
                stage: 'PAGES',
                status: 'PASS',
                durationMs: Date.now() - t4,
                detail: `Found ${pages.length} pages`,
            });
        }
        catch (err) {
            stages.push({
                stage: 'PAGES',
                status: 'FAIL',
                durationMs: Date.now() - t4,
                detail: err?.message,
            });
            return {
                sourceId,
                timestamp: nowIso,
                overallStatus: 'FAIL',
                stages,
                recommendedState: 'UPSTREAM_BLOCKED',
                classification: 'API_BLOCK',
                cfRay,
                safeRate: 0,
                isAsnBlock: false,
            };
        }
        await sleep(600);
        // STAGE 6: BINARY IMAGE DOWNLOAD
        const testImageUrl = typeof pages[0] === 'string' ? pages[0] : pages[0]?.imageUrl;
        const t5 = Date.now();
        try {
            const imgHeaders = adapter.getImageHeaders
                ? await this.withStageTimeout('IMAGE_HEADERS', Promise.resolve(adapter.getImageHeaders(testImageUrl)))
                : {};
            const imgRes = await transport(testImageUrl, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                    Referer: `${adapter.baseUrl}/`,
                    ...imgHeaders,
                },
                signal: AbortSignal.timeout(12_000),
            });
            const buf = await imgRes.arrayBuffer();
            const bodyText = buf.byteLength < 4000 ? new TextDecoder().decode(buf) : '';
            const insp = CloudflareClassifier.inspect(imgRes.status, imgRes.headers, bodyText, {
                url: testImageUrl,
                expectedType: 'image',
                buffer: buf,
                isIsolatedRequest: true,
            });
            const isValidImage = insp.isValidImage ||
                (imgRes.status === 200 && buf.byteLength > 0 && !insp.isFakeContent && !insp.isChallenge && !insp.isBlocked);
            if (!isValidImage || insp.isBlocked || insp.isChallenge || buf.byteLength === 0) {
                stages.push({
                    stage: 'IMAGE_DOWNLOAD',
                    status: 'FAIL',
                    httpStatus: imgRes.status,
                    durationMs: Date.now() - t5,
                    detail: `Image download failed or fake content. Status: ${imgRes.status}, bytes: ${buf.byteLength}, reason: ${insp.reason}`,
                    classification: insp.classification || 'IMAGE_CDN_BLOCK',
                });
                return {
                    sourceId,
                    timestamp: nowIso,
                    overallStatus: 'FAIL',
                    stages,
                    recommendedState: 'UPSTREAM_BLOCKED',
                    classification: insp.classification || 'IMAGE_CDN_BLOCK',
                    cfRay: insp.cfRay,
                    safeRate: 0,
                    isAsnBlock: insp.classification === 'DATACENTER_ASN_BLOCK',
                };
            }
            stages.push({
                stage: 'IMAGE_DOWNLOAD',
                status: 'PASS',
                httpStatus: imgRes.status,
                durationMs: Date.now() - t5,
                detail: `Downloaded ${buf.byteLength} valid binary bytes (${imgRes.headers?.get ? imgRes.headers.get('content-type') : 'image/jpeg'})`,
            });
        }
        catch (err) {
            stages.push({
                stage: 'IMAGE_DOWNLOAD',
                status: 'FAIL',
                durationMs: Date.now() - t5,
                detail: err?.message,
                classification: 'IMAGE_CDN_BLOCK',
            });
            return {
                sourceId,
                timestamp: nowIso,
                overallStatus: 'FAIL',
                stages,
                recommendedState: 'UPSTREAM_BLOCKED',
                classification: 'IMAGE_CDN_BLOCK',
                cfRay,
                safeRate: 0,
                isAsnBlock: false,
            };
        }
        // ALL 6 STAGES PASSED!
        this.logger.info(`Source ${sourceId} PASSED all 6 stages of Production Admission Probe!`);
        return {
            sourceId,
            timestamp: nowIso,
            overallStatus: 'PASS',
            stages,
            recommendedState: 'PROD_WARMUP',
            classification: null,
            cfRay,
            safeRate: 2.0,
            isAsnBlock: false,
        };
    }
}
