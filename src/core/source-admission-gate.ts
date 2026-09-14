import { SourceAdapter } from '../sources/types.js';
import { CloudflareClassifier, CloudflareClassification } from './cloudflare-classifier.js';
import { Logger } from './logger.js';

export type AdmissionState =
  | 'DISCOVERED'
  | 'LOCAL_VALIDATED'
  | 'PROD_PROBE'
  | 'PROD_WARMUP'
  | 'PROD_SOAK'
  | 'ACTIVE'
  | 'UPSTREAM_BLOCKED';

export interface AdmissionStageResult {
  stage: 'BASE_URL' | 'CATALOG' | 'DETAILS' | 'CHAPTERS' | 'PAGES' | 'IMAGE_DOWNLOAD';
  status: 'PASS' | 'FAIL';
  httpStatus?: number;
  durationMs: number;
  detail?: string;
  classification?: CloudflareClassification | null;
}

export interface ProbeReport {
  sourceId: string;
  timestamp: string;
  overallStatus: 'PASS' | 'FAIL';
  stages: AdmissionStageResult[];
  recommendedState: AdmissionState;
  classification: CloudflareClassification | null;
  cfRay: string | null;
  safeRate: number;
  isAsnBlock: boolean;
}

export class SourceAdmissionGate {
  private logger = new Logger('SourceAdmissionGate');

  /**
   * Executes a strict production probe directly from the current runtime environment.
   * Tests: Base URL -> Catalog -> Details -> Chapters -> Pages -> Image Download.
   * Differentiates ASN Datacenter block from Rate Limits.
   */
  public async executeProdProbe(adapter: SourceAdapter, transport: typeof fetch = fetch): Promise<ProbeReport> {
    const sourceId = adapter.id;
    const stages: AdmissionStageResult[] = [];
    const nowIso = new Date().toISOString();
    let cfRay: string | null = null;
    let primaryClassification: CloudflareClassification | null = null;
    let isAsnBlock = false;

    // Use probeUrl if adapter defines one (e.g. Kuro via CF Workers bridge)
    const targetUrl = adapter.probeUrl ?? adapter.baseUrl;
    this.logger.info(`Starting Production Admission Probe for source: ${sourceId} (${targetUrl})`);

    // Helper sleep
    const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

    // STAGE 1: BASE URL
    const t0 = Date.now();
    try {
      const res = await transport(targetUrl, {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
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
    } catch (err: any) {
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
    let discoveredWorks: any[] = [];
    try {
      if (typeof adapter.searchWorks === 'function') {
        try {
          discoveredWorks = await adapter.searchWorks('Solo');
        } catch {}
      }

      if ((!discoveredWorks || discoveredWorks.length === 0) && typeof adapter.fetchUpdatedWorks === 'function') {
        try {
          const catRes = await adapter.fetchUpdatedWorks(null, { mode: 'bootstrap' });
          discoveredWorks = catRes?.works || [];
        } catch {}
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
    } catch (err: any) {
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
    const testWork = discoveredWorks[0];
    const t2 = Date.now();
    try {
      if (typeof adapter.fetchWorkDetails === 'function') {
        const details = await adapter.fetchWorkDetails(testWork.sourceWorkId);
        stages.push({
          stage: 'DETAILS',
          status: 'PASS',
          durationMs: Date.now() - t2,
          detail: `Title: ${details?.title || testWork.title}`,
        });
      } else {
        stages.push({
          stage: 'DETAILS',
          status: 'PASS',
          durationMs: 0,
          detail: `Title: ${testWork.title}`,
        });
      }
    } catch (err: any) {
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
    let chapters: any[] = [];
    try {
      chapters = await adapter.fetchChapters(testWork.sourceWorkId);
      if (chapters.length === 0) {
        stages.push({
          stage: 'CHAPTERS',
          status: 'FAIL',
          durationMs: Date.now() - t3,
          detail: '0 chapters found for work',
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
        stage: 'CHAPTERS',
        status: 'PASS',
        durationMs: Date.now() - t3,
        detail: `Found ${chapters.length} chapters`,
      });
    } catch (err: any) {
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
    let pages: string[] = [];
    try {
      pages = await adapter.fetchChapterPages(testChapter.sourceChapterId, testChapter.number);
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
    } catch (err: any) {
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
    const testImageUrl = typeof pages[0] === 'string' ? pages[0] : (pages[0] as any)?.imageUrl;
    const t5 = Date.now();
    try {
      const imgHeaders = adapter.getImageHeaders ? await adapter.getImageHeaders(testImageUrl) : {};
      const imgRes = await transport(testImageUrl, {
        headers: {
          'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
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

      const isValidImage =
        insp.isValidImage ||
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
    } catch (err: any) {
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
