import { SourceAdapter } from '../sources/types.js';
import { CloudflareClassification } from './cloudflare-classifier.js';
export type AdmissionState = 'DISCOVERED' | 'LOCAL_VALIDATED' | 'PROD_PROBE' | 'PROD_WARMUP' | 'PROD_SOAK' | 'ACTIVE' | 'UPSTREAM_BLOCKED';
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
export declare class SourceAdmissionGate {
    private logger;
    /**
     * Executes a strict production probe directly from the current runtime environment.
     * Tests: Base URL -> Catalog -> Details -> Chapters -> Pages -> Image Download.
     * Differentiates ASN Datacenter block from Rate Limits.
     */
    executeProdProbe(adapter: SourceAdapter, transport?: typeof fetch): Promise<ProbeReport>;
}
