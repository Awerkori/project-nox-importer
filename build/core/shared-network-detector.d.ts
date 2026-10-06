import { CloudflareClassification } from './cloudflare-classifier.js';
export interface BlockEvent {
    sourceId: string;
    timestamp: number;
    classification: CloudflareClassification;
    url?: string;
    cfRay?: string | null;
}
export declare class SharedNetworkDetector {
    private logger;
    private recentEvents;
    private windowMs;
    private thresholdCount;
    private activeIncident;
    private incidentStartedAt;
    constructor(windowMs?: number, thresholdCount?: number);
    recordBlockEvent(event: Omit<BlockEvent, 'timestamp'>): boolean;
    isSharedBlockActive(): boolean;
    /**
     * A shared datacenter incident must not starve probes for unrelated
     * provider-specific failures. API_BLOCK and TURNSTILE are still probed
     * safely (the admission probe itself remains bounded and can keep them
     * blocked), while datacenter/ASN blocks continue to suppress probe storms.
     */
    isSharedBlockActiveFor(classification?: CloudflareClassification | null): boolean;
    getIncidentSummary(): {
        active: boolean;
        startedAt: string | null;
        affectedSources: string[];
        eventCount: number;
    };
}
