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
    getIncidentSummary(): {
        active: boolean;
        startedAt: string | null;
        affectedSources: string[];
        eventCount: number;
    };
}
