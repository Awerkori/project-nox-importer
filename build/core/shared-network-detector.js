import { Logger } from './logger.js';
export class SharedNetworkDetector {
    logger = new Logger('SharedNetworkDetector');
    recentEvents = [];
    windowMs;
    thresholdCount;
    activeIncident = false;
    incidentStartedAt = null;
    constructor(windowMs = 10 * 60_000, thresholdCount = 5) {
        this.windowMs = windowMs;
        this.thresholdCount = thresholdCount;
    }
    recordBlockEvent(event) {
        const now = Date.now();
        this.recentEvents.push({ ...event, timestamp: now });
        // Clean events older than sliding window
        const cutoff = now - this.windowMs;
        this.recentEvents = this.recentEvents.filter((e) => e.timestamp >= cutoff);
        // Count distinct sources experiencing block
        const uniqueSources = new Set(this.recentEvents.map((e) => e.sourceId));
        if (uniqueSources.size >= this.thresholdCount) {
            if (!this.activeIncident) {
                this.activeIncident = true;
                this.incidentStartedAt = now;
                this.logger.error(`[CRITICAL ALERT] POSSIBLE_SHARED_NETWORK_BLOCK detected across ${uniqueSources.size} distinct sources within ${Math.round(this.windowMs / 60_000)}m window on current datacenter network! Suppressing probe storms.`, {
                    affectedSources: Array.from(uniqueSources),
                    eventCount: this.recentEvents.length,
                });
            }
            return true;
        }
        if (this.activeIncident && uniqueSources.size < Math.ceil(this.thresholdCount / 2)) {
            this.activeIncident = false;
            this.incidentStartedAt = null;
            this.logger.info(`POSSIBLE_SHARED_NETWORK_BLOCK incident resolved. Distinct failing sources fell below threshold (${uniqueSources.size}).`);
        }
        return this.activeIncident;
    }
    isSharedBlockActive() {
        const now = Date.now();
        const cutoff = now - this.windowMs;
        this.recentEvents = this.recentEvents.filter((e) => e.timestamp >= cutoff);
        const uniqueSources = new Set(this.recentEvents.map((e) => e.sourceId));
        this.activeIncident = uniqueSources.size >= this.thresholdCount;
        return this.activeIncident;
    }
    getIncidentSummary() {
        return {
            active: this.activeIncident,
            startedAt: this.incidentStartedAt ? new Date(this.incidentStartedAt).toISOString() : null,
            affectedSources: Array.from(new Set(this.recentEvents.map((e) => e.sourceId))),
            eventCount: this.recentEvents.length,
        };
    }
}
