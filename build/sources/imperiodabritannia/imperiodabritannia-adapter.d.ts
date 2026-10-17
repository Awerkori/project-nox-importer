import { SourceAdapter, SourceChapterSummary, SourceWorkDetails, SourceWorkSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
/**
 * Current implementation of the public MangoTheme API used by Sagrado
 * Império da Britannia. The token is an optional public cache hint exposed by
 * its own `env` asset; the adapter continues to work when that hint rotates.
 */
export declare class ImperioDaBritanniaAdapter implements SourceAdapter {
    private readonly rateLimiter;
    private readonly transport;
    readonly id = "imperiodabritannia";
    readonly name = "Sagrado Imp\u00E9rio da Britannia";
    readonly baseUrl = "https://imperiodabritannia.net";
    readonly admissionProbe: {
        url: string;
        headers: {
            Accept: string;
            'X-Brit-Cache': string;
            'X-Noencryptionbritta': string;
        };
        expectedType: "json";
    };
    private apiToken;
    private tokenFetchedAt;
    private readonly logger;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private refreshApiToken;
    private request;
    private toSummary;
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
    private work;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    isChapterOwnedByWork(sourceWorkId: string, sourceChapterId: string): boolean;
    fetchChapterPages(sourceChapterId: string): Promise<string[]>;
    getImageHeaders(): Record<string, string>;
}
