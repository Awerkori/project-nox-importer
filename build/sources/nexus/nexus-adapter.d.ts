import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class NexusAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "nexus";
    readonly name = "Nexus Mangas";
    readonly baseUrl = "https://www.nexusmangas.com";
    readonly admissionProbe: {
        url: string;
        headers: {
            apikey: string;
            Authorization: string;
            Accept: string;
        };
        expectedType: "json";
    };
    private apiUrl;
    private functionsUrl;
    private logger;
    private catalogItems;
    private catalogByNormalizedTitle;
    private catalogBySlug;
    private catalogById;
    private catalogLoaded;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private ensureCatalogLoaded;
    private get headers();
    private request;
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    fetchChapterPages(sourceChapterId: string): Promise<string[]>;
    private searchCatalog;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
}
