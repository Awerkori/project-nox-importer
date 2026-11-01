import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary, SourceWorkSnapshot } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class ManhastroAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "manhastro";
    readonly name = "Manhastro";
    readonly baseUrl = "https://manhastro.net";
    readonly probeUrl = "https://api.manhastro.net/lancamentos?p=1";
    private apiUrl;
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
    fetchWorkSnapshot(sourceWorkId: string, _signal?: AbortSignal): Promise<SourceWorkSnapshot>;
    fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]>;
    private searchCatalog;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
}
