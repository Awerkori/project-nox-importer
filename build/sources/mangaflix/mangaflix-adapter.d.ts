import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class MangaFlixAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "mangaflix";
    readonly name = "MangaFlix";
    readonly baseUrl = "https://mangaflix.net";
    readonly probeUrl = "https://api.mangaflix.net/v1/genres/6511eb5eae08773cd4189ec5/mangas/?offset=0&limit=1";
    private apiUrl;
    private logger;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private get headers();
    getImageHeaders(_url: string): Record<string, string>;
    private request;
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]>;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
}
