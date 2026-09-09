import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class MangoToonsAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "mangotoons";
    readonly name = "Mango Toons";
    readonly baseUrl = "https://api.mangotoons.com";
    private apiUrl;
    private cdnUrl;
    private logger;
    private encKey;
    private salt;
    private token;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    login(): Promise<boolean>;
    private requestApi;
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    fetchChapterPages(sourceChapterId: string, chapterNumber?: number): Promise<string[]>;
    private resolveCoverUrl;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
}
