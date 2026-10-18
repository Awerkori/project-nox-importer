import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
export declare class TaimuMangasAdapter implements SourceAdapter {
    private rateLimiter;
    private transport;
    readonly id = "taimumangas";
    readonly name = "TaimuMangas";
    readonly baseUrl = "https://beta.taimumangas.com";
    private apiUrl;
    private logger;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private get headers();
    /** The importer uses this public reader API, not the Cloudflare-protected
     * marketing homepage. Keep the admission gate on the same sustainable route
     * so a homepage challenge cannot incorrectly park an otherwise usable API. */
    get admissionProbe(): NonNullable<SourceAdapter['admissionProbe']>;
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
    getImageHeaders(url: string): Record<string, string>;
}
