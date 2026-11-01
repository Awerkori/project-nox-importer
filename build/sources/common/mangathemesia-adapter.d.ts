import { SourceAdapter, SourceWorkSummary, SourceWorkDetails, SourceChapterSummary, SourceWorkSnapshot } from '../types.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { Logger } from '../../core/logger.js';
export interface MangaThemesiaOptions {
    id: string;
    name: string;
    baseUrl: string;
    mangaSubString?: string;
    rateLimitRps?: number;
}
export declare class MangaThemesiaAdapter implements SourceAdapter {
    protected rateLimiter: HostRateLimiter;
    protected transport: typeof fetch;
    readonly id: string;
    readonly name: string;
    readonly baseUrl: string;
    readonly mangaSubString: string;
    /** See MadaraAdapter.probeUrl: use the actual public catalog, not a
     * separately protected landing page. */
    get probeUrl(): string;
    get admissionProbe(): NonNullable<SourceAdapter['admissionProbe']>;
    protected logger: Logger;
    constructor(options: MangaThemesiaOptions, rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    protected get headers(): Record<string, string>;
    protected fetchHtml(url: string, options?: RequestInit): Promise<string>;
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    fetchWorkSnapshot(sourceWorkId: string, _signal?: AbortSignal): Promise<SourceWorkSnapshot>;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]>;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
    getImageHeaders(_url?: string): Record<string, string>;
}
