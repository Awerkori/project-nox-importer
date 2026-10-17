import { MangaThemesiaAdapter } from '../common/mangathemesia-adapter.js';
import { HostRateLimiter } from '../../core/rate-limiter.js';
import { SourceWorkSummary, SourceWorkDetails, SourceChapterSummary } from '../types.js';
export declare class PointZeroToonsAdapter extends MangaThemesiaAdapter {
    get probeUrl(): string;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    fetchChapterPages(sourceChapterId: string, _chapterNumber?: number): Promise<string[]>;
    getImageHeaders(_url: string): Record<string, string>;
}
