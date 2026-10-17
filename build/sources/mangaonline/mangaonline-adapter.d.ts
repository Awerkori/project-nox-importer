import { HostRateLimiter } from '../../core/rate-limiter.js';
import { SourceAdapter, SourceChapterSummary, SourceWorkDetails, SourceWorkSummary } from '../types.js';
/**
 * Manga Online moved from its old Madara installation to a bespoke WordPress
 * theme. Keep this separate from Madara: chapter URLs now live at
 * \`/capitulo/<work>-capitulo-<n>/\`, which is deliberately not compatible with
 * Madara's \`/manga/<work>/capitulo-<n>/\` ownership invariant.
 */
export declare class MangaOnlineAdapter implements SourceAdapter {
    private readonly rateLimiter;
    private readonly transport;
    readonly id = "mangaonline";
    readonly name = "Manga Online";
    readonly baseUrl = "https://mangaonline.love";
    readonly probeUrl = "https://mangaonline.love/?atualizacoes=1";
    get admissionProbe(): NonNullable<SourceAdapter['admissionProbe']>;
    private readonly logger;
    constructor(rateLimiter?: HostRateLimiter, transport?: typeof fetch);
    private get headers();
    private absolute;
    private fetchHtml;
    private parseCards;
    fetchUpdatedWorks(cursor?: string | null, options?: {
        mode?: 'bootstrap' | 'maintenance';
    }): Promise<{
        works: SourceWorkSummary[];
        nextCursor: string | null;
    }>;
    private workUrl;
    private parseDetails;
    private parseChapters;
    fetchWorkDetails(sourceWorkId: string): Promise<SourceWorkDetails>;
    fetchChapters(sourceWorkId: string): Promise<SourceChapterSummary[]>;
    isChapterOwnedByWork(sourceWorkId: string, sourceChapterId: string): boolean;
    fetchChapterPages(sourceChapterId: string): Promise<string[]>;
    searchWorks(query: string): Promise<SourceWorkSummary[]>;
    getImageHeaders(): Record<string, string>;
}
