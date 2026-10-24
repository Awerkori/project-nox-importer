import { type SourceHttpClientOptions } from './http-client.js';
import type { AliasOnlySourceAdapter, AliasSourceDetailOutcome, AliasSourceHealthSnapshot, AliasSourceSearchOutcome } from './types.js';
export declare function signMangaFirePath(canonicalPathWithDecodedQuery: string): string;
export declare function buildSignedMangaFireApiUrl(baseUrl: string, apiPath: string, params?: Array<[string, string]>): string;
export declare function extractMangaFireHid(urlOrHid: string): string;
export declare class MangaFireAliasSource implements AliasOnlySourceAdapter {
    readonly id: "mangafire";
    readonly name = "MangaFire";
    readonly baseUrl = "https://mangafire.to";
    readonly category: "ALIAS_ONLY";
    private readonly http;
    constructor(httpOptions?: Partial<SourceHttpClientOptions>);
    getHealthSnapshot(): AliasSourceHealthSnapshot;
    searchWorks(query: string, limit?: number): Promise<AliasSourceSearchOutcome>;
    fetchWorkAliases(sourceWorkId: string): Promise<AliasSourceDetailOutcome>;
}
