import { type SourceHttpClientOptions } from './http-client.js';
import type { AliasOnlySourceAdapter, AliasSourceCandidate, AliasSourceDetailOutcome, AliasSourceHealthSnapshot, AliasSourceSearchOutcome } from './types.js';
export declare class MangaDexAliasSource implements AliasOnlySourceAdapter {
    readonly id: "mangadex";
    readonly name = "MangaDex";
    readonly baseUrl = "https://api.mangadex.org";
    readonly category: "ALIAS_ONLY";
    private readonly http;
    constructor(httpOptions?: Partial<SourceHttpClientOptions>);
    getHealthSnapshot(): AliasSourceHealthSnapshot;
    searchWorks(query: string, limit?: number): Promise<AliasSourceSearchOutcome>;
    fetchWorkAliases(sourceWorkId: string): Promise<AliasSourceDetailOutcome>;
    fetchWorksByIds(uuids: string[]): Promise<Map<string, AliasSourceCandidate>>;
}
