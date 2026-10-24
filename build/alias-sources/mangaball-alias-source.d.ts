import { type SourceHttpClientOptions } from './http-client.js';
import type { AliasOnlySourceAdapter, AliasSourceDetailOutcome, AliasSourceHealthSnapshot, AliasSourceSearchOutcome } from './types.js';
export declare class MangaBallAliasSource implements AliasOnlySourceAdapter {
    readonly id: "mangaball";
    readonly name = "MangaBall";
    readonly baseUrl = "https://mangaball.com";
    readonly category: "ALIAS_ONLY";
    private readonly apiBase;
    private readonly fallbackApiBase;
    private readonly http;
    constructor(httpOptions?: Partial<SourceHttpClientOptions>);
    getHealthSnapshot(): AliasSourceHealthSnapshot;
    searchWorks(query: string, limit?: number): Promise<AliasSourceSearchOutcome>;
    fetchWorkAliases(sourceWorkId: string): Promise<AliasSourceDetailOutcome>;
}
