import type { AliasMatchStatus, AliasSourceCandidate, ExternalCrossLinks, NoxWorkAliasTarget } from './types.js';
export declare function isPersistableAliasScript(title: string): boolean;
export declare function isPortugueseAlias(title: string, explicitLang?: string): boolean;
/**
 * Protects adult/tankoubon manga from chapter-list story dumps mistakenly listed in `altTitles`.
 */
export declare function sanitizeCandidateAliases(nox: NoxWorkAliasTarget, cand: AliasSourceCandidate): Array<{
    title: string;
    language?: string;
}>;
export interface CandidateEvaluation {
    status: AliasMatchStatus;
    candidate: AliasSourceCandidate | null;
    confidence: number;
    matchMethod: string | null;
    matchReason: string | null;
    evidence: Record<string, unknown>;
}
/**
 * Evaluates all candidates returned for a work on a single ALIAS_ONLY source
 * and classifies into CONFIRMED_MATCH, AMBIGUOUS_MATCH, or NOT_FOUND.
 */
export declare function evaluateSourceCandidates(nox: NoxWorkAliasTarget, candidates: AliasSourceCandidate[], corroboratedContext?: {
    confirmedTitles?: string[];
    confirmedLinks?: ExternalCrossLinks;
}): CandidateEvaluation;
/**
 * Computes the exact merged `works.aliases` array and delta metrics without ever removing existing valid aliases.
 */
export declare function computeMergedWorkAliases(nox: NoxWorkAliasTarget, incomingLocalized: Array<{
    title: string;
    language?: string;
}>): {
    aliasesAfter: string[];
    addedAliases: string[];
    ptBrAliasesAdded: string[];
    aliasesChanged: boolean;
};
