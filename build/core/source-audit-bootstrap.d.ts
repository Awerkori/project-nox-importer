import type pg from 'pg';
/**
 * One-time, idempotent production reconciliation for the 2026-10 source
 * audit.  Registry membership alone is intentionally not enough to schedule
 * a provider: Lite also reads importer_sources.  Keeping this narrow list
 * here prevents a validated provider from being present only in code while
 * avoiding a broad "enable every registered adapter" action on each boot.
 */
export declare const SOURCE_AUDIT_RELEASE = "2026-10-17";
export declare const KURO_RETIREMENT_RELEASE = "2026-10-18";
export declare const BLOCKED_SOURCE_RETIREMENT_RELEASE = "2026-10-18";
type Queryable = Pick<pg.Pool, 'query'>;
/** Permanently retired after a full production admission-gate audit. */
export declare const BLOCKED_SOURCE_RETIREMENTS: readonly [readonly ["acervohentai", "AUDIT_2026_10_JS_CHALLENGE"], readonly ["amuy", "AUDIT_2026_10_JS_CHALLENGE"], readonly ["arthurscan", "AUDIT_2026_10_JS_CHALLENGE"], readonly ["geasscomics", "AUDIT_2026_10_JS_CHALLENGE"], readonly ["inkapk", "AUDIT_2026_10_JS_CHALLENGE"], readonly ["mangaonline", "AUDIT_2026_10_JS_CHALLENGE"], readonly ["mangastop", "AUDIT_2026_10_JS_CHALLENGE"], readonly ["pointzerotoons", "AUDIT_2026_10_JS_CHALLENGE"], readonly ["vegitoons", "AUDIT_2026_10_API_BLOCK"], readonly ["yaoifanclub", "AUDIT_2026_10_JS_CHALLENGE"], readonly ["yuriverso", "AUDIT_2026_10_JS_CHALLENGE"]];
export declare function reconcileAuditedSources(pool: Queryable): Promise<void>;
export {};
