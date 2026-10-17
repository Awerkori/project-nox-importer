import type pg from 'pg';
/**
 * One-time, idempotent production reconciliation for the 2026-10 source
 * audit.  Registry membership alone is intentionally not enough to schedule
 * a provider: Lite also reads importer_sources.  Keeping this narrow list
 * here prevents a validated provider from being present only in code while
 * avoiding a broad "enable every registered adapter" action on each boot.
 */
export declare const SOURCE_AUDIT_RELEASE = "2026-10-17";
type Queryable = Pick<pg.Pool, 'query'>;
export declare function reconcileAuditedSources(pool: Queryable): Promise<void>;
export {};
