/**
 * The persisted source state is intentionally fail-closed when a provider has
 * a recorded health failure but still carries the legacy ACTIVE status.  A
 * successful admission probe writes an explicit recovery marker; that marker
 * is the only way an ACTIVE row with historical blocked_reason remains
 * executable.
 */
export interface SourceHealthRecord {
    status?: string | null;
    enabled?: boolean | null;
    chapterIngestionEnabled?: boolean | null;
    catalogDiscoveryEnabled?: boolean | null;
    cooldownUntil?: number | null;
    blockedReason?: string | null;
    blockedDetails?: Record<string, unknown> | null;
}
export declare function hasSuccessfulSourceRecovery(record: SourceHealthRecord): boolean;
export declare function isSourceExecutionEligible(record: SourceHealthRecord, now?: number): boolean;
export declare function shouldProbePersistedSource(record: SourceHealthRecord): boolean;
/** SQL fragment for queries that use importer_sources as `s`. */
export declare const SOURCE_EXECUTION_ELIGIBILITY_SQL = "(\n  (\n    s.status = 'ACTIVE'\n    AND (\n      s.blocked_reason IS NULL\n      OR s.blocked_details->>'probe_success' = 'true'\n      OR s.blocked_details->>'recovered_at' IS NOT NULL\n    )\n  )\n  OR (\n    s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED')\n    AND (\n      s.blocked_reason IS NULL\n      OR s.blocked_details->>'probe_success' = 'true'\n      OR s.blocked_details->>'recovered_at' IS NOT NULL\n    )\n    AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())\n  )\n)";
