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

export function hasSuccessfulSourceRecovery(record: SourceHealthRecord): boolean {
  const details = record.blockedDetails;
  return details?.probe_success === true || typeof details?.recovered_at === 'string';
}

export function isSourceExecutionEligible(record: SourceHealthRecord, now = Date.now()): boolean {
  if (record.enabled === false || record.chapterIngestionEnabled === false) return false;

  if (record.status === 'ACTIVE') {
    // ACTIVE + blocked_reason is a legacy/stale state unless a successful
    // production probe explicitly marked the row recovered.
    return !record.blockedReason || hasSuccessfulSourceRecovery(record);
  }

  if (record.status === 'COOLDOWN' || record.status === 'DEGRADED' || record.status === 'PROBING') {
    return !record.cooldownUntil || record.cooldownUntil <= now;
  }

  return false;
}

export function shouldProbePersistedSource(record: SourceHealthRecord): boolean {
  if (record.status === 'ACTIVE') {
    return Boolean(record.blockedReason) && !hasSuccessfulSourceRecovery(record);
  }
  return record.status === 'COOLDOWN' || record.status === 'DEGRADED' || record.status === 'PROBING';
}

/** SQL fragment for queries that use importer_sources as `s`. */
export const SOURCE_EXECUTION_ELIGIBILITY_SQL = `(
  (
    s.status = 'ACTIVE'
    AND (
      s.blocked_reason IS NULL
      OR s.blocked_details->>'probe_success' = 'true'
      OR s.blocked_details->>'recovered_at' IS NOT NULL
    )
  )
  OR (
    s.status IN ('COOLDOWN', 'PROBING', 'DEGRADED')
    AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())
  )
)`;
