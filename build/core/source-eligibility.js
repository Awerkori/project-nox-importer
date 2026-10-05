export function hasSuccessfulSourceRecovery(record) {
    const details = record.blockedDetails;
    return details?.probe_success === true || typeof details?.recovered_at === 'string';
}
export function isSourceExecutionEligible(record, now = Date.now()) {
    if (record.enabled === false || record.chapterIngestionEnabled === false)
        return false;
    if (record.status === 'ACTIVE') {
        // ACTIVE + blocked_reason is a legacy/stale state unless a successful
        // production probe explicitly marked the row recovered.
        return !record.blockedReason || hasSuccessfulSourceRecovery(record);
    }
    if (record.status === 'COOLDOWN' || record.status === 'DEGRADED' || record.status === 'PROBING') {
        // Expiry only authorizes a fresh health probe.  A source with an
        // unresolved persisted block must not enter execution until that probe
        // records an explicit recovery marker.
        if (record.blockedReason && !hasSuccessfulSourceRecovery(record))
            return false;
        return !record.cooldownUntil || record.cooldownUntil <= now;
    }
    return false;
}
export function shouldProbePersistedSource(record) {
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
    AND (
      s.blocked_reason IS NULL
      OR s.blocked_details->>'probe_success' = 'true'
      OR s.blocked_details->>'recovered_at' IS NOT NULL
    )
    AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW())
  )
)`;
