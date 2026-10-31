export function incidents(sample, now = Date.now()) {
  const result = {};
  if (sample.error) return { database_unreachable: sample.error };
  if (!sample.heartbeat) return { heartbeat_stale: 'heartbeat missing' };
  const h = sample.heartbeat;
  const at = Date.parse(h?.observedAt || '');
  if (!Number.isFinite(at) || now - at > 5 * 60_000) result.heartbeat_stale = `heartbeat=${h?.observedAt || 'missing'}`;
  const rss = Number(h?.memoryPressure?.rssMiB);
  if (Number.isFinite(rss) && rss >= 550) result.memory_high = `rssMiB=${rss}; configured historical cap=624MiB`;
  const pool = Number(h?.ysql?.poolWaiting);
  if (Number.isFinite(pool) && pool >= 8) result.sql_waiters = `primaryPoolWaiters=${pool}`;
  const catalogFresh = Number.isFinite(Date.parse(sample.catalog?.at || '')) && now-Date.parse(sample.catalog.at)<=900000;
  const long = Object.entries(catalogFresh ? sample.catalog.tasks : {}).filter(([,task]) => now-Number(task.firstSeen) >= 30*60_000);
  if (long.length) result.catalog_prolonged = `${long.length} catalog task(s) held by the same owner for at least 30 minutes; inspect before recovery` ;
  const p0Fresh = Number.isFinite(Date.parse(sample.p0?.at || '')) && now-Date.parse(sample.p0.at)<=900000;
  const nextValue = (p0Fresh ? sample.p0.next : null) || h?.nextP0MonitorAt;
  const next = Date.parse(nextValue || '');
  if (Number.isFinite(next) && now-next > 60*60_000) result.p0_overdue = `oldest READY P0 observation=${nextValue}; may include cooled sources`;
  return result;
}
export function transitions(previous, current, now = Date.now(), cooldown = 60*60_000, unknown = []) {
  const state = {}; const events = [];
  for (const [key, evidence] of Object.entries(current)) {
    const old=previous[key]; const notify=!old || now-Number(old.lastNotified || 0)>=cooldown;
    state[key]={firstSeen:old?.firstSeen ?? now,lastNotified:notify?now:old.lastNotified,evidence};
    if (notify) events.push({type:old?'reminder':'opened',key,evidence,at:new Date(now).toISOString()});
  }
  for (const [key, old] of Object.entries(previous)) if (!(key in current)) {
    if (unknown.includes(key)) state[key]=old;
    else events.push({type:'recovered',key,evidence:old.evidence,at:new Date(now).toISOString()});
  }
  return {state,events};
}

// An unchanged lease owner for 30 minutes indicates prolonged work, not proof
// of a dead worker. Observations begin when this sentinel first sees the task.
export function trackCatalog(previous, rows, now) {
  const next = {};
  for (const row of rows) {
    const old = previous[row.id];
    next[row.id] = {owner:row.lease_owner, firstSeen:old?.owner === row.lease_owner ? old.firstSeen : now};
  }
  return next;
}
