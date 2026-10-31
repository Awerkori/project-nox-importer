import fs from 'node:fs/promises';
import path from 'node:path';
import { withPostgresClient } from './postgres-probe.mjs';

// Explicit invocation only. Bounded, sequential, read-only production evidence.
const directory = process.argv[2];
if (!directory) throw new Error('Provide an evidence directory');
await fs.mkdir(directory, { recursive: true });
const result = { observedAt: new Date().toISOString(), measurements: {} };
const queries = {
  version: 'SELECT version(),now() AS database_time',
  heartbeat: "SELECT value FROM settings WHERE key='importer_heartbeat'",
  connections: `SELECT application_name,state,count(*)::int AS connections
    FROM pg_stat_activity WHERE datname=current_database() GROUP BY 1,2`,
  publications: `SELECT date_trunc('minute',c.published_at) AS minute,count(*)::int AS chapters,
    count(DISTINCT c.work_id)::int AS works
    FROM chapters c JOIN works w ON w.id=c.work_id
    WHERE c.published_at>=date_trunc('minute',now())-interval '3 hours'
      AND c.published_at<date_trunc('minute',now()) AND w.published=true
    GROUP BY 1 ORDER BY 1`,
  monitors: `SELECT state,count(*)::int AS tasks,count(DISTINCT work_id)::int AS works,
    count(*) FILTER(WHERE next_run_at<now())::int AS due,
    min(next_run_at) AS earliest_due, max(updated_at) AS latest_update
    FROM importer_lite_tasks WHERE monitoring=true AND task_type='CATALOG' GROUP BY state`,
  activeTasks: `SELECT task_type,state,priority,count(*)::int AS tasks,min(created_at) AS oldest_created,
    min(updated_at) AS oldest_updated,count(*) FILTER(WHERE lease_until<now())::int AS expired_leases
    FROM importer_lite_tasks WHERE state IN ('READY','RUNNING','RETRY_WAIT','BLOCKED') AND monitoring=false
    GROUP BY 1,2,3`,
};
for (const [name,sql] of Object.entries(queries)) {
  const start = performance.now();
  try {
    const rows = await withPostgresClient(async client => {
      await client.query('SET default_transaction_read_only=on');
      return (await client.query(sql)).rows;
    });
    result.measurements[name] = { rows, durationMs: Math.round(performance.now()-start) };
  } catch (error) {
    result.measurements[name] = { status: 'UNKNOWN', code: error.code ?? null, error: error.message };
  }
}
const file = path.join(directory, `baseline-${result.observedAt.replaceAll(':','-')}.json`);
await fs.writeFile(file, JSON.stringify(result,null,2)+'\n', {flag:'wx', mode:0o600});
console.log(file);
for (const [key,value] of Object.entries(result.measurements)) {
  if (key==='heartbeat') {
    let h=value.rows?.[0]?.value; if(typeof h==='string') h=JSON.parse(h);
    console.log(JSON.stringify({heartbeat:h?{observedAt:h.observedAt,fingerprint:h.runtimeFingerprint,memory:h.memoryPressure,pool:h.ysql,metadataPool:h.sourceMetadataYsql,p0:h.p0MonitorCoverage,site:h.site}:value}));
  } else console.log(JSON.stringify({[key]:value}));
}
