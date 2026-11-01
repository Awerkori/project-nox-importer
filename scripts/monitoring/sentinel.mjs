import fs from 'node:fs/promises';
import path from 'node:path';
import {withPostgresClient} from './postgres-probe.mjs';
import {atomicWriteJson,readJson,sleep} from './monitor-utils.mjs';
import {incidents,transitions,trackCatalog} from './sentinel-policy.mjs';
const dir=process.env.NOX_SENTINEL_DIR || path.join(process.env.HOME,'.local/state/project-nox-sentinel');
await fs.mkdir(dir,{recursive:true,mode:0o700});
for (;;) {
  const sample={at:new Date().toISOString()};
  const old=await readJson(path.join(dir,'state.json'),{});
  sample.p0=old.p0 || null;
  sample.catalog=old.catalog || null;
  sample.receipts=old.receipts || null;
  try { sample.heartbeat=await withPostgresClient(async c=>{
    const v=(await c.query("SELECT value FROM settings WHERE key='importer_heartbeat'")).rows[0]?.value;
    if (!sample.p0 || Date.now()-Date.parse(sample.p0.at)>600000) {
      try { const r=await c.query("SELECT next_run_at FROM importer_lite_tasks WHERE state='READY' AND task_type='CATALOG' AND monitoring=true ORDER BY next_run_at ASC LIMIT 1"); sample.p0={at:new Date().toISOString(),next:r.rows[0]?.next_run_at ?? null}; }
      catch { sample.p0=null; }
    }
    if (!sample.catalog || Date.now()-Date.parse(sample.catalog.at)>600000) {
      try { const r=await c.query("SELECT id,lease_owner FROM importer_lite_tasks WHERE state='RUNNING' AND task_type='CATALOG' AND monitoring=false ORDER BY lease_until LIMIT 21");
        sample.catalog=r.rows.length>20 ? null : {at:new Date().toISOString(),tasks:trackCatalog(old.catalog?.tasks || {},r.rows,Date.now())};
      } catch { sample.catalog=null; }
    }
    if (!sample.receipts || Date.now()-Date.parse(sample.receipts.at)>600000) {
      try { const r=await c.query("SELECT count(*)::int AS count,min(updated_at) AS oldest FROM importer_lite_tasks WHERE state='DONE' AND task_type='IMPORT_CHAPTER' AND updated_at >= now() - interval '2 hours' AND checkpoint ? 'mediaIds' AND NOT checkpoint ? 'publishedAt'");
        sample.receipts={at:new Date().toISOString(),...r.rows[0]};
      } catch { sample.receipts=null; }
    }
    return typeof v==='string'?JSON.parse(v):v;}); }
  catch(e){sample.error=String(e.message).slice(0,240);}
  const h=sample.heartbeat;
  const unknown=[];
  if (sample.error) unknown.push('heartbeat_stale');
  if (sample.error || !sample.receipts || Date.now()-Date.parse(sample.receipts.at)>900000) unknown.push('unpublished_receipts');
  if (sample.error || !sample.catalog || Date.now()-Date.parse(sample.catalog.at)>900000) unknown.push('catalog_prolonged');
  if (sample.error || !h) unknown.push('p0_overdue','memory_high','sql_waiters');
  else {
    if (!h.nextP0MonitorAt && (!sample.p0 || Date.now()-Date.parse(sample.p0.at)>900000)) unknown.push('p0_overdue');
    if (!Number.isFinite(Number(h.memoryPressure?.rssMiB))) unknown.push('memory_high');
    if (!Number.isFinite(Number(h.ysql?.poolWaiting))) unknown.push('sql_waiters');
  }
  const result=transitions(old.incidents || {},incidents(sample),Date.now(),3600000,unknown);
  for(const event of result.events) {await fs.appendFile(path.join(dir,'incidents.jsonl'),JSON.stringify(event)+'\n',{mode:0o600});console.error(JSON.stringify(event));}
  await atomicWriteJson(path.join(dir,'state.json'),{updatedAt:sample.at,heartbeatAt:h?.observedAt ?? null,p0:sample.p0,catalog:sample.catalog,receipts:sample.receipts,unknown,incidents:result.state});
  await sleep(120000);
}
