import fs from 'node:fs/promises';
import path from 'node:path';
import {withPostgresClient} from './postgres-probe.mjs';
import {atomicWriteJson,readJson,sleep} from './monitor-utils.mjs';
import {incidents,transitions} from './sentinel-policy.mjs';
const dir=process.env.NOX_SENTINEL_DIR || path.join(process.env.HOME,'.local/state/project-nox-sentinel');
await fs.mkdir(dir,{recursive:true,mode:0o700});
for (;;) {
  const sample={at:new Date().toISOString()};
  try { sample.heartbeat=await withPostgresClient(async c=>{const v=(await c.query("SELECT value FROM settings WHERE key='importer_heartbeat'")).rows[0]?.value;return typeof v==='string'?JSON.parse(v):v;}); }
  catch(e){sample.error=String(e.message).slice(0,240);}
  const old=await readJson(path.join(dir,'state.json'),{});
  const h=sample.heartbeat;
  const unknown=[];
  if (sample.error || !h) unknown.push('p0_overdue','memory_high','sql_waiters');
  else {
    if (!h.nextP0MonitorAt) unknown.push('p0_overdue');
    if (!Number.isFinite(Number(h.memoryPressure?.rssMiB))) unknown.push('memory_high');
    if (!Number.isFinite(Number(h.ysql?.poolWaiting))) unknown.push('sql_waiters');
  }
  const result=transitions(old.incidents || {},incidents(sample),Date.now(),3600000,unknown);
  for(const event of result.events) {await fs.appendFile(path.join(dir,'incidents.jsonl'),JSON.stringify(event)+'\n',{mode:0o600});console.error(JSON.stringify(event));}
  await atomicWriteJson(path.join(dir,'state.json'),{updatedAt:sample.at,heartbeatAt:h?.observedAt ?? null,unknown,incidents:result.state});
  await sleep(120000);
}
