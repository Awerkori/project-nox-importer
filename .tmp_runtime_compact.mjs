import 'dotenv/config';
import pg from 'pg';
const p = new pg.Pool({host:process.env.YUGABYTE_HOST,port:+(process.env.YUGABYTE_PORT||5433),user:process.env.YUGABYTE_USER,password:process.env.YUGABYTE_PASSWORD,database:process.env.YUGABYTE_DATABASE,ssl:{rejectUnauthorized:false}});
const one = async () => {
 const hb=(await p.query("SELECT value FROM settings WHERE key='importer_heartbeat'")).rows[0]?.value;
 const h=hb?JSON.parse(hb):{};
 const w=(await p.query("SELECT worker_id,MAX(created_at) last_seen FROM importer_telemetry WHERE created_at>NOW()-INTERVAL '20 minutes' GROUP BY worker_id ORDER BY last_seen DESC")).rows;
 const r=(await p.query("SELECT bucket_minute,visible_published,completed_jobs FROM importer_rate_buckets ORDER BY bucket_minute DESC LIMIT 5")).rows;
 const s=(await p.query("SELECT COUNT(*) FILTER (WHERE status='ACTIVE' AND blocked_reason IS NOT NULL AND blocked_details->>'probe_success' IS DISTINCT FROM 'true' AND blocked_details->>'recovered_at' IS NULL)::int unresolved, COUNT(*) FILTER (WHERE status='ACTIVE' AND blocked_reason IS NOT NULL AND (blocked_details->>'probe_success'='true' OR blocked_details->>'recovered_at' IS NOT NULL))::int recovered, COUNT(*) FILTER (WHERE status='ACTIVE' AND blocked_reason IS NULL)::int clean FROM importer_sources")).rows[0];
 const q=(await p.query("SELECT status,COUNT(*)::int count FROM importer_queue WHERE task_type='IMPORT_CHAPTER' GROUP BY status ORDER BY status")).rows;
 const m=(await p.query("SELECT sync_status,COUNT(*)::int count FROM importer_work_mappings GROUP BY sync_status ORDER BY sync_status")).rows;
 const wa=(await p.query("SELECT COUNT(*) FILTER (WHERE s.status='ACTIVE' AND s.blocked_reason IS NOT NULL AND s.blocked_details->>'probe_success' IS DISTINCT FROM 'true' AND s.blocked_details->>'recovered_at' IS NULL)::int unresolved_blocked, COUNT(*) FILTER (WHERE s.status='ACTIVE' AND (s.blocked_reason IS NULL OR s.blocked_details->>'probe_success'='true' OR s.blocked_details->>'recovered_at' IS NOT NULL))::int active_eligible, COUNT(*) FILTER (WHERE s.status IN ('UPSTREAM_BLOCKED','COOLDOWN','DEGRADED','PROBING'))::int non_active_health FROM importer_work_mappings wm LEFT JOIN importer_sources s ON s.id=wm.source WHERE wm.sync_status='WAITING_ADMISSION'")).rows[0];
 console.log(JSON.stringify({now:new Date().toISOString(),worker:w,heartbeat:{state:h.state,autoHealState:h.autoHealState,concurrency:h.capacity?.concurrency,publishableStaged:h.publishableStaged,stuckStaged:h.stuckStaged,waitingAdmission:h.waitingAdmissionCount,eligible:h.eligibleJobs,claimableWorks:h.claimableWorks,importing:h.importingCount,activeJobs:h.pipelineCapacity?.slots?.busySlots,productive:h.pipelineCapacity?.slots?.productiveSlots,claims:h.pipelineCapacity?.claims,rate1m:h.rate1m,rate5m:h.rate5m,rate10m:h.rate10m,site:h.capacity?.siteHealth,limiting:h.capacity?.limitingFactor},sources:s,waitingAdmissionSplit:wa,rates:r,queue:q,mappings:m}));
};
for(let i=0;i<3;i++){await one(); if(i<2) await new Promise(r=>setTimeout(r,20000));}
await p.end();
