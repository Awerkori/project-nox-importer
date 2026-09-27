// Bounded, read-only production validation. Never import the engine or print env.
import fs from 'node:fs';
import pg from 'pg';
import dotenv from 'dotenv';
import { setTimeout as sleep } from 'node:timers/promises';

const envFile = process.env.NOX_AUDIT_ENV_FILE;
if (!envFile) throw new Error('NOX_AUDIT_ENV_FILE is required');
const env = dotenv.parse(fs.readFileSync(envFile));
const minutes = Number(process.argv[2] || 5);
if (!Number.isInteger(minutes) || minutes < 1 || minutes > 10) throw new Error('Window must be 1..10 minutes');
const output = process.argv[3];
const pool = new pg.Pool({
  host: env.YUGABYTE_HOST, port: Number(env.YUGABYTE_PORT || 5433), user: env.YUGABYTE_USER,
  password: env.YUGABYTE_PASSWORD, database: env.YUGABYTE_DATABASE,
  ssl: {rejectUnauthorized:true,ca:fs.readFileSync(env.YUGABYTE_SSL_CERT)},
  max:1, connectionTimeoutMillis:5000, query_timeout:7000, application_name:'nox-bounded-validation',
  options:'-c statement_timeout=6000 -c default_transaction_read_only=on',
});
const samples=[], probes=[];
function quantile(xs,p) { const s=[...xs].sort((a,b)=>a-b);return s.length?s[Math.min(s.length-1,Math.floor(s.length*p))]:null; }
async function query(sql, args=[]) { return (await pool.query(sql,args)).rows; }
async function probe(route,url) {
  const started=performance.now();
  try {
    const r=await fetch(url,{signal:AbortSignal.timeout(12000),headers:{'User-Agent':'NoxBoundedValidation/1.0'}});
    const ttfbMs=Math.round(performance.now()-started);
    // Reading the HTML checks transport completion; no images/assets are fetched.
    const html=await r.text();
    probes.push({at:new Date().toISOString(),route,status:r.status,ttfbMs,bytes:Buffer.byteLength(html),routeError:/data-error="true"|Internal Error|Internal Server Error/.test(html)});
  } catch(e) {probes.push({at:new Date().toISOString(),route,status:0,ttfbMs:Math.round(performance.now()-started),error:e.name});}
}
try {
  const clock=(await query("SELECT date_trunc('minute',clock_timestamp()) + interval '1 minute' AS start"))[0];
  const start=new Date(clock.start), end=new Date(start.getTime()+minutes*60000);
  const [reader]=await query(`SELECT c.id,w.slug FROM chapters c JOIN works w ON w.id=c.work_id
    WHERE c.published_at IS NOT NULL AND w.published=true ORDER BY c.published_at DESC LIMIT 1`);
  console.log(JSON.stringify({phase:'scheduled',start,end,minutes}));
  if(Date.now()<start.getTime()) await sleep(start.getTime()-Date.now());
  let nextSample=Date.now();
  while(Date.now()<end.getTime()) {
    const at=new Date().toISOString();
    const routes=[['home','/'],['work',`/obra/${reader.slug}`],['reader',`/ler/${reader.id}`]];
    await Promise.allSettled(routes.map(([route,path])=>probe(route,`https://manga.project-nox-awerkori.workers.dev${path}`)));
    try {
      const [t]=await query("SELECT created_at,data FROM importer_diagnostic_telemetry WHERE session_id='runtime'");
      const d=t?.data||{};
      const [activity]=await query(`SELECT count(*)::int AS total, count(*) FILTER(WHERE state='active')::int AS active,
        count(*) FILTER(WHERE state='idle')::int AS idle,count(*) FILTER(WHERE state='idle in transaction')::int AS idle_in_transaction
        FROM pg_stat_activity WHERE datname=current_database()`);
      const [progress]=await query('SELECT count(*)::int AS visible FROM chapters WHERE published_at >= $1 AND published_at < LEAST(clock_timestamp(),$2::timestamptz)',[start,end]);
      const sample={at,telemetryAt:t?.created_at,visible:progress.visible,activity,
        runtime:d.runtimeFingerprint,slots:d.slotsConfigured,effective:d.effectiveConcurrency,
        states:d.avgSlotStates,occupancy:d.slotOccupancy,node:d.eventLoopAndNode,database:d.database,
        pool:d.yugabyteDbPool,telegram:d.telegramStorage,downloads:d.imageDownload,limiters:d.limitersAudit,
        stages:d.jobProfile?.stages,wallTimeBreakdown:d.jobProfile?.wallTimeBreakdownMs,sourceDistribution:d.sourceDistribution};
      samples.push(sample);
      console.log(JSON.stringify({phase:'sample',at,visible:sample.visible,effective:sample.effective,rss:sample.node?.rssMb,cpu:sample.node?.processCpuPercentAvg,lagP95:sample.node?.eventLoopLagP95,poolWaitP95:sample.pool?.waitP95Ms,telemetryAt:sample.telemetryAt}));
    } catch(e) {samples.push({at,error:e.code||e.name});}
    nextSample+=30000;
    await sleep(Math.max(0,Math.min(nextSample,end.getTime())-Date.now()));
  }
  const buckets=await query(`WITH minutes AS (SELECT generate_series($1::timestamptz,$2::timestamptz-interval '1 minute',interval '1 minute') AS minute),
    canonical AS (SELECT date_trunc('minute',published_at) AS minute,count(*)::int AS n FROM chapters WHERE published_at >= $1 AND published_at < $2 GROUP BY 1)
    SELECT m.minute,COALESCE(c.n,0)::int AS canonical,COALESCE(b.visible_published,0)::int AS bucket
    FROM minutes m LEFT JOIN canonical c USING(minute) LEFT JOIN importer_rate_buckets b ON b.bucket_minute=m.minute ORDER BY 1`,[start,end]);
  const [events]=await query(`SELECT count(*)::int AS events,count(*) FILTER(WHERE e.transition_at<>c.published_at)::int AS clock_mismatch,
    count(*) FILTER(WHERE e.bucket_minute<>date_trunc('minute',c.published_at))::int AS bucket_mismatch
    FROM importer_publication_events e JOIN chapters c ON c.id=e.chapter_id WHERE e.transition_at >= $1 AND e.transition_at < $2`,[start,end]);
  const workload=await query(`SELECT source,count(*)::int AS completed,count(DISTINCT chapter_id)::int AS chapters,
    avg(page_count)::numeric(12,2) AS pages_per_chapter,avg(total_bytes)::bigint AS bytes_per_chapter,
    percentile_cont(.5) WITHIN GROUP(ORDER BY duration_ms) AS occupancy_p50,
    percentile_cont(.95) WITHIN GROUP(ORDER BY duration_ms) AS occupancy_p95,
    avg(db_ms)::int AS db_ms,avg(download_ms)::int AS download_ms,avg(upload_ms)::int AS upload_ms
    FROM importer_job_metrics WHERE created_at >= $1 AND created_at < $2 GROUP BY source`,[start,end]);
  const health=Object.fromEntries(['home','work','reader'].map(route=>{
    const p=probes.filter(x=>x.route===route);return [route,{samples:p.length,success:p.filter(x=>x.status===200&&!x.routeError).length,
      notFound:p.filter(x=>x.status===404).length,serverError:p.filter(x=>x.status>=500).length,transportError:p.filter(x=>x.status===0).length,
      p50:quantile(p.map(x=>x.ttfbMs),.5),p95:quantile(p.map(x=>x.ttfbMs),.95)}];
  }));
  const values=buckets.map(b=>b.canonical), total=values.reduce((a,b)=>a+b,0), mean=total/minutes;
  const report={start,end,minutes,buckets,total,rate:mean,median:quantile(values,.5),min:Math.min(...values),max:Math.max(...values),
    standardDeviation:Math.sqrt(values.reduce((a,b)=>a+(b-mean)**2,0)/minutes),events,workload,health,samples,probes};
  if(output) fs.writeFileSync(output,JSON.stringify(report,null,2),{mode:0o600});
  console.log(JSON.stringify({phase:'complete',...report,samples:report.samples.length,probes:report.probes.length}));
} catch(e) {console.log(JSON.stringify({phase:'error',code:e.code||e.name}));process.exitCode=1;}
finally {await pool.end();}
