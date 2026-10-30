import dotenv from "dotenv";
import pg from "pg";
import { performance } from "node:perf_hooks";

dotenv.config({ path: "/home/awerkori/.Projects/project-nox-importer/.env" });

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || "5433", 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

const SITE_URLS = [
  "https://manga.project-nox-awerkori.workers.dev/",
  "https://manga.project-nox-awerkori.workers.dev/lancamentos",
  "https://manga.project-nox-awerkori.workers.dev/catalogo",
  "https://manga.project-nox-awerkori.workers.dev/obra/nano-machine"
];

async function probeSite(): Promise<{ ok: boolean; status: number; latencyMs: number; error1102: boolean }[]> {
  const results = [];
  for (const url of SITE_URLS) {
    const t0 = performance.now();
    try {
      const res = await fetch(url, { headers: { "User-Agent": "nox-homologation-probe" }, signal: AbortSignal.timeout(8000) });
      const latencyMs = Math.round(performance.now() - t0);
      const text = await res.text();
      const error1102 = (res.status >= 500 && text.includes("1102")) || text.includes("Worker exceeded CPU");
      results.push({ ok: res.ok && !error1102, status: res.status, latencyMs, error1102 });
    } catch (err: any) {
      results.push({ ok: false, status: 0, latencyMs: Math.round(performance.now() - t0), error1102: false });
    }
  }
  return results;
}

export async function runMonitoring(durationMinutes = 60) {
  await client.connect();
  console.log(`Starting 60-Minute Sustained Production Homologation Monitor... Target: >= 600 chapters`);
  
  const startTime = Date.now();
  let initialPubCount = 0;

  const initRes = await client.query(`SELECT COUNT(*)::int as total FROM importer_publication_events`);
  initialPubCount = initRes.rows[0].total;
  console.log(`Initial total publications recorded in DB: ${initialPubCount}`);

  const minuteLogs: any[] = [];
  let currentMinute = 0;

  while (currentMinute < durationMinutes) {
    await new Promise(r => setTimeout(r, 60_000));
    currentMinute++;
    const now = Date.now();
    const elapsedMinutes = Math.round((now - startTime) / 60_000);

    // 1. Check publications in this minute & window
    const pubRes = await client.query(`
      SELECT 
        COUNT(*) FILTER (WHERE transition_at >= NOW() - INTERVAL '1 minute')::int as last_1m,
        COUNT(*) FILTER (WHERE transition_at >= NOW() - INTERVAL '5 minutes')::int as last_5m,
        COUNT(*) FILTER (WHERE transition_at >= NOW() - INTERVAL '15 minutes')::int as last_15m,
        COUNT(*) FILTER (WHERE transition_at >= $1::timestamptz)::int as total_in_run,
        COUNT(DISTINCT work_id) FILTER (WHERE transition_at >= $1::timestamptz)::int as distinct_works
      FROM importer_publication_events
    `, [new Date(startTime).toISOString()]);

    const stats = pubRes.rows[0];

    // 2. Probe site health
    const probes = await probeSite();
    const maxLatency = Math.max(...probes.map(p => p.latencyMs));
    const allOk = probes.every(p => p.ok);
    const has1102 = probes.some(p => p.error1102);

    // 3. Heartbeat & worker check
    const hbRes = await client.query(`SELECT value FROM settings WHERE key = 'importer_heartbeat'`);
    let hb: any = null;
    if (hbRes.rows.length > 0) {
      hb = typeof hbRes.rows[0].value === 'string' ? JSON.parse(hbRes.rows[0].value) : hbRes.rows[0].value;
    }

    const logEntry = {
      minute: currentMinute,
      elapsedMinutes,
      newThisMinute: stats.last_1m,
      totalPublished: stats.total_in_run,
      distinctWorks: stats.distinct_works,
      ratePerMin: (stats.total_in_run / elapsedMinutes).toFixed(1),
      maxSiteLatencyMs: maxLatency,
      siteHealthy: allOk,
      error1102: has1102,
      activeWorkers: hb?.activeChapterWorkers ?? '?',
      rssMiB: hb?.memoryPressure?.rssMiB ?? '?'
    };

    minuteLogs.push(logEntry);
    console.log(`[Min ${currentMinute}/${durationMinutes}] +${logEntry.newThisMinute} cap (Total: ${logEntry.totalPublished}, ${logEntry.distinctWorks} obras, Rate: ${logEntry.ratePerMin} c/m) | Site Latency Max: ${maxLatency}ms (Healthy: ${allOk}, 1102: ${has1102}) | Workers: ${logEntry.activeWorkers} | RSS: ${logEntry.rssMiB}MiB`);
  }

  console.log("\n=== 60-MINUTE MONITORING COMPLETED ===");
  console.log(JSON.stringify(minuteLogs, null, 2));

  await client.end();
}

if (process.argv[1] && process.argv[1].endsWith("monitor_clean_60m.ts")) {
  runMonitoring(60).catch(console.error);
}
