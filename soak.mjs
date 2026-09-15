import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const SOAK_MINUTES = 30;
const END_TIME = Date.now() + SOAK_MINUTES * 60 * 1000;

let metrics = {
  ramAverage: [],
  failed503: 0,
  failed504: 0,
  dbErrors: 0,
  ooms: 0,
  stalls: 0,
  publishedInitial: 0,
  publishedFinal: 0,
  webP95: [],
  homeP95: [],
  workP95: [],
  readerP95: [],
  adminP95: []
};

async function checkHealth() {
  while (Date.now() < END_TIME) {
    try {
      const { data: tel } = await sb.from('importer_telemetry').select('*').order('created_at', { ascending: false }).limit(1).single();
      if (tel) {
        metrics.ramAverage.push(tel.rss_mb || 0);
        if (tel.cycle_reason?.includes('OOM') || tel.cycle_reason?.includes('Killed')) metrics.ooms++;
      }
      
      const routes = [
        { path: '/', bucket: metrics.homeP95 },
        { path: '/obra/one-piece-ptbr', bucket: metrics.workP95 },
        { path: '/ler/f4e74bac-1956-4b88-8863-e262adbe0c79', bucket: metrics.readerP95 },
        { path: '/admin/importer', bucket: metrics.adminP95 }
      ];

      for (const route of routes) {
        const start = Date.now();
        const res = await fetch(`https://manga.project-nox-awerkori.workers.dev${route.path}`, {
          headers: { 'User-Agent': 'SoakTest/1.0' }
        });
        const duration = Date.now() - start;
        route.bucket.push(duration);

        if (res.status === 503) metrics.failed503++;
        if (res.status === 504) metrics.failed504++;
        if (res.status >= 500 && res.status !== 503 && res.status !== 504) metrics.dbErrors++;
      }
    } catch (e) {
      console.log("Error in soak loop", e.message);
    }
    await new Promise(r => setTimeout(r, 10000));
  }
}

async function main() {
  console.log(`Starting Soak test for ${SOAK_MINUTES} minutes...`);
  const { data: initialPubs } = await sb.from('chapters').select('id', { count: 'exact', head: true });
  metrics.publishedInitial = initialPubs ? initialPubs.length : 0;

  // Let's get actual count
  const { count } = await sb.from('chapters').select('id', { count: 'exact', head: true });
  metrics.publishedInitial = count || 0;

  await checkHealth();

  const { count: finalCount } = await sb.from('chapters').select('id', { count: 'exact', head: true });
  metrics.publishedFinal = finalCount || 0;

  console.log("SOAK COMPLETED");
  
  const calcP95 = (arr) => {
    if (!arr.length) return 0;
    arr.sort((a,b) => a-b);
    return arr[Math.floor(arr.length * 0.95)];
  };

  const calcAvg = (arr) => {
    if (!arr.length) return 0;
    return arr.reduce((a,b) => a+b, 0) / arr.length;
  };

  const finalMetrics = {
    durationMinutes: SOAK_MINUTES,
    ramAverage: calcAvg(metrics.ramAverage),
    ramPeak: Math.max(...(metrics.ramAverage.length ? metrics.ramAverage : [0])),
    failed503: metrics.failed503,
    failed504: metrics.failed504,
    dbErrors: metrics.dbErrors,
    ooms: metrics.ooms,
    publishedInitial: metrics.publishedInitial,
    publishedFinal: metrics.publishedFinal,
    chaptersPublished: metrics.publishedFinal - metrics.publishedInitial,
    publishRatePerMin: (metrics.publishedFinal - metrics.publishedInitial) / SOAK_MINUTES,
    homeP95: calcP95(metrics.homeP95),
    workP95: calcP95(metrics.workP95),
    readerP95: calcP95(metrics.readerP95),
    adminP95: calcP95(metrics.adminP95),
  };

  console.log(JSON.stringify(finalMetrics, null, 2));
}

main();
