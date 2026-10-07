import puppeteer from 'puppeteer-core';
import pg from 'pg';
import dotenv from 'dotenv';
import https from 'https';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const SITE_URL = 'https://manga.project-nox-awerkori.workers.dev';
const CHROME_PATH = '/usr/bin/chromium';

// Active session token for legacytest (ADMIN)
const AUTH_COOKIE = {
  name: 'better-auth.session_token',
  value: 'zRBtDOEBdX15smtnmVG4GhRcYPgxRUac',
  domain: 'manga.project-nox-awerkori.workers.dev',
  path: '/',
  httpOnly: true,
  secure: true,
  sameSite: 'Lax',
};

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
});

function calculatePercentiles(samples) {
  if (!samples.length) return { p50: 0, p95: 0, min: 0, max: 0, avg: 0 };
  const sorted = [...samples].sort((a, b) => a - b);
  const p50 = sorted[Math.floor(sorted.length * 0.5)];
  const p95 = sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.95))];
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  const avg = Math.round(sorted.reduce((acc, cur) => acc + cur, 0) / sorted.length);
  return { p50, p95, min, max, avg };
}

async function measureRouteHttps(path, iterations = 10) {
  const agent = new https.Agent({ keepAlive: true });
  const latencies = [];
  const statusCodes = [];

  for (let i = 0; i < iterations; i++) {
    await new Promise((resolve) => {
      const t0 = performance.now();
      const req = https.get(
        `${SITE_URL}${path}`,
        {
          agent,
          headers: {
            'User-Agent': 'Mozilla/5.0 (HealthCheck/2.0)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
          },
          timeout: 5000,
        },
        (res) => {
          res.resume();
          res.on('end', () => {
            latencies.push(Math.round(performance.now() - t0));
            statusCodes.push(res.statusCode);
            resolve();
          });
        }
      );
      req.on('error', (err) => {
        statusCodes.push(599);
        resolve();
      });
      req.on('timeout', () => {
        req.destroy();
        statusCodes.push(504);
        resolve();
      });
    });
  }
  return {
    path,
    ...calculatePercentiles(latencies),
    statusCodes: [...new Set(statusCodes)],
  };
}

async function testPuppeteer(auth = false) {
  const browser = await puppeteer.launch({
    executablePath: CHROME_PATH,
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
  });

  const page = await browser.newPage();
  if (auth) {
    await page.setCookie(AUTH_COOKIE);
  }

  const errors = [];
  page.on('pageerror', (err) => errors.push(err.message));
  page.on('requestfailed', (req) => errors.push(`${req.url()}: ${req.failure()?.errorText}`));

  const t0 = performance.now();
  const res = await page.goto(SITE_URL, { waitUntil: 'networkidle2', timeout: 15000 });
  const loadTime = Math.round(performance.now() - t0);
  const status = res?.status();

  // Check if content rendered
  const title = await page.title();
  const bodyTextLength = (await page.evaluate(() => document.body.innerText)).length;
  const hasWorks = await page.evaluate(() => document.querySelectorAll('a[href^="/obra/"]').length);

  await browser.close();

  return {
    auth,
    status,
    loadTime,
    title,
    bodyTextLength,
    hasWorks,
    errors,
  };
}

async function main() {
  await client.connect();

  console.log('--- 1. Testing Puppeteer Navigation (Anonymous & Authenticated) ---');
  const anonTest = await testPuppeteer(false);
  console.log('Anonymous:', anonTest);

  const authTest = await testPuppeteer(true);
  console.log('Authenticated:', authTest);

  console.log('\n--- 2. Measuring Route Percentiles ---');
  // 1. Home
  const homeStats = await measureRouteHttps('/', 10);
  console.log('Home:', homeStats);

  // 2. Catalogo
  const catalogoStats = await measureRouteHttps('/catalogo', 10);
  console.log('Catalogo:', catalogoStats);

  // 3. Obra
  const obraRes = await client.query("SELECT slug FROM works WHERE published = true ORDER BY updated_at DESC LIMIT 1");
  const sampleSlug = obraRes.rows[0]?.slug || 'solo-leveling';
  const obraStats = await measureRouteHttps(`/obra/${sampleSlug}`, 10);
  console.log(`Obra (/obra/${sampleSlug}):`, obraStats);

  // 4. Reader
  const readerRes = await client.query("SELECT id FROM chapters WHERE published_at IS NOT NULL ORDER BY published_at DESC LIMIT 1");
  const sampleChapterId = readerRes.rows[0]?.id || '46b7538b-fcb8-40ec-b3ee-cdadd2edb04c';
  const readerStats = await measureRouteHttps(`/ler/${sampleChapterId}`, 10);
  console.log(`Reader (/ler/${sampleChapterId}):`, readerStats);

  // 5. Media
  const mediaRes = await client.query("SELECT id FROM media WHERE id IS NOT NULL LIMIT 1");
  const sampleMediaId = mediaRes.rows[0]?.id;
  let mediaStats = { p50: 0, p95: 0, statusCodes: [200] };
  if (sampleMediaId) {
    mediaStats = await measureRouteHttps(`/media/${sampleMediaId}`, 10);
    console.log(`Media (/media/${sampleMediaId}):`, mediaStats);
  }

  console.log('\n--- 3. Checking Importer & DB Health Metrics ---');
  // P0 Waiting
  const p0Res = await client.query(`
    SELECT count(*) as count
    FROM importer_queue
    WHERE status = 'QUEUED' AND (priority = 0 OR task_type IN ('DISCOVERY_UPDATE', 'HIGH_PRIORITY_FETCH'))
  `);

  // P1 Claimable
  const p1ClaimableRes = await client.query(`
    SELECT COUNT(*) as count
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    JOIN importer_sources s ON s.id = q.source
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND q.status IN ('QUEUED', 'RETRY')
      AND (q.next_run_at IS NULL OR q.next_run_at <= NOW())
      AND w.published = true
      AND s.enabled = true
      AND s.status = 'ACTIVE'
      AND (s.cooldown_until IS NULL OR s.cooldown_until <= NOW());
  `);

  // P1 Claims Last 5M
  const p1Claims5mRes = await client.query(`
    SELECT COUNT(*) as count
    FROM importer_queue q
    JOIN works w ON w.id = (q.payload->>'workId')::uuid
    WHERE q.task_type = 'IMPORT_CHAPTER'
      AND q.locked_at >= NOW() - INTERVAL '5 minutes';
  `);

  // Chapters published last 5M & last published chapter time
  const pubRes = await client.query(`
    SELECT count(*) as count, max(published_at) as last_published
    FROM chapters
    WHERE published_at >= NOW() - INTERVAL '5 minutes';
  `);
  const lastOverallPubRes = await client.query(`
    SELECT max(published_at) as last_published FROM chapters WHERE published_at IS NOT NULL;
  `);

  // P3 admissions last 5m
  const p3AdmissionsRes = await client.query(`
    SELECT count(*) as count
    FROM importer_work_mappings
    WHERE created_at >= NOW() - INTERVAL '5 minutes'
      AND sync_status = 'SYNCED';
  `);

  // New works last 5m
  const newWorks5mRes = await client.query(`
    SELECT count(*) as count
    FROM works
    WHERE created_at >= NOW() - INTERVAL '5 minutes';
  `);

  // Settings
  const settingsRes = await client.query(`
    SELECT key, value FROM settings WHERE key IN ('importer_protective_stop', 'publication_safety_barrier', 'work_affinity_scheduler_enabled')
  `);

  // DB connections
  const actRes = await client.query(`
    SELECT count(*) as total,
           count(*) FILTER (WHERE state = 'active') as active,
           count(*) FILTER (WHERE application_name = 'Cloudflare Hyperdrive') as hyperdrive,
           count(*) FILTER (WHERE application_name = 'project-nox-importer-direct') as importer
    FROM pg_stat_activity;
  `);

  // Slow queries
  const slowRes = await client.query(`
    SELECT pid, now() - query_start as duration, state, query, application_name
    FROM pg_stat_activity
    WHERE state = 'active' AND query NOT ILIKE '%pg_stat_activity%'
    ORDER BY duration DESC LIMIT 5;
  `);

  console.log(JSON.stringify({
    importer: {
      p0Waiting: p0Res.rows[0].count,
      p1Claimable: p1ClaimableRes.rows[0].count,
      p1Claims5m: p1Claims5mRes.rows[0].count,
      chaptersPublished5m: pubRes.rows[0].count,
      lastPublished5m: pubRes.rows[0].last_published,
      lastPublishedOverall: lastOverallPubRes.rows[0].last_published,
      p3Admissions5m: p3AdmissionsRes.rows[0].count,
      newWorks5m: newWorks5mRes.rows[0].count,
      settings: settingsRes.rows,
    },
    db: {
      connections: actRes.rows[0],
      slowQueries: slowRes.rows,
    },
  }, null, 2));

  await client.end();
}

main().catch(console.error);
