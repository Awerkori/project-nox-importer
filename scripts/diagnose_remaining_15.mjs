import pg from 'pg';
import dotenv from 'dotenv';
import { SourceRegistry } from '../build/sources/registry.js';
import { HostRateLimiter } from '../build/core/rate-limiter.js';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;
const DB_CONFIG = {
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
};

const TARGET_SOURCES = [
  'acervohentai',
  'amuy',
  'arthurscan',
  'inkapk',
  'kuro',
  'mangaonline',
  'nexus_toons',
  'nocturnesummer',
  'osakascan',
  'pointzerotoons',
  'tankouhentai',
  'tiamanhwa',
  'toonlivre',
  'yaoifanclub',
  'yuriverso'
];

async function main() {
  const client = new Client(DB_CONFIG);
  await client.connect();

  const rateLimiter = new HostRateLimiter(5.0);
  const registry = new SourceRegistry(rateLimiter);

  console.log('Fetching database stats for the 15 target sources...');
  const dbSourcesRes = await client.query(`
    SELECT id, status, enabled, chapter_ingestion_enabled, catalog_discovery_enabled, blocked_reason, updated_at
    FROM importer_sources
    WHERE id = ANY($1)
  `, [TARGET_SOURCES]);
  const dbSourcesMap = new Map(dbSourcesRes.rows.map(r => [r.id, r]));

  const mappingsRes = await client.query(`
    SELECT source, COUNT(*)::int as work_count
    FROM importer_work_mappings
    WHERE source = ANY($1)
    GROUP BY source
  `, [TARGET_SOURCES]);
  const mappingsMap = new Map(mappingsRes.rows.map(r => [r.source, r.work_count]));

  const queueRes = await client.query(`
    SELECT source, status, COUNT(*)::int as job_count
    FROM importer_queue
    WHERE source = ANY($1)
    GROUP BY source, status
  `, [TARGET_SOURCES]);
  const queueMap = new Map();
  for (const r of queueRes.rows) {
    if (!queueMap.has(r.source)) queueMap.set(r.source, {});
    queueMap.get(r.source)[r.status] = r.job_count;
  }

  const results = [];

  for (const sourceId of TARGET_SOURCES) {
    const dbSource = dbSourcesMap.get(sourceId);
    const workCount = mappingsMap.get(sourceId) || 0;
    const queueStats = queueMap.get(sourceId) || {};
    const adapter = registry.get(sourceId);

    const info = {
      source: sourceId,
      registeredInImporter: !!adapter,
      adapterClass: adapter ? adapter.constructor.name : 'NONE',
      baseUrl: adapter ? adapter.baseUrl : 'N/A',
      dbStatus: dbSource ? dbSource.status : 'NOT_IN_DB',
      dbEnabled: dbSource ? dbSource.enabled : false,
      blockedReason: dbSource ? dbSource.blocked_reason : null,
      mappedWorks: workCount,
      queuedJobs: queueStats,
      diagnostic: '',
      sampleWorksFound: 0,
      sampleChaptersFound: 0,
      samplePagesFound: 0,
      recommendedAction: ''
    };

    if (!adapter) {
      info.diagnostic = 'No adapter found in SourceRegistry';
      info.recommendedAction = 'REMOVE_FROM_IMPORTER';
      results.push(info);
      continue;
    }

    // Check domain connectivity & fetchUpdatedWorks
    try {
      console.log(`\n--- Diagnosing [${sourceId}] (${adapter.baseUrl}) ---`);
      const t0 = Date.now();
      const catalog = await adapter.fetchUpdatedWorks(null, { mode: 'bootstrap' });
      const duration = Date.now() - t0;
      info.sampleWorksFound = catalog?.works?.length || 0;
      console.log(`[${sourceId}] fetchUpdatedWorks returned ${info.sampleWorksFound} works (${duration}ms)`);

      if (info.sampleWorksFound > 0) {
        const sampleWork = catalog.works[0];
        console.log(`[${sourceId}] Testing sample work: "${sampleWork.title}" (ID: ${sampleWork.sourceWorkId})`);
        
        try {
          const chapters = await adapter.fetchChapters(sampleWork.sourceWorkId);
          info.sampleChaptersFound = chapters?.length || 0;
          console.log(`[${sourceId}] fetchChapters returned ${info.sampleChaptersFound} chapters`);

          if (info.sampleChaptersFound > 0) {
            const sampleChapter = chapters[0];
            try {
              const pages = await adapter.fetchChapterPages(sampleChapter.sourceChapterId);
              info.samplePagesFound = pages?.length || 0;
              console.log(`[${sourceId}] fetchChapterPages returned ${info.samplePagesFound} pages`);

              if (info.samplePagesFound > 0) {
                // Test fetching 1 sample page image
                const pageUrl = pages[0];
                const imgRes = await fetch(pageUrl, {
                  headers: { 'User-Agent': 'Mozilla/5.0', 'Referer': adapter.baseUrl + '/' },
                  signal: AbortSignal.timeout(8000)
                });
                console.log(`[${sourceId}] Image download test (${pageUrl.slice(0, 60)}...): status ${imgRes.status}, size ${imgRes.headers.get('content-length') || 'unknown'}`);
                if (imgRes.ok) {
                  info.diagnostic = 'FULL_PIPELINE_OK';
                  info.recommendedAction = 'ACTIVATE_IN_PRODUCTION';
                } else {
                  info.diagnostic = `IMAGE_FETCH_FAILED_HTTP_${imgRes.status}`;
                  info.recommendedAction = 'INVESTIGATE_CDN';
                }
              } else {
                info.diagnostic = 'PAGES_RETURNED_EMPTY';
                info.recommendedAction = 'FIX_PARSER_OR_REMOVE';
              }
            } catch (pErr) {
              info.diagnostic = `PAGES_ERROR: ${pErr.message}`;
              info.recommendedAction = 'FIX_PARSER_OR_REMOVE';
            }
          } else {
            info.diagnostic = 'CHAPTERS_RETURNED_EMPTY';
            info.recommendedAction = 'FIX_PARSER_OR_REMOVE';
          }
        } catch (cErr) {
          info.diagnostic = `CHAPTERS_ERROR: ${cErr.message}`;
          info.recommendedAction = 'FIX_PARSER_OR_REMOVE';
        }
      } else {
        info.diagnostic = 'CATALOG_EMPTY';
        info.recommendedAction = 'CHECK_DOMAIN_OR_REMOVE';
      }
    } catch (err) {
      console.log(`[${sourceId}] Error during diagnostic: ${err.message}`);
      info.diagnostic = `FETCH_ERROR: ${err.message}`;
      if (err.message.includes('403') || err.message.includes('Cloudflare') || err.message.includes('Challenge')) {
        info.diagnostic = 'CLOUDFLARE_BLOCKED_403';
        info.recommendedAction = 'REMOVE_FROM_IMPORTER';
      } else if (err.message.includes('ENOTFOUND') || err.message.includes('ECONNREFUSED') || err.message.includes('404')) {
        info.diagnostic = 'DOMAIN_OFFLINE_OR_DEAD';
        info.recommendedAction = 'REMOVE_FROM_IMPORTER';
      } else {
        info.recommendedAction = 'INVESTIGATE';
      }
    }

    results.push(info);
  }

  await client.end();

  console.log('\n======================================================================');
  console.log('15 SOURCES DIAGNOSTIC RESULTS');
  console.log('======================================================================');
  console.table(results.map(r => ({
    source: r.source,
    adapter: r.registeredInImporter ? r.adapterClass : 'NONE',
    dbStatus: r.dbStatus,
    works: r.mappedWorks,
    worksFound: r.sampleWorksFound,
    chapsFound: r.sampleChaptersFound,
    pagesFound: r.samplePagesFound,
    diagnostic: r.diagnostic.slice(0, 35),
    action: r.recommendedAction
  })));

  import('node:fs').then(fs => {
    fs.writeFileSync('diagnostic_15_sources.json', JSON.stringify(results, null, 2));
  });
}

main().catch(console.error);
