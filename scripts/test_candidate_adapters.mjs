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

const rateLimiter = new HostRateLimiter(5.0);
const registry = new SourceRegistry(rateLimiter);

// The 29 sources classified with DISABLED_RESIDUAL_FLAG
const CANDIDATE_SOURCES = [
  'apecomics',
  'apenasumafa',
  'borutoexplorer',
  'brasilhentai',
  'cafecomyaoi',
  'covenscan',
  'euphoriascan',
  'galaxscanlator',
  'hentaifusion',
  'hentaihome',
  'hentaiseason',
  'hentaitokyo',
  'hipercool',
  'instahentai',
  'kamisamaexplorer',
  'ler999',
  'littletyrant',
  'maidscan',
  'mangaonline',
  'mangotoons',
  'megahentai',
  'mundohentai',
  'nhentaibr',
  'osakascan',
  'pinkrosa',
  'pizzariascan',
  'pointzerotoons',
  'universohentai',
  'zettahq'
];

async function main() {
  const client = new Client(DB_CONFIG);
  await client.connect();

  console.log('Testing candidate adapters on real work mappings...');
  const results = [];

  for (const sourceId of CANDIDATE_SOURCES) {
    const adapter = registry.get(sourceId);
    if (!adapter) {
      results.push({ sourceId, status: 'NO_ADAPTER', error: 'Adapter not found' });
      continue;
    }

    // Find 1 sample work mapping from DB
    const mappingRes = await client.query(`
      SELECT source_work_id, source_slug, source_title
      FROM importer_work_mappings
      WHERE source = $1
      ORDER BY updated_at DESC
      LIMIT 1
    `, [sourceId]);

    const sampleWork = mappingRes.rows[0];
    if (!sampleWork) {
      // Try to call fetchUpdatedWorks to see if catalog works
      try {
        const t0 = Date.now();
        const { works } = await adapter.fetchUpdatedWorks(null, { mode: 'bootstrap' });
        results.push({
          sourceId,
          status: works && works.length > 0 ? 'ADAPTER_HEALTHY_CATALOG' : 'EMPTY_CATALOG',
          worksFound: works?.length || 0,
          sampleWorkId: null,
          durationMs: Date.now() - t0,
          error: null
        });
      } catch (err) {
        results.push({
          sourceId,
          status: 'PARSER_BROKEN',
          worksFound: 0,
          sampleWorkId: null,
          error: err.message
        });
      }
      continue;
    }

    // Try fetching chapters for this sample work
    try {
      const t0 = Date.now();
      const chapters = await adapter.fetchChapters(sampleWork.source_work_id);
      const durationMs = Date.now() - t0;
      if (Array.isArray(chapters) && chapters.length > 0) {
        results.push({
          sourceId,
          status: 'ADAPTER_HEALTHY_CONFIRMED',
          sampleWorkId: sampleWork.source_work_id,
          chaptersFound: chapters.length,
          durationMs,
          error: null
        });
      } else {
        // Returned empty array
        results.push({
          sourceId,
          status: 'CHAPTERS_EMPTY',
          sampleWorkId: sampleWork.source_work_id,
          chaptersFound: 0,
          durationMs,
          error: 'Returned 0 chapters'
        });
      }
    } catch (err) {
      const errMsg = err.message || String(err);
      let classification = 'PARSER_BROKEN';
      if (errMsg.includes('403') || errMsg.includes('Cloudflare') || errMsg.includes('blocked') || errMsg.includes('Challenge')) {
        classification = 'RATE_LIMIT/BLOCKED';
      } else if (errMsg.includes('404') || errMsg.includes('ENOTFOUND')) {
        classification = 'SOURCE_OFFLINE';
      }
      results.push({
        sourceId,
        status: classification,
        sampleWorkId: sampleWork.source_work_id,
        chaptersFound: 0,
        error: errMsg.slice(0, 120)
      });
    }

    console.log(`Tested [${sourceId.padEnd(20)}] -> ${results[results.length - 1].status}`);
  }

  await client.end();

  console.log('\n======================================================================');
  console.log('ADAPTER VERIFICATION RESULTS');
  console.log('======================================================================');
  console.table(results);

  import('node:fs').then(fs => {
    fs.writeFileSync('adapter_test_results.json', JSON.stringify(results, null, 2));
  });
}

main().catch(console.error);
