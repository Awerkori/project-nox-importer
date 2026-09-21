import { SourceRegistry } from '../build/sources/registry.js';
import { HostRateLimiter } from '../build/core/rate-limiter.js';
import dotenv from 'dotenv';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const rateLimiter = new HostRateLimiter(5.0);
const registry = new SourceRegistry(rateLimiter);

const RECOVERED_SOURCES = [
  'kuro',
  'nocturnesummer',
  'tankouhentai',
  'osakascan',
  'acervohentai',
  'amuy',
  'arthurscan',
  'inkapk',
  'mangaonline',
  'yaoifanclub',
  'yuriverso'
];

async function main() {
  console.log('======================================================================');
  console.log('END-TO-END VERIFICATION OF 11 RECOVERED SOURCES');
  console.log('======================================================================');

  const report = [];

  for (const sourceId of RECOVERED_SOURCES) {
    const adapter = registry.get(sourceId);
    if (!adapter) {
      report.push({ source: sourceId, status: 'FAILED', error: 'No adapter registered' });
      continue;
    }

    try {
      console.log(`\nTesting [${sourceId}] (${adapter.baseUrl})...`);
      const t0 = Date.now();

      // 1. Fetch updated works
      const catalog = await adapter.fetchUpdatedWorks(null, { mode: 'bootstrap' });
      const works = catalog?.works || [];
      if (works.length === 0) {
        throw new Error('Catalog returned 0 works');
      }

      // Pick a suitable work (avoid text novels for sources that have them)
      let sampleWork = works[0];
      if (sourceId === 'nocturnesummer') {
        const manga = works.find(w => !w.title.toLowerCase().includes('novel') && !w.sourceWorkId.includes('novel')) || works[0];
        sampleWork = manga;
      } else if (sourceId === 'yaoifanclub') {
        const comic = works.find(w => !w.sourceWorkId.includes('novel') && !w.title.toLowerCase().includes('novel')) || works[0];
        sampleWork = comic;
      } else if (sourceId === 'osakascan') {
        const mama = works.find(w => w.title.toLowerCase().includes('mama')) || works[0];
        sampleWork = mama;
      }

      // 2. Fetch chapters
      const chapters = await adapter.fetchChapters(sampleWork.sourceWorkId);
      if (!chapters || chapters.length === 0) {
        throw new Error(`fetchChapters returned 0 chapters for work ${sampleWork.sourceWorkId}`);
      }
      const sampleChapter = chapters[0];

      // 3. Fetch chapter pages
      const pages = await adapter.fetchChapterPages(sampleChapter.sourceChapterId);
      if (!pages || pages.length === 0) {
        throw new Error(`fetchChapterPages returned 0 pages for chapter ${sampleChapter.sourceChapterId}`);
      }

      // 4. Download sample image
      const sampleImageUrl = pages[0];
      const headers = adapter.getImageHeaders ? adapter.getImageHeaders(sampleImageUrl) : {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        Referer: adapter.baseUrl + '/'
      };
      const imgRes = await fetch(sampleImageUrl, {
        headers,
        signal: AbortSignal.timeout(10000)
      });

      if (!imgRes.ok) {
        throw new Error(`Sample image download returned HTTP ${imgRes.status} for ${sampleImageUrl}`);
      }

      const imgBuffer = await imgRes.arrayBuffer();
      const durationMs = Date.now() - t0;

      console.log(`✓ [${sourceId}] PASSED in ${durationMs}ms: ${works.length} works, work "${sampleWork.title}" has ${chapters.length} chaps, chap "${sampleChapter.title || sampleChapter.number}" has ${pages.length} pages, sample img downloaded: ${imgBuffer.byteLength} bytes`);

      report.push({
        source: sourceId,
        status: 'VERIFIED_HEALTHY',
        worksFound: works.length,
        sampleWork: sampleWork.title.slice(0, 30),
        chaptersFound: chapters.length,
        pagesFound: pages.length,
        sampleImageSize: imgBuffer.byteLength,
        durationMs,
        error: null
      });
    } catch (err) {
      console.error(`✗ [${sourceId}] FAILED: ${err.message}`);
      report.push({
        source: sourceId,
        status: 'FAILED',
        error: err.message
      });
    }
  }

  console.log('\n======================================================================');
  console.log('RECOVERED SOURCES FINAL VERIFICATION TABLE');
  console.log('======================================================================');
  console.table(report);

  const passedCount = report.filter(r => r.status === 'VERIFIED_HEALTHY').length;
  console.log(`\nVerification complete: ${passedCount}/${RECOVERED_SOURCES.length} passed.`);
}

main().catch(console.error);
