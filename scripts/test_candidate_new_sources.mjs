import fs from 'node:fs';
import { MadaraAdapter } from '../build/sources/common/madara-adapter.js';
import { ZeistMangaAdapter } from '../build/sources/common/zeistmanga-adapter.js';
import { MangaThemesiaAdapter } from '../build/sources/common/mangathemesia-adapter.js';
import { HostRateLimiter } from '../build/core/rate-limiter.js';

const rateLimiter = new HostRateLimiter(5.0);

// Candidate sources identified as ONLINE
const CANDIDATES = [
  // Madara candidates
  { id: 'azuretoons', name: 'Azuretoons', baseUrl: 'https://azuretoons.com', type: 'madara' },
  { id: 'fenixproject', name: 'Fenix Project', baseUrl: 'https://fenixproject.site', type: 'madara' },
  { id: 'flowermanga', name: 'Flower Manga', baseUrl: 'https://flowermangas.net', type: 'madara' },
  
  // ZeistManga candidates
  { id: 'hanmokkuscan', name: 'Hanmokku Scan', baseUrl: 'https://hanmokkuscan.blogspot.com', type: 'zeistmanga' },
  { id: 'temakimangas', name: 'Temaki Mangás', baseUrl: 'https://temakimangas.blogspot.com', type: 'zeistmanga' },
  { id: 'timelinecomics', name: 'Timeline Comics', baseUrl: 'https://timelinecomics.blogspot.com', type: 'zeistmanga' },
  { id: 'traducoesdolipe', name: 'Traduções do Lipe', baseUrl: 'https://traducoesdolipe.blogspot.com', type: 'zeistmanga' },

  // MangaThemesia candidates
  { id: 'mangastop', name: 'Manga Stop', baseUrl: 'https://mangastop.net', type: 'mangathemesia' },
  { id: 'starlightscan', name: 'Starlight Scan', baseUrl: 'https://starligthscan.com', type: 'mangathemesia' },

  // Other PT-BR sources
  { id: 'bryaoi', name: 'BR Yaoi', baseUrl: 'https://bryaoi.com', type: 'madara' },
  { id: 'horahentai', name: 'Hora Hentai', baseUrl: 'https://horahentai.com', type: 'madara' },
  { id: 'exhentainetbr', name: 'ExHentai BR', baseUrl: 'https://exhentai.net.br', type: 'madara' },
  { id: 'terceiroz', name: 'Terceiro Z', baseUrl: 'https://terceiroz.com', type: 'madara' },
  { id: 'muitohentai', name: 'Muito Hentai', baseUrl: 'https://www.muitohentai.com', type: 'madara' }
];

async function main() {
  console.log(`Testing ${CANDIDATES.length} candidate sources for potential addition...`);
  const results = [];

  for (const cand of CANDIDATES) {
    let adapter;
    if (cand.type === 'madara') {
      adapter = new MadaraAdapter({ id: cand.id, name: cand.name, baseUrl: cand.baseUrl, rateLimitRps: 2.0 }, rateLimiter);
    } else if (cand.type === 'zeistmanga') {
      adapter = new ZeistMangaAdapter({ id: cand.id, name: cand.name, baseUrl: cand.baseUrl, rateLimitRps: 2.0 }, rateLimiter);
    } else if (cand.type === 'mangathemesia') {
      adapter = new MangaThemesiaAdapter({ id: cand.id, name: cand.name, baseUrl: cand.baseUrl, rateLimitRps: 2.0 }, rateLimiter);
    }

    const resInfo = {
      id: cand.id,
      name: cand.name,
      baseUrl: cand.baseUrl,
      type: cand.type,
      status: 'TESTING',
      worksFound: 0,
      chaptersFound: 0,
      pagesFound: 0,
      imageSize: 0,
      error: null
    };

    try {
      console.log(`\n--- Testing [${cand.id}] (${cand.baseUrl}) [${cand.type}] ---`);
      const t0 = Date.now();
      const cat = await adapter.fetchUpdatedWorks(null, { mode: 'bootstrap' });
      resInfo.worksFound = cat?.works?.length || 0;
      console.log(`[${cand.id}] fetchUpdatedWorks returned ${resInfo.worksFound} works`);

      if (resInfo.worksFound > 0) {
        const sampleWork = cat.works[0];
        console.log(`[${cand.id}] Sample work: "${sampleWork.title}" (ID: ${sampleWork.sourceWorkId})`);
        const chaps = await adapter.fetchChapters(sampleWork.sourceWorkId);
        resInfo.chaptersFound = chaps?.length || 0;
        console.log(`[${cand.id}] fetchChapters returned ${resInfo.chaptersFound} chapters`);

        if (resInfo.chaptersFound > 0) {
          const sampleChap = chaps[0];
          const pages = await adapter.fetchChapterPages(sampleChap.sourceChapterId);
          resInfo.pagesFound = pages?.length || 0;
          console.log(`[${cand.id}] fetchChapterPages returned ${resInfo.pagesFound} pages`);

          if (resInfo.pagesFound > 0) {
            const samplePage = pages[0];
            const headers = adapter.getImageHeaders ? adapter.getImageHeaders(samplePage) : {
              'User-Agent': 'Mozilla/5.0',
              Referer: cand.baseUrl + '/'
            };
            const imgRes = await fetch(samplePage, { headers, signal: AbortSignal.timeout(8000) });
            if (imgRes.ok) {
              const buf = await imgRes.arrayBuffer();
              resInfo.imageSize = buf.byteLength;
              resInfo.status = 'FULLY_FUNCTIONAL';
              console.log(`[${cand.id}] Image download OK: ${resInfo.imageSize} bytes`);
            } else {
              resInfo.status = `IMG_HTTP_${imgRes.status}`;
            }
          } else {
            resInfo.status = 'PAGES_EMPTY';
          }
        } else {
          resInfo.status = 'CHAPTERS_EMPTY';
        }
      } else {
        resInfo.status = 'CATALOG_EMPTY';
      }
    } catch (err) {
      console.log(`[${cand.id}] Error: ${err.message}`);
      resInfo.status = 'ERROR';
      resInfo.error = err.message.slice(0, 100);
    }

    results.push(resInfo);
  }

  console.log('\n======================================================================');
  console.log('CANDIDATE SOURCES RESULTS');
  console.log('======================================================================');
  console.table(results);

  fs.writeFileSync('candidate_sources_results.json', JSON.stringify(results, null, 2));
}

main().catch(console.error);
