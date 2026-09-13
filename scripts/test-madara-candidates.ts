import { MadaraAdapter } from '../src/sources/common/madara-adapter.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';

const candidates = [
  { id: 'acervohentai', name: 'Acervo Hentai', baseUrl: 'https://acervohentai.com', mangaSubString: 'manhwa' },
  { id: 'huntersscans', name: 'Hunters Scans', baseUrl: 'https://readhunters.xyz', mangaSubString: 'comics' },
  { id: 'inkapk', name: 'Inkapk', baseUrl: 'https://inkapk.net', mangaSubString: 'obras' },
  { id: 'tiamanhwa', name: 'Tia Manhwa', baseUrl: 'https://tiamanhwa.com', mangaSubString: 'manhwa' },
  { id: 'xxxyaoi', name: 'XXX Yaoi', baseUrl: 'https://3xyaoi.com', mangaSubString: 'bl' },
];

async function testCandidate(cand: { id: string; name: string; baseUrl: string; mangaSubString: string }) {
  const rateLimiter = new HostRateLimiter(2.0);
  const adapter = new MadaraAdapter({
    id: cand.id,
    name: cand.name,
    baseUrl: cand.baseUrl,
    mangaSubString: cand.mangaSubString,
  }, rateLimiter);

  console.log(`\nTesting ${cand.name} (${cand.baseUrl})...`);
  try {
    const { works } = await adapter.fetchUpdatedWorks(null, { mode: 'maintenance' });
    if (!works || works.length === 0) {
      console.log(`  ✗ [CATALOG FAIL] 0 works returned.`);
      return { id: cand.id, name: cand.name, status: 'FAIL', reason: '0 works in catalog' };
    }
    console.log(`  ✓ [CATALOG PASS] ${works.length} works. First: "${works[0].title}" (${works[0].slug})`);

    const sample = works[0];
    const details = await adapter.fetchWorkDetails(sample.sourceWorkId);
    console.log(`  ✓ [DETAILS PASS] Kind=${details.kind}, Cover=${!!details.coverUrl}`);

    const chapters = await adapter.fetchChapters(sample.sourceWorkId);
    if (!chapters || chapters.length === 0) {
      console.log(`  ✗ [CHAPTERS FAIL] 0 chapters found for ${sample.slug}`);
      return { id: cand.id, name: cand.name, status: 'FAIL', reason: '0 chapters found' };
    }
    console.log(`  ✓ [CHAPTERS PASS] ${chapters.length} chapters. First: Ch.${chapters[0].number} (${chapters[0].slug})`);

    const pages = await adapter.fetchChapterPages(chapters[0].sourceChapterId);
    if (!pages || pages.length === 0) {
      console.log(`  ✗ [PAGES FAIL] 0 pages extracted for chapter ${chapters[0].slug}`);
      return { id: cand.id, name: cand.name, status: 'FAIL', reason: '0 pages extracted' };
    }
    console.log(`  ✓ [PAGES PASS] ${pages.length} pages. First page URL: ${pages[0]}`);

    // Download first page to verify image bytes & anti-hotlink
    const imgRes = await fetch(pages[0], {
      headers: adapter.getImageHeaders ? adapter.getImageHeaders() : { Referer: `${cand.baseUrl}/` }
    });
    const buf = await imgRes.arrayBuffer();
    if (imgRes.status !== 200 || buf.byteLength < 500) {
      console.log(`  ✗ [DOWNLOAD FAIL] HTTP ${imgRes.status}, size=${buf.byteLength}`);
      return { id: cand.id, name: cand.name, status: 'FAIL', reason: `Download failed: HTTP ${imgRes.status}, size=${buf.byteLength}` };
    }
    console.log(`  ✓ [DOWNLOAD PASS] HTTP 200 OK, Content-Type=${imgRes.headers.get('content-type')}, Size=${buf.byteLength} bytes`);
    console.log(`  🌟 ${cand.name} is 100% CERTIFIED ACTIVE!`);
    return { id: cand.id, name: cand.name, status: 'PASS', pagesCount: pages.length, sampleSize: buf.byteLength };
  } catch (err: any) {
    console.log(`  ✗ [ERROR] ${err.message}`);
    return { id: cand.id, name: cand.name, status: 'ERROR', reason: err.message };
  }
}

async function main() {
  const results = [];
  for (const c of candidates) {
    results.push(await testCandidate(c));
  }
  console.log('\n========================================');
  console.log('SUMMARY RESULTS:');
  console.log('========================================');
  for (const r of results) {
    console.log(`${r.status === 'PASS' ? '✅' : '❌'} ${r.name} (${r.id}): ${r.status} ${r.reason || ''}`);
  }
}

main().catch(console.error);
