import { ApeComicsAdapter } from '../src/sources/apecomics/apecomics-adapter.js';
import { PizzariaScanAdapter } from '../src/sources/pizzariascan/pizzariascan-adapter.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';

async function testSource(adapter: any, name: string) {
  console.log(`\nTesting ${name}...`);
  try {
    const { works } = await adapter.fetchUpdatedWorks(null, { mode: 'maintenance' });
    console.log(`  ✓ Works count: ${works.length}`);
    if (works.length > 0) {
      console.log(`  ✓ Sample work: "${works[0].title}" (${works[0].slug})`);
      const details = await adapter.fetchWorkDetails(works[0].sourceWorkId);
      console.log(`  ✓ Details: Kind=${details.kind}, Cover=${!!details.coverUrl}`);
      const chapters = await adapter.fetchChapters(works[0].sourceWorkId);
      console.log(`  ✓ Chapters count: ${chapters.length}`);
      if (chapters.length > 0) {
        console.log(`  ✓ Sample chapter: Ch.${chapters[0].number}`);
        const pages = await adapter.fetchChapterPages(chapters[0].sourceChapterId);
        console.log(`  ✓ Pages count: ${pages.length}`);
        if (pages.length > 0) {
          console.log(`  ✓ Sample page URL: ${pages[0]}`);
          const imgRes = await fetch(pages[0], { headers: adapter.getImageHeaders() });
          const buf = await imgRes.arrayBuffer();
          console.log(`  ✓ Image download: HTTP ${imgRes.status}, ${imgRes.headers.get('content-type')}, ${buf.byteLength} bytes`);
          if (imgRes.status === 200 && buf.byteLength > 500) {
            console.log(`  🌟 ${name} is 100% CERTIFIED ACTIVE!`);
          }
        }
      }
    }
  } catch (err: any) {
    console.error(`  ✗ ${name} error:`, err.message);
  }
}

async function main() {
  const rateLimiter = new HostRateLimiter(2.0);
  await testSource(new ApeComicsAdapter(rateLimiter), 'Capitoons (Ape Comics)');
  await testSource(new PizzariaScanAdapter(rateLimiter), 'Pizzaria Scan');
}

main().catch(console.error);
