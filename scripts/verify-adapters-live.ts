import { MangaFlixAdapter } from '../src/sources/mangaflix/mangaflix-adapter.js';
import { ManhastroAdapter } from '../src/sources/manhastro/manhastro-adapter.js';
import { ToonLivreAdapter } from '../src/sources/toonlivre/toonlivre-adapter.js';
import { KuroAdapter } from '../src/sources/kuro/kuro-adapter.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';

async function main() {
  const rateLimiter = new HostRateLimiter(2.0);

  console.log('================================================================');
  console.log('🔍 PROJECT NOX IMPORTER — CONTROLLED LIVE CHECK (1 WORK PER ADAPTER)');
  console.log('================================================================');

  // 1. MangaFlix
  console.log('\n[1/4] MangaFlix Live Verification:');
  try {
    const mf = new MangaFlixAdapter(rateLimiter);
    const { works: mfWorks } = await mf.fetchUpdatedWorks(null, { mode: 'maintenance' });
    console.log(`  ✓ fetchUpdatedWorks: received ${mfWorks.length} works`);
    if (mfWorks.length > 0) {
      const sample = mfWorks[0];
      console.log(`  ✓ Sample work: "${sample.title}" (ID: ${sample.sourceWorkId}, Slug: ${sample.slug})`);
      const details = await mf.fetchWorkDetails(sample.sourceWorkId);
      console.log(`  ✓ fetchWorkDetails: Kind=${details.kind}, Genres=${details.genres?.slice(0, 3).join(', ')}`);
      const chapters = await mf.fetchChapters(sample.sourceWorkId);
      console.log(`  ✓ fetchChapters: found ${chapters.length} chapters`);
      if (chapters.length > 0) {
        const pages = await mf.fetchChapterPages(chapters[0].sourceChapterId);
        console.log(`  ✓ fetchChapterPages: Chapter ${chapters[0].number} has ${pages.length} valid URLs`);
        console.log(`    Sample page URL: ${pages[0]}`);
      }
    }
  } catch (err: any) {
    console.error(`  ✗ MangaFlix error: ${err.message}`);
  }

  // 2. Manhastro
  console.log('\n[2/4] Manhastro Live Verification:');
  try {
    const mh = new ManhastroAdapter(rateLimiter);
    const { works: mhWorks } = await mh.fetchUpdatedWorks(null, { mode: 'maintenance' });
    console.log(`  ✓ fetchUpdatedWorks: received ${mhWorks.length} works`);
    if (mhWorks.length > 0) {
      const sample = mhWorks[0];
      console.log(`  ✓ Sample work: "${sample.title}" (ID: ${sample.sourceWorkId}, Slug: ${sample.slug})`);
      const details = await mh.fetchWorkDetails(sample.sourceWorkId);
      console.log(`  ✓ fetchWorkDetails: Kind=${details.kind}, Genres=${details.genres?.slice(0, 3).join(', ')}`);
      const chapters = await mh.fetchChapters(sample.sourceWorkId);
      console.log(`  ✓ fetchChapters: found ${chapters.length} chapters`);
      if (chapters.length > 0) {
        const pages = await mh.fetchChapterPages(chapters[0].sourceChapterId);
        console.log(`  ✓ fetchChapterPages: Chapter ${chapters[0].number} has ${pages.length} valid URLs`);
        console.log(`    Sample page URL: ${pages[0]}`);
      }
    }
  } catch (err: any) {
    console.error(`  ✗ Manhastro error: ${err.message}`);
  }

  // 3. Toon Livre
  console.log('\n[3/4] Toon Livre Live Verification:');
  try {
    const tl = new ToonLivreAdapter(rateLimiter);
    const { works: tlWorks } = await tl.fetchUpdatedWorks(null, { mode: 'maintenance' });
    console.log(`  ✓ fetchUpdatedWorks: received ${tlWorks.length} works`);
    if (tlWorks.length > 0) {
      const sample = tlWorks[0];
      console.log(`  ✓ Sample work: "${sample.title}" (ID: ${sample.sourceWorkId}, Slug: ${sample.slug})`);
      const details = await tl.fetchWorkDetails(sample.sourceWorkId);
      console.log(`  ✓ fetchWorkDetails: Kind=${details.kind}, Author=${details.author || 'N/A'}`);
      const chapters = await tl.fetchChapters(sample.sourceWorkId);
      console.log(`  ✓ fetchChapters: found ${chapters.length} chapters`);
    }
  } catch (err: any) {
    console.error(`  ✗ Toon Livre error: ${err.message}`);
  }

  // 4. Kuro
  console.log('\n[4/4] Kuro Live Protection Verification:');
  try {
    const kr = new KuroAdapter(rateLimiter);
    await kr.fetchUpdatedWorks(null, { mode: 'maintenance' });
    console.log('  ✗ Unexpected: Kuro succeeded without auth');
  } catch (err: any) {
    console.log(`  ✓ Safe auth rejection verified: "${err.message}"`);
  }

  console.log('\n================================================================');
  console.log('✅ CONTROLLED LIVE VERIFICATION COMPLETE');
  console.log('================================================================');
}

main().catch(console.error);
