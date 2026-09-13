import { PointZeroToonsAdapter } from '../src/sources/pointzerotoons/pointzerotoons-adapter.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';

async function test() {
  const ad = new PointZeroToonsAdapter(new HostRateLimiter(2.0));
  const { works } = await ad.fetchUpdatedWorks(null, { mode: 'maintenance' });
  console.log('Works count:', works.length);
  if (works.length > 0) {
    console.log('Sample work:', works[0]);
    const details = await ad.fetchWorkDetails(works[0].sourceWorkId);
    console.log('Details:', details.title, 'genres:', details.genres);
    const chaps = await ad.fetchChapters(works[0].sourceWorkId);
    console.log('Chapters count:', chaps.length);
    if (chaps.length > 0) {
      console.log('Sample chapter:', chaps[0]);
      const pages = await ad.fetchChapterPages(chaps[0].sourceChapterId);
      console.log('Pages count:', pages.length);
      if (pages.length > 0) {
        console.log('First page URL:', pages[0]);
        const imgRes = await fetch(pages[0], { headers: ad.getImageHeaders() });
        const buf = await imgRes.arrayBuffer();
        console.log('Image download:', imgRes.status, imgRes.headers.get('content-type'), buf.byteLength, 'bytes');
        if (imgRes.status === 200 && buf.byteLength > 500) {
          console.log('🌟 PointZeroToonsAdapter is 100% CERTIFIED ACTIVE!');
        }
      }
    }
  }
}

test().catch(console.error);
