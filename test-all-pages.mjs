import { MadaraAdapter } from './build/sources/common/madara-adapter.js';

const adapter = new MadaraAdapter({
  id: 'mangaonlinetv',
  name: 'Manga Online TV',
  baseUrl: 'https://mangaonline.tv',
  rateLimit: { maxRequestsPerMinute: 60 }
});

async function run() {
  const pages = await adapter.fetchChapterPages('https://mangaonline.tv/manga/one-piece/capitulo-51/');
  console.log("Pages found:", pages.length);
  for (let i = 0; i < pages.length; i++) {
    const res = await fetch(pages[i], { headers: { referer: 'https://mangaonline.tv/' } });
    console.log(`Page ${i + 1}: ${res.status} ${res.headers.get('content-type')}`);
  }
}
run();
