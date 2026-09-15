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
  if (pages.length > 0) {
    console.log("First URL:", pages[0]);
    console.log("Fetching first page...");
    const res = await fetch(pages[0], { headers: { referer: 'https://mangaonline.tv/' } });
    console.log("Status:", res.status);
    console.log("Bytes:", (await res.arrayBuffer()).byteLength);
  }
}
run();
