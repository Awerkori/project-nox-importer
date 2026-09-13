const adultSources = [
  { id: 'bryaoi', url: 'https://bryaoi.com' },
  { id: 'hentaifusion', url: 'https://hentaifusion.me' },
  { id: 'hentaihome', url: 'https://www.hentaihome.net' },
  { id: 'horahentai', url: 'https://horahentai.com' },
  { id: 'mundohentai', url: 'https://mundohentaioficial.com' },
  { id: 'muitohentai', url: 'https://www.muitohentai.com' },
  { id: 'nhentaibr', url: 'https://nhentai.net.br' },
  { id: 'sexkomix', url: 'https://sexkomix2.com' },
  { id: 'thehentai', url: 'https://thehentai.net' },
  { id: 'terceiroz', url: 'https://terceiroz.com' },
  { id: 'zettahq', url: 'https://zettahq.com' },
  { id: 'exhentainetbr', url: 'https://exhentai.net.br' },
  { id: 'brasilhentai', url: 'https://brasilhentai.com' },
];

async function checkOne(s: any) {
  try {
    const res = await fetch(s.url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
      signal: AbortSignal.timeout(6000)
    });
    if (!res.ok) {
      console.log(`${s.id}: HTTP ${res.status}`);
      return;
    }
    const html = await res.text();
    const isWp = html.includes('wp-content');
    const isMadara = html.includes('wp-manga') || html.includes('madara');
    const wpJson = await fetch(`${s.url}/wp-json`, { signal: AbortSignal.timeout(4000) }).catch(() => null);
    const hasWpMangaV1 = wpJson?.ok ? (await wpJson.text().catch(() => '')).includes('wp-manga') : false;
    console.log(`${s.id.padEnd(16)} | WP: ${isWp} | Madara: ${isMadara} | wp-manga/v1: ${hasWpMangaV1}`);
  } catch (err: any) {
    console.log(`${s.id.padEnd(16)} | ERR: ${err.message}`);
  }
}

async function main() {
  for (const s of adultSources) {
    await checkOne(s);
  }
}
main().catch(console.error);
