const zeistSources = [
  { id: 'osakascan', url: 'https://www.osakascan.com' },
  { id: 'temakimangas', url: 'https://temakimangas.blogspot.com' },
  { id: 'traducoesdolipe', url: 'https://traducoesdolipe.blogspot.com' },
  { id: 'galaxscanlator', url: 'https://galaxscanlator.blogspot.com' },
  { id: 'apenasumafa', url: 'https://apenasuma-fa.blogspot.com' },
  { id: 'ler999', url: 'https://ler999.blogspot.com' },
  { id: 'pinkrosa', url: 'https://scanpinkrosa.blogspot.com' },
  { id: 'hanmokkuscan', url: 'https://hanmokkuscan.blogspot.com' }
];

async function testZeist(s: any) {
  const feedUrl = `${s.url}/feeds/posts/default/-/Series?alt=json&max-results=5`;
  try {
    const res = await fetch(feedUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      },
      signal: AbortSignal.timeout(6000)
    });
    if (!res.ok) {
      console.log(`${s.id}: HTTP ${res.status}`);
      return;
    }
    const data = await res.json();
    const entries = data?.feed?.entry || [];
    const title = entries[0]?.title?.['$t'] || 'no title';
    console.log(`${s.id}: [PASS] ${entries.length} series in feed! First: "${title}"`);
  } catch (err: any) {
    console.log(`${s.id}: [FAIL] ${err.message}`);
  }
}

async function run() {
  for (const s of zeistSources) {
    await testZeist(s);
  }
}
run();
