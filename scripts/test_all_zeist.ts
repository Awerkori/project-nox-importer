const zeistSources = [
  { id: 'osakascan', name: 'Osaka Scan', url: 'https://www.osakascan.com' },
  { id: 'galaxscanlator', name: 'GALAX Scans', url: 'https://galaxscanlator.blogspot.com' },
  { id: 'apenasumafa', name: 'Apenas Uma Fã', url: 'https://apenasuma-fa.blogspot.com' },
  { id: 'ler999', name: 'Ler 999', url: 'https://ler999.blogspot.com' },
  { id: 'pinkrosa', name: 'Pink Rosa', url: 'https://scanpinkrosa.blogspot.com' },
  { id: 'temakimangas', name: 'Temaki Mangás', url: 'https://temakimangas.blogspot.com' },
  { id: 'traducoesdolipe', name: 'Traduções do Lipe', url: 'https://traducoesdolipe.blogspot.com' },
  { id: 'hanmokkuscan', name: 'Hanmokku Scan', url: 'https://hanmokkuscan.blogspot.com' },
];

async function testOne(s: any) {
  console.log(`\n=== Testing ${s.name} (${s.url}) ===`);
  try {
    // 1. Fetch series feed
    const seriesUrl = `${s.url}/feeds/posts/default/-/Series?alt=json&max-results=5`;
    const resSeries = await fetch(seriesUrl, { signal: AbortSignal.timeout(6000) });
    if (!resSeries.ok) {
      console.log(`  ✗ Series feed HTTP ${resSeries.status}`);
      return false;
    }
    const seriesData = await resSeries.json();
    const series = seriesData?.feed?.entry || [];
    console.log(`  ✓ Series count: ${series.length}`);
    if (series.length === 0) {
      // Check latest posts
      const latestUrl = `${s.url}/feeds/posts/default?alt=json&max-results=5`;
      const resLatest = await fetch(latestUrl, { signal: AbortSignal.timeout(6000) });
      const latestData = await resLatest.json();
      console.log(`  (fallback latest posts: ${latestData?.feed?.entry?.length || 0})`);
      return false;
    }

    const firstSeries = series[0];
    const seriesTitle = firstSeries.title?.['$t'];
    console.log(`  ✓ First series: "${seriesTitle}"`);

    // 2. Fetch recent chapter posts
    const chUrl = `${s.url}/feeds/posts/default?alt=json&max-results=10`;
    const resCh = await fetch(chUrl, { signal: AbortSignal.timeout(6000) });
    const chData = await resCh.json();
    const posts = chData?.feed?.entry || [];
    
    let foundChapter = null;
    let foundImages: string[] = [];

    for (const p of posts) {
      const pTitle = p.title?.['$t'] || '';
      const content = p.content?.['$t'] || '';
      const imgs = Array.from(content.matchAll(/<img[^>]+src=["']([^"']+)["']/gi)).map(m => (m as any)[1])
        .filter(u => !u.includes('capa-oculta') && (u.includes('blogger.googleusercontent.com') || u.includes('.bp.blogspot.com') || u.includes('imgur.com') || u.includes('.jpg') || u.includes('.png') || u.includes('.webp')));
      
      if (imgs.length > 2) {
        foundChapter = pTitle;
        foundImages = imgs;
        break;
      }
    }

    if (!foundChapter || foundImages.length === 0) {
      console.log(`  ✗ Could not find chapter post with >2 images in latest 10 posts`);
      return false;
    }

    console.log(`  ✓ Sample chapter post: "${foundChapter}" with ${foundImages.length} images`);
    console.log(`  ✓ First image: ${foundImages[0]}`);

    // 3. Download test
    const imgRes = await fetch(foundImages[0], {
      headers: { Referer: `${s.url}/` },
      signal: AbortSignal.timeout(8000)
    });
    const buf = await imgRes.arrayBuffer();
    if (imgRes.status === 200 && buf.byteLength > 1000) {
      console.log(`  ✓ Download PASS: ${buf.byteLength} bytes (${imgRes.headers.get('content-type')})`);
      console.log(`  🌟 ${s.name} is FULLY VERIFIED!`);
      return true;
    } else {
      console.log(`  ✗ Download failed: HTTP ${imgRes.status}, size: ${buf.byteLength}`);
      return false;
    }
  } catch (err: any) {
    console.log(`  ✗ ERROR: ${err.message}`);
    return false;
  }
}

async function main() {
  const verified = [];
  for (const s of zeistSources) {
    const ok = await testOne(s);
    if (ok) verified.push(s);
  }
  console.log(`\n========================================`);
  console.log(`Total verified ZeistManga sources: ${verified.length} / ${zeistSources.length}`);
  for (const v of verified) {
    console.log(`  ✅ ${v.id}: ${v.name} (${v.url})`);
  }
}

main().catch(console.error);
