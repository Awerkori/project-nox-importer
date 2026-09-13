const gattsuSources = [
  { id: 'hentaiseason', name: 'Hentai Season', url: 'https://hentaiseason.com' },
  { id: 'hentaitokyo', name: 'Hentai Tokyo', url: 'https://hentaitokyo.net' },
  { id: 'universohentai', name: 'Universo Hentai', url: 'https://universohentai.com' },
];

async function testGattsu(s: any) {
  console.log(`\n=== Testing Gattsu: ${s.name} (${s.url}) ===`);
  try {
    const res = await fetch(s.url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Referer': `${s.url}/`,
      },
      signal: AbortSignal.timeout(7000)
    });
    if (!res.ok) {
      console.log(`  ✗ HTTP ${res.status}`);
      return false;
    }
    const html = await res.text();
    const matches = Array.from(html.matchAll(/<a[^>]+href=["'](https:\/\/[^"']+)["'][^>]*>[\s\S]*?<span class=["']thumb-titulo["']>([^<]+)<\/span>/gi));
    console.log(`  ✓ Found ${matches.length} works on homepage!`);
    if (matches.length === 0) {
      // try other regex
      const altMatches = Array.from(html.matchAll(/href=["'](https?:\/\/[^"']+\/[^"']+\/)["'][^>]*class=["']thumb["']/gi));
      console.log(`  (alt matches: ${altMatches.length})`);
      return false;
    }

    const firstUrl = matches[0][1];
    const firstTitle = matches[0][2].trim();
    console.log(`  ✓ Sample work: "${firstTitle}" -> ${firstUrl}`);

    // fetch work page
    const workRes = await fetch(firstUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Referer': `${s.url}/`,
      },
      signal: AbortSignal.timeout(7000)
    });
    const workHtml = await workRes.text();
    const imgMatches = Array.from(workHtml.matchAll(/<img[^>]+(?:data-src|src)=["']([^"']+)["'][^>]*class=["']wp-post-image["']|<ul class=["']post-fotos["']>[\s\S]*?<\/ul>/gi));
    
    // Check pages inside post-fotos or galeriaHtml
    const pageImgs = Array.from(workHtml.matchAll(/<img[^>]+(?:data-src|src)=["']([^"']+)["']/gi))
      .map(m => m[1])
      .filter(u => u.includes('wp-content/uploads') && !u.includes('logo') && !u.includes('avatar'));
    
    console.log(`  ✓ Extracted ${pageImgs.length} chapter/gallery images!`);
    if (pageImgs.length === 0) {
      return false;
    }

    // download first image
    const imgUrl = pageImgs[0];
    const imgRes = await fetch(imgUrl, {
      headers: { Referer: firstUrl },
      signal: AbortSignal.timeout(7000)
    });
    const buf = await imgRes.arrayBuffer();
    if (imgRes.status === 200 && buf.byteLength > 1000) {
      console.log(`  ✓ Image Download PASS: ${buf.byteLength} bytes (${imgRes.headers.get('content-type')})`);
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
  for (const s of gattsuSources) {
    await testGattsu(s);
  }
}

main().catch(console.error);
