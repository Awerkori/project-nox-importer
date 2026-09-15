import puppeteer from 'puppeteer-core';
async function run() {
  const res = await fetch('http://127.0.0.1:9222/json/version');
  const { webSocketDebuggerUrl } = await res.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 5000));
  
  const result = await page.evaluate(async () => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    const jwt = JSON.parse(token).access_token;
    
    // Pick a test work
    const wdata = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: `SELECT id, latest_chapter_published_at FROM works WHERE published = true LIMIT 1;` })
    }).then(r => r.json());
    
    if (wdata.error || !wdata[0]) return { error: wdata.error || 'No works found' };
    
    const workId = wdata[0].id;
    const oldLatest = wdata[0].latest_chapter_published_at;
    
    // 1. Insert a new chapter
    const t0 = performance.now();
    await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: `
        INSERT INTO chapters (work_id, number, title, published_at, origin)
        VALUES ('${workId}', 9999, 'Test Chapter', NOW(), 'import')
        RETURNING id;
      ` })
    });
    const t1 = performance.now();
    
    // 2. Check the new latest_chapter_published_at
    const wdata2 = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: `SELECT latest_chapter_published_at FROM works WHERE id = '${workId}';` })
    }).then(r => r.json());
    const newLatest = wdata2[0].latest_chapter_published_at;
    
    // 3. Delete the chapter
    const t2 = performance.now();
    await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: `DELETE FROM chapters WHERE work_id = '${workId}' AND number = 9999;` })
    });
    const t3 = performance.now();
    
    // 4. Check the latest_chapter_published_at again
    const wdata3 = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: `SELECT latest_chapter_published_at FROM works WHERE id = '${workId}';` })
    }).then(r => r.json());
    const restoredLatest = wdata3[0].latest_chapter_published_at;

    return {
      workId,
      oldLatest,
      newLatest,
      restoredLatest,
      insertTimeMs: (t1 - t0).toFixed(2),
      deleteTimeMs: (t3 - t2).toFixed(2)
    };
  });
  
  console.log(JSON.stringify(result, null, 2));
  await browser.disconnect();
}
run().catch(console.error);
