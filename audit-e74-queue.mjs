import puppeteer from 'puppeteer-core';
import fs from 'fs';

async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  let supabasePage = pages.find(p => p.url().includes('supabase.com'));
  
  const token = await supabasePage.evaluate(() => window.localStorage.getItem('supabase.dashboard.auth.token'));
  if (!token) process.exit(1);
  const jwt = JSON.parse(token).access_token;
  
  const sql = `
  SELECT status, cancel_reason
  FROM public.importer_queue
  WHERE (payload->>'workId')::uuid = 'e74aa68d-149f-400f-b65f-c2535e04845b' AND coalesce(chapter_sort_key, (payload->>'chapterNumber')::numeric) = 102;
  `;
  
  const res = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
    body: JSON.stringify({ query: sql })
  });
  
  console.log(await res.text());
  await browser.disconnect();
}
run().catch(console.error);
