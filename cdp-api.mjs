import puppeteer from 'puppeteer-core';
import fs from 'fs';
const sql = fs.readFileSync('/home/awerkori/.Projects/project-nox-importer/migrations/20260914230000_queue_fairness.sql', 'utf8');

async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  let supabasePage = pages.find(p => p.url().includes('supabase.com'));
  
  // Extract token from localStorage
  const token = await supabasePage.evaluate(() => {
    return window.localStorage.getItem('supabase.dashboard.auth.token');
  });
  
  if (!token) {
    console.log("Token not found in localStorage!");
    process.exit(1);
  }
  
  const parsedToken = JSON.parse(token);
  const jwt = parsedToken.access_token;
  
  console.log("Executing SQL via API...");
  const res = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${jwt}`
    },
    body: JSON.stringify({ query: sql })
  });
  
  const resData = await res.text();
  console.log("API Response:");
  console.log(resData);
  
  await browser.disconnect();
}
run().catch(console.error);
