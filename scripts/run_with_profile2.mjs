import puppeteer from 'puppeteer-core';
import fs from 'fs';
import { FIX_RPC_SQL } from '../build/fix_rpc.js';

async function run() {
  const browser = await puppeteer.launch({
    executablePath: '/usr/bin/chromium',
    userDataDir: '/home/awerkori/.config/chromium',
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' }).catch(e => console.log('Goto timeout ignored'));
  
  await new Promise(r => setTimeout(r, 5000));
  
  const res = await page.evaluate(async (query) => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    if (!token) return { status: 'No token' };
    const jwt = JSON.parse(token).access_token;
    
    // Apply RPC FIX
    const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query })
    });
    
    // Reload PostgREST schema cache just in case!
    await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query: "NOTIFY pgrst, 'reload schema';" })
    });

    return { status: response.status, body: await response.text() };
  }, FIX_RPC_SQL);
  
  console.log('Result:', res);
  await browser.close();
}
run().catch(console.error);
