import puppeteer from 'puppeteer-core';
import fs from 'fs';

const sql = fs.readFileSync('/home/awerkori/.Projects/project-nox-importer/migrations/20260914230000_queue_fairness.sql', 'utf8');

async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  
  console.log("Connecting...");
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  
  let supabasePage = pages.find(p => p.url().includes('supabase.com'));
  if (!supabasePage) {
    supabasePage = await browser.newPage();
  }
  
  await supabasePage.bringToFront();
  
  console.log("Navigating...");
  await supabasePage.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded', timeout: 15000 }).catch(() => {});
  
  await new Promise(r => setTimeout(r, 5000));
  await supabasePage.screenshot({path: 'step1-loaded.png'});
  
  console.log("Injecting SQL...");
  await supabasePage.evaluate((sqlText) => {
    const monaco = window.monaco;
    if (monaco && monaco.editor && monaco.editor.getModels().length > 0) {
      monaco.editor.getModels()[0].setValue(sqlText);
    }
  }, sql);
  
  await new Promise(r => setTimeout(r, 2000));
  await supabasePage.screenshot({path: 'step2-injected.png'});
  
  console.log("Clicking Run...");
  await supabasePage.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    const runBtn = btns.find(b => b.textContent === 'Run' || b.textContent.includes('Run'));
    if (runBtn) runBtn.click();
  });
  
  await new Promise(r => setTimeout(r, 5000));
  await supabasePage.screenshot({path: 'step3-executed.png'});
  
  console.log("Done.");
  await browser.disconnect();
}
run().catch(console.error);
