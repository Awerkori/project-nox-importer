import puppeteer from 'puppeteer-core';
import fs from 'fs';

const sql = fs.readFileSync('/home/awerkori/.Projects/project-nox-importer/migrations/20260914230000_queue_fairness.sql', 'utf8');

async function run() {
  console.log("Connecting to Chromium WS...");
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  
  let supabasePage = pages.find(p => p.url().includes('supabase.com'));
  if (!supabasePage) {
    console.log("Supabase tab not found, creating a new one...");
    supabasePage = await browser.newPage();
  } else {
    console.log("Found Supabase tab, bringing to front...");
    await supabasePage.bringToFront();
  }

  console.log("Navigating to SQL Editor...");
  await supabasePage.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { timeout: 10000 }).catch(e => console.log("goto timeout, continuing"));
  
  console.log("Waiting for editor...");
  await supabasePage.waitForSelector('.monaco-editor', { timeout: 30000 });
  await new Promise(r => setTimeout(r, 2000));

  console.log("Injecting SQL...");
  await supabasePage.evaluate(async (sqlText) => {
    const monaco = window.monaco;
    if (monaco && monaco.editor && monaco.editor.getModels().length > 0) {
      monaco.editor.getModels()[0].setValue(sqlText);
    }
  }, sql);
  
  await new Promise(r => setTimeout(r, 1000));

  console.log("Executing SQL...");
  await supabasePage.click('.monaco-editor');
  await new Promise(r => setTimeout(r, 500));
  await supabasePage.keyboard.down('Control');
  await supabasePage.keyboard.press('Enter');
  await supabasePage.keyboard.up('Control');
  
  console.log("Waiting for result...");
  await new Promise(r => setTimeout(r, 4000));
  
  console.log("Done.");
  await browser.disconnect();
}

run().catch(console.error);
