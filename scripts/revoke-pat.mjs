import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  const page = pages.find(p => p.url().includes('supabase.com'));
  
  if (!page) { console.log("No Supabase tab found"); return; }
  
  await page.goto('https://supabase.com/dashboard/account/tokens', { waitUntil: 'networkidle2' });
  await new Promise(r => setTimeout(r, 3000));
  
  // Find the token row and delete it
  await page.evaluate(() => {
    const buttons = Array.from(document.querySelectorAll('button'));
    const deleteButtons = buttons.filter(b => b.innerText.includes('Delete') || b.innerText.includes('Revoke'));
    if (deleteButtons.length > 0) {
      deleteButtons[0].click();
    }
  });
  
  await new Promise(r => setTimeout(r, 1000));
  
  // Confirm deletion if there's a modal
  await page.evaluate(() => {
    const buttons = Array.from(document.querySelectorAll('button'));
    const confirmButtons = buttons.filter(b => b.innerText.includes('Confirm') || b.innerText.includes('Delete token'));
    if (confirmButtons.length > 0) {
      confirmButtons[0].click();
    }
  });
  
  await new Promise(r => setTimeout(r, 2000));
  console.log("Attempted to revoke token via Supabase UI.");
  
  await browser.disconnect();
}
run().catch(console.error);
