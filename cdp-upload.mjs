import puppeteer from 'puppeteer-core';

async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  
  await page.setCookie({
    name: 'session_id',
    value: '1a700b6f1cb770ac16ee8af488fbc756a6d2e035b31e15e338a728de4c6e97db087c0784150598bc59a473ca4b1a525e6aa87ae7',
    domain: 'discloudbot.com'
  });
  
  console.log("Navigating to dashboard...");
  await page.goto('https://discloudbot.com/dashboard/apps/project-nox-importer', { waitUntil: 'domcontentloaded' });
  
  const title = await page.title();
  console.log("Page title:", title);
  
  // Try to find file input
  const fileInputs = await page.$$('input[type="file"]');
  console.log(`Found ${fileInputs.length} file inputs.`);
  
  if (fileInputs.length > 0) {
    console.log("Uploading file...");
    await fileInputs[0].uploadFile('/home/awerkori/.Projects/project-nox-importer/dist-discloud/project-nox-importer.zip');
    await new Promise(r => setTimeout(r, 2000));
    
    // Find commit/upload/deploy button
    await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const btn = buttons.find(b => b.textContent.toLowerCase().includes('commit') || b.textContent.toLowerCase().includes('upload') || b.textContent.toLowerCase().includes('deploy') || b.textContent.toLowerCase().includes('restart'));
      if (btn) btn.click();
    });
    
    console.log("Clicked! Waiting 15s...");
    await new Promise(r => setTimeout(r, 15000));
  } else {
    // If no file input, maybe we need to click "Upload" first to open a modal?
    console.log("Looking for an upload button to click first...");
    await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button, a'));
      const btn = buttons.find(b => b.textContent.toLowerCase().includes('upload') || b.textContent.toLowerCase().includes('deploy'));
      if (btn) btn.click();
    });
    await new Promise(r => setTimeout(r, 2000));
    const fi = await page.$$('input[type="file"]');
    if (fi.length > 0) {
      console.log("Found file input after click!");
      await fi[0].uploadFile('/home/awerkori/.Projects/project-nox-importer/dist-discloud/project-nox-importer.zip');
      await new Promise(r => setTimeout(r, 2000));
      await page.evaluate(() => {
        const buttons = Array.from(document.querySelectorAll('button'));
        const btn = buttons.find(b => b.textContent.toLowerCase().includes('confirm') || b.textContent.toLowerCase().includes('commit'));
        if (btn) btn.click();
      });
      console.log("Clicked confirm! Waiting 15s...");
      await new Promise(r => setTimeout(r, 15000));
    } else {
      console.log("Still no file input.");
    }
  }
  
  await browser.disconnect();
}
run().catch(console.error);
