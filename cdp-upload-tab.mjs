import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const pages = await browser.pages();
  const page = pages.find(p => p.url().includes('1788873398156') || p.url().includes('project-nox-importer'));
  
  if (!page) { console.log("No tab found"); return; }
  console.log("Using tab:", page.url());
  
  // Go to the main app dashboard tab (if it's in /activity, go to /)
  await page.goto('https://discloud.com/dashboard/app/1788873398156', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 2000));
  
  const fileInputs = await page.$$('input[type="file"]');
  console.log(`Found ${fileInputs.length} file inputs.`);
  
  if (fileInputs.length > 0) {
    console.log("Uploading zip...");
    await fileInputs[0].uploadFile('/home/awerkori/.Projects/project-nox-importer/dist-discloud/project-nox-importer.zip');
    await new Promise(r => setTimeout(r, 2000));
    await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const btn = buttons.find(b => b.textContent.toLowerCase().includes('commit') || b.textContent.toLowerCase().includes('upload') || b.textContent.toLowerCase().includes('deploy') || b.textContent.toLowerCase().includes('restart'));
      if (btn) btn.click();
    });
    console.log("Clicked upload/deploy!");
  } else {
    // maybe there's a button we need to click to show the file input
    console.log("No file input. Searching for Upload button to click...");
    await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button, a'));
      const btn = buttons.find(b => b.textContent.toLowerCase().includes('upload') || b.textContent.toLowerCase().includes('deploy') || b.textContent.toLowerCase().includes('atualizar'));
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
        const btn = buttons.find(b => b.textContent.toLowerCase().includes('confirm') || b.textContent.toLowerCase().includes('commit') || b.textContent.toLowerCase().includes('enviar') || b.textContent.toLowerCase().includes('deploy'));
        if (btn) btn.click();
      });
      console.log("Clicked confirm!");
    } else {
      console.log("Still no file input.");
      await page.screenshot({ path: '/home/awerkori/.Projects/project-nox-importer/discloud-app-2.png' });
    }
  }
  
  await browser.disconnect();
}
run().catch(console.error);
