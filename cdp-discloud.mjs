import puppeteer from 'puppeteer-core';
async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  
  console.log("Navigating to DIScloud...");
  await page.goto('https://discloudbot.com/dashboard/apps/project-nox-importer', { waitUntil: 'networkidle2', timeout: 30000 }).catch(e => console.log("Navigation timeout but continuing..."));
  
  await new Promise(r => setTimeout(r, 5000));
  
  console.log("Looking for file input to upload ZIP...");
  const fileInput = await page.$('input[type="file"]');
  if (fileInput) {
    console.log("Found file input, uploading zip...");
    await fileInput.uploadFile('/home/awerkori/.Projects/project-nox-importer/dist-discloud/project-nox-importer.zip');
    
    await new Promise(r => setTimeout(r, 3000));
    
    console.log("Clicking Confirm/Deploy button...");
    await page.evaluate(() => {
      const btns = Array.from(document.querySelectorAll('button'));
      const deployBtn = btns.find(b => 
        b.textContent.toLowerCase().includes('commit') || 
        b.textContent.toLowerCase().includes('deploy') || 
        b.textContent.toLowerCase().includes('enviar') || 
        b.textContent.toLowerCase().includes('upload') ||
        b.textContent.toLowerCase().includes('atualizar')
      );
      if (deployBtn) deployBtn.click();
    });
    
    console.log("Waiting for upload/restart...");
    await new Promise(r => setTimeout(r, 10000));
  } else {
    console.log("No file input found!");
    await page.screenshot({ path: 'discloud-error.png' });
  }
  
  console.log("Done.");
  await browser.disconnect();
}
run().catch(console.error);
