import puppeteer from 'puppeteer-core';

const PUPPETEER_OPTS = {
  executablePath: '/usr/bin/chromium',
  headless: true,
  ignoreHTTPSErrors: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--ignore-certificate-errors']
};

const DISCLOUD_COOKIE = {
  name: 'session_id',
  value: '4bc8293a4be7d5e98fe447b89d6d3bb0e12897b1b421820829d4ee8127348f2404af1f927eea5a62c186f98bc11ef7676a9f9a68',
  domain: '.discloud.com',
  path: '/'
};

async function main() {
  console.log('Launching browser to trigger Rebuild on Discloud...');
  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  try {
    const page = await browser.newPage();
    await page.setCookie(DISCLOUD_COOKIE);

    console.log('Navigating to Discloud app page...');
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        await page.goto('https://discloud.com/dashboard/app/1788873398156', { waitUntil: 'networkidle2', timeout: 30000 });
        break;
      } catch (navErr) {
        console.warn(`Attempt ${attempt} navigation failed: ${navErr.message}`);
        if (attempt === 3) throw navErr;
        await new Promise(r => setTimeout(r, 2000));
      }
    }
    await new Promise(r => setTimeout(r, 3000));

    // Check status
    const statusText = await page.evaluate(() => document.body.innerText);
    console.log('Status snippet:', statusText.split('\n').filter(l => l.includes('Online') || l.includes('Offline') || l.includes('TITAN')));

    console.log('Clicking Rebuild button...');
    const rebuildClicked = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const btn = buttons.find(b => b.textContent.trim().toLowerCase() === 'rebuild' || b.textContent.trim().toLowerCase().includes('rebuild'));
      if (btn) {
        btn.click();
        return true;
      }
      return false;
    });
    console.log('Main Rebuild button clicked:', rebuildClicked);

    await new Promise(r => setTimeout(r, 1500));
    console.log('Clicking Rebuild confirmation button in modal...');
    const confirmed = await page.evaluate(() => {
      const modal = document.querySelector('.v4modal-panel, [role="dialog"], .modal');
      if (modal) {
        const modalBtns = Array.from(modal.querySelectorAll('button'));
        const confirmBtn = modalBtns.find(b => b.innerText.trim().toLowerCase() === 'rebuild');
        if (confirmBtn) {
          confirmBtn.click();
          return true;
        }
      }
      return false;
    });
    console.log('Rebuild modal confirmation clicked:', confirmed);

    console.log('Waiting for container to pull latest commit and env, and rebuild (35s)...');
    await new Promise(r => setTimeout(r, 35000));

    // Check logs up to 12 times
    for (let attempt = 1; attempt <= 12; attempt++) {
      console.log(`\nChecking logs (attempt ${attempt}/12)...`);
      await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded', timeout: 30000 });
      await new Promise(r => setTimeout(r, 4000));

      const logs = await page.evaluate(() => {
        const el = document.querySelector('pre, code, .logs, .terminal') || document.body;
        return el.innerText;
      });

      const lines = logs.split('\n').map(l => l.trim()).filter(Boolean);
      const recent = lines.slice(-25);
      console.log('--- LATEST LOGS ---');
      recent.forEach(l => console.log(l));

      const directModeLogged = recent.some(l => l.includes('IMPORTER DATABASE MODE: DIRECT'));
      const runnerPool18Logged = recent.some(l => l.includes('shared chapter runner pool (18 slots'));
      const fatalError = recent.some(l => l.includes('Fatal initialization error') || l.includes('TypeError'));

      if (directModeLogged && runnerPool18Logged && !fatalError) {
        console.log('\n🎉 SUCCESS: Direct Yugabyte mode is active with 18 SLOTS on Discloud!');
        break;
      }

      await new Promise(r => setTimeout(r, 6000));
    }

  } finally {
    await browser.close();
  }
}

main().catch(console.error);
