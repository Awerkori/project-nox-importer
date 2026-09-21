import puppeteer from 'puppeteer-core';

const PUPPETEER_OPTS = {
  executablePath: '/usr/bin/chromium',
  headless: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--ignore-certificate-errors']
};

const DISCLOUD_COOKIE = {
  name: 'session_id',
  value: '4bc8293a4be7d5e98fe447b89d6d3bb0e12897b1b421820829d4ee8127348f2404af1f927eea5a62c186f98bc11ef7676a9f9a68',
  domain: '.discloud.com',
  path: '/'
};

async function main() {
  console.log('Launching browser to start/verify Discloud app...');
  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  try {
    const page = await browser.newPage();
    await page.setCookie(DISCLOUD_COOKIE);

    console.log('Navigating to Discloud app page...');
    await page.goto('https://discloud.com/dashboard/app/1788873398156', { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));

    const statusText = await page.evaluate(() => document.body.innerText);
    const isOffline = statusText.includes('Offline');
    console.log('Status isOffline:', isOffline);

    const clicked = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const startBtn = buttons.find(b => {
        const t = b.textContent.trim().toLowerCase();
        return t === 'iniciar' || t.includes('iniciar');
      });
      if (startBtn) {
        startBtn.click();
        return 'iniciar';
      }
      const restartBtn = buttons.find(b => {
        const t = b.textContent.trim().toLowerCase();
        return t === 'reiniciar' || t.includes('reiniciar');
      });
      if (restartBtn) {
        restartBtn.click();
        return 'reiniciar';
      }
      return null;
    });
    console.log('Action clicked:', clicked);

    console.log('Waiting 15s for container to boot...');
    await new Promise(r => setTimeout(r, 15000));

    // Check logs
    for (let attempt = 1; attempt <= 10; attempt++) {
      console.log(`\nChecking logs (attempt ${attempt}/10)...`);
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

      const running = recent.some(l => l.includes('shared chapter runner pool (18 slots') || l.includes('Reconciler loop started') || l.includes('Publication Safety Barrier'));
      if (running) {
        console.log('\n Container is running with active workers!');
        break;
      }
      await new Promise(r => setTimeout(r, 5000));
    }
  } finally {
    await browser.close();
  }
}

main().catch(console.error);
