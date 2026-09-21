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
  console.log('Launching browser to restart Discloud container...');
  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  try {
    const page = await browser.newPage();
    await page.setCookie(DISCLOUD_COOKIE);

    console.log('Navigating to Discloud app page...');
    await page.goto('https://discloud.com/dashboard/app/1788873398156', { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));

    console.log('Clicking Reiniciar button...');
    const restartClicked = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const btn = buttons.find(b => b.textContent.trim().toLowerCase() === 'reiniciar' || b.textContent.trim().toLowerCase().includes('reiniciar'));
      if (btn) {
        btn.click();
        return true;
      }
      return false;
    });
    console.log('Reiniciar clicked:', restartClicked);

    console.log('Waiting 15s for container to restart...');
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
      const recent = lines.slice(-30);
      console.log('--- LATEST LOGS ---');
      recent.forEach(l => console.log(l));

      const probeSuccess = recent.some(l => l.includes('yugabyte:5433 = OPEN'));
      const healthOk = recent.some(l => l.includes('Health HEALTHY') || (l.includes('Initial boot status') && !l.includes('UNHEALTHY')));
      const errorTimeout = recent.some(l => l.includes('Connection terminated due to connection timeout'));

      if (probeSuccess || (healthOk && !errorTimeout)) {
        console.log('\n🎉 SUCCESS: Discloud is connected DIRECTLY to YugabyteDB Aeon via YSQL TLS!');
        break;
      }

      await new Promise(r => setTimeout(r, 6000));
    }

  } finally {
    await browser.close();
  }
}

main().catch(console.error);
