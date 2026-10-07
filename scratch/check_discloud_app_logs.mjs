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
  console.log('Connecting to Discloud dashboard...');
  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  try {
    const page = await browser.newPage();
    await page.setCookie(DISCLOUD_COOKIE);

    console.log('Fetching app overview...');
    await page.goto('https://discloud.com/dashboard/app/1788873398156', { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));

    const overviewText = await page.evaluate(() => document.body.innerText);
    const overviewLines = overviewText.split('\n').map(l => l.trim()).filter(Boolean);
    console.log('=== APP OVERVIEW ===');
    const relevantLines = overviewLines.filter(l => 
      l.includes('Online') || l.includes('Offline') || l.includes('Uptime') || 
      l.includes('CPU') || l.includes('RAM') || l.includes('TITAN') || 
      l.includes('Restart') || l.includes('Status')
    );
    console.log(relevantLines);

    console.log('\nFetching logs...');
    await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded', timeout: 30000 });
    await new Promise(r => setTimeout(r, 4000));

    const logs = await page.evaluate(() => {
      const el = document.querySelector('pre, code, .logs, .terminal') || document.body;
      return el.innerText;
    });

    const lines = logs.split('\n').map(l => l.trim()).filter(Boolean);
    console.log(`Total log lines captured: ${lines.length}`);
    console.log('\n=== LAST 60 LOG LINES ===');
    lines.slice(-60).forEach(l => console.log(l));

  } finally {
    await browser.close();
  }
}

main().catch(console.error);
