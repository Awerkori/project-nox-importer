import puppeteer from 'puppeteer-core';

const PUPPETEER_OPTS = {
  executablePath: '/usr/bin/chromium',
  headless: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
};

const DISCLOUD_COOKIE = {
  name: 'session_id',
  value: '4bc8293a4be7d5e98fe447b89d6d3bb0e12897b1b421820829d4ee8127348f2404af1f927eea5a62c186f98bc11ef7676a9f9a68',
  domain: '.discloud.com',
  path: '/'
};

async function main() {
  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  try {
    const page = await browser.newPage();
    await page.setCookie(DISCLOUD_COOKIE);
    
    // Check main app page for status/CPU/RAM
    console.log('Navigating to Discloud app page...');
    await page.goto('https://discloud.com/dashboard/app/1788873398156', { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 4000));
    
    const pageText = await page.evaluate(() => document.body.innerText);
    console.log('--- DISCLOUD APP PAGE TEXT SNIPPETS ---');
    const lines = pageText.split('\n').map(l => l.trim()).filter(Boolean);
    // Find status, RAM, CPU lines
    lines.slice(0, 40).forEach(l => console.log('  ', l));

    // Now check logs
    console.log('\nNavigating to Discloud logs page...');
    await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded', timeout: 30000 });
    await new Promise(r => setTimeout(r, 6000));
    const logs = await page.evaluate(() => {
      const el = document.querySelector('pre, code, .logs, .terminal') || document.body;
      return el.innerText;
    });
    console.log('--- RECENT DISCLOUD LOGS ---');
    const logLines = logs.split('\n').filter(l => l.trim()).slice(-40);
    logLines.forEach(l => console.log(l));

  } finally {
    await browser.close();
  }
}

main().catch(console.error);
