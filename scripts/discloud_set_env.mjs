import puppeteer from 'puppeteer-core';

const ENV_CONTENT = `NODE_ENV=production
STORAGE_PROVIDER=direct_telegram
NOX_IMPORTER_GATEWAY_URL=https://project-nox-importer-gateway.project-nox-awerkori.workers.dev
UPLOAD_RATE_LIMIT_BYTES_PER_SEC=50000000
MAX_CONCURRENT_CHAPTERS=5
BATCH_PAGE_DOWNLOAD_CONCURRENCY=8
POLL_INTERVAL_SECONDS=5
QUEUE_LEASE_DURATION_SECONDS=300
QUEUE_HEARTBEAT_INTERVAL_SECONDS=20
WORKER_ID=discloud-importer-1
LOG_LEVEL=info
NOX_STORAGE_BRIDGE_TOKEN=97c17bd54b085c5cace7f415f31644baa3f935018b7ad8568f1947eddf6cf50cae2d64bc71507bf3901d3db808b4cecb9819870457482f638f242087624d9959
NOX_MANGA_URL=https://manga.project-nox-awerkori.workers.dev
KURO_EMAIL=awerkorilinux@gmail.com
KURO_PASSWORD=Isaque221
KURO_SESSION=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOjE5NjExLCJyb2xlIjoidXNlciIsInNlc3Npb25WZXJzaW9uIjoxMSwiaWF0IjoxNzg5OTg2MzA2LCJleHAiOjE3OTAwMTkxOTh9.IC-Azt3rnU4fK6zLyUL_SczkFQIB2w5lcqRAfR41DwQ
KURO_CLIENT_TOKEN=4e1de216d265f648dff22cb4ea4860bdf30ce6fbe4fa1807
KURO_COOKIE=kuro_session=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOjE5NjExLCJyb2xlIjoidXNlciIsInNlc3Npb25WZXJzaW9uIjoxMSwiaWF0IjoxNzg5NDE0Mzk4LCJleHAiOjE3OTAwMTkxOTh9.IC-Azt3rnU4fK6zLyUL_SczkFQIB2w5lcqRAfR41DwQ; _kn=4e1de216d265f648dff22cb4ea4860bdf30ce6fbe4fa1807
IMPORTER_USER_ID=732fbe87-5040-41fb-9983-0aedb2af44c8
SUPABASE_URL=https://placeholder.supabase.co
SUPABASE_SERVICE_ROLE_KEY=placeholder-service-role-key`;

async function main() {
  const browser = await puppeteer.launch({
    executablePath: '/usr/bin/chromium',
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
  });

  try {
    const page = await browser.newPage();
    await page.setCookie({
      name: 'session_id',
      value: '4bc8293a4be7d5e98fe447b89d6d3bb0e12897b1b421820829d4ee8127348f2404af1f927eea5a62c186f98bc11ef7676a9f9a68',
      domain: '.discloud.com',
      path: '/'
    });

    console.log('Navigating to Discloud env page...');
    await page.goto('https://discloud.com/dashboard/app/1788873398156/env', { waitUntil: 'networkidle2' });

    console.log('Opening "Colar .env" modal...');
    await page.evaluate(() => {
      const btns = Array.from(document.querySelectorAll('button'));
      const btn = btns.find(b => b.innerText && b.innerText.includes('Colar .env'));
      if (btn) btn.click();
      else throw new Error('Colar .env button not found');
    });

    await page.waitForSelector('textarea.imp-textarea', { timeout: 10000 });

    console.log('Typing env content into textarea...');
    await page.evaluate((text) => {
      const ta = document.querySelector('textarea.imp-textarea');
      if (!ta) throw new Error('Textarea not found');
      // @ts-ignore
      ta.value = text;
      ta.dispatchEvent(new Event('input', { bubbles: true }));
      ta.dispatchEvent(new Event('change', { bubbles: true }));
    }, ENV_CONTENT);

    await new Promise(r => setTimeout(r, 1000));

    console.log('Submitting Importar...');
    await page.evaluate(() => {
      const modal = document.querySelector('.modal, [role="dialog"], div[class*="modal"]');
      if (!modal) throw new Error('Modal not found');
      const btns = Array.from(modal.querySelectorAll('button'));
      const importBtn = btns.find(b => b.innerText && b.innerText.includes('Importar'));
      if (importBtn) importBtn.click();
      else throw new Error('Importar button not found in modal');
    });

    console.log('Waiting 5 seconds for import to save...');
    await new Promise(r => setTimeout(r, 5000));

    const rows = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('table tr, div[role="row"]')).map(r => r.innerText.replace(/\n+/g, ' | '));
    });
    console.log('Import finished! Current rows count:', rows.length);
    console.log('Sample rows:', rows.slice(0, 8));

  } finally {
    await browser.close();
  }
}

main().catch(console.error);
