import puppeteer from 'puppeteer-core';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

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

const envMap = {
  NODE_ENV: 'production',
  STORAGE_PROVIDER: 'direct_telegram',
  UPLOAD_RATE_LIMIT_BYTES_PER_SEC: '50000000',
  MAX_CONCURRENT_CHAPTERS: '8',
  BATCH_PAGE_DOWNLOAD_CONCURRENCY: '8',
  POLL_INTERVAL_SECONDS: '5',
  QUEUE_LEASE_DURATION_SECONDS: '300',
  QUEUE_HEARTBEAT_INTERVAL_SECONDS: '20',
  WORKER_ID: 'discloud-importer-1',
  LOG_LEVEL: 'info',
  NOX_STORAGE_BRIDGE_TOKEN: process.env.NOX_STORAGE_BRIDGE_TOKEN || '',
  NOX_MANGA_URL: process.env.NOX_MANGA_URL || 'https://manga.project-nox-awerkori.workers.dev',
  NOX_IMPORTER_GATEWAY_URL: process.env.NOX_IMPORTER_GATEWAY_URL || 'https://project-nox-importer-gateway.project-nox-awerkori.workers.dev',
  KURO_EMAIL: process.env.KURO_EMAIL || '',
  KURO_PASSWORD: process.env.KURO_PASSWORD || '',
  KURO_SESSION: process.env.KURO_SESSION || '',
  KURO_CLIENT_TOKEN: process.env.KURO_CLIENT_TOKEN || '',
  KURO_COOKIE: process.env.KURO_COOKIE || '',
  IMPORTER_USER_ID: process.env.IMPORTER_USER_ID || '732fbe87-5040-41fb-9983-0aedb2af44c8',
  SUPABASE_URL: 'https://placeholder.supabase.co',
  SUPABASE_SERVICE_ROLE_KEY: 'placeholder-service-role-key',
  IMPORTER_DB_MODE: 'direct',
  YUGABYTE_HOST: process.env.YUGABYTE_HOST || '',
  YUGABYTE_PORT: process.env.YUGABYTE_PORT || '5433',
  YUGABYTE_USER: process.env.YUGABYTE_USER || '',
  YUGABYTE_PASSWORD: process.env.YUGABYTE_PASSWORD || '',
  YUGABYTE_DATABASE: process.env.YUGABYTE_DATABASE || 'project_nox_prod',
  YUGABYTE_SSL_CERT: 'certs/yugabyte-root.crt'
};

const ENV_CONTENT = Object.entries(envMap)
  .map(([k, v]) => `${k}=${v}`)
  .join('\n');

async function main() {
  console.log('Starting Discloud Env Update & Rebuild...');
  console.log(`Setting ${Object.keys(envMap).length} environment variables...`);
  console.log('IMPORTER_DB_MODE:', envMap.IMPORTER_DB_MODE);
  console.log('MAX_CONCURRENT_CHAPTERS:', envMap.MAX_CONCURRENT_CHAPTERS);
  console.log('YUGABYTE_HOST:', envMap.YUGABYTE_HOST ? 'CONFIGURED' : 'MISSING');
  console.log('YUGABYTE_SSL_CERT:', envMap.YUGABYTE_SSL_CERT);

  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  try {
    const page = await browser.newPage();
    await page.setCookie(DISCLOUD_COOKIE);

    // 1. Navigate to Env Page
    console.log('\n[1/3] Navigating to Discloud env page...');
    await page.goto('https://discloud.com/dashboard/app/1788873398156/env', { waitUntil: 'networkidle2', timeout: 30000 });

    // Open "Colar .env" modal
    console.log('Opening "Colar .env" modal...');
    const btns = await page.$$('button');
    for (const b of btns) {
      const text = await (await b.getProperty('innerText')).jsonValue();
      if (text.includes('Colar .env')) {
        await b.click();
        break;
      }
    }

    await page.waitForSelector('textarea.imp-textarea', { timeout: 10000 });

    // Click checkbox to replace all
    await page.evaluate(() => {
      const cb = document.querySelector('label.imp-mode input[type="checkbox"]');
      if (cb && !cb.checked) {
        cb.click();
      }
    });

    console.log('Pasting env content into textarea...');
    await page.focus('textarea.imp-textarea');
    await page.evaluate((text) => {
      const ta = document.querySelector('textarea.imp-textarea');
      const setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value')?.set;
      setter?.call(ta, text);
      ta.dispatchEvent(new Event('input', { bubbles: true }));
      ta.dispatchEvent(new Event('change', { bubbles: true }));
    }, ENV_CONTENT);

    await new Promise(r => setTimeout(r, 800));

    console.log('Clicking Importar in modal...');
    const modal = await page.$('.v4modal-panel');
    if (!modal) throw new Error('Modal not found');
    const modalBtns = await modal.$$('button');
    for (const b of modalBtns) {
      const text = await (await b.getProperty('innerText')).jsonValue();
      if (text.includes('Importar')) {
        await b.click();
        break;
      }
    }

    await new Promise(r => setTimeout(r, 1500));

    console.log('Clicking Salvar button...');
    const saveSuccess = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const saveBtn = buttons.find(b => b.innerText.trim() === 'Salvar' && b.className.includes('primary'));
      if (saveBtn) {
        saveBtn.click();
        return true;
      }
      return false;
    });

    console.log('Salvar button clicked:', saveSuccess);
    await new Promise(r => setTimeout(r, 5000));

    // 2. Trigger Rebuild
    console.log('\n[2/3] Navigating to app overview to trigger Rebuild...');
    await page.goto('https://discloud.com/dashboard/app/1788873398156', { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 3000));

    const rebuildSuccess = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const btn = buttons.find(b => b.textContent.trim().toLowerCase() === 'rebuild' || b.textContent.trim().toLowerCase().includes('rebuild'));
      if (btn) {
        btn.click();
        return true;
      }
      return false;
    });
    console.log('Rebuild clicked:', rebuildSuccess);

    // 3. Monitor Logs
    console.log('\n[3/3] Waiting for container rebuild and boot...');
    await new Promise(r => setTimeout(r, 15000));

    for (let attempt = 1; attempt <= 8; attempt++) {
      console.log(`Checking logs (attempt ${attempt}/8)...`);
      await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded', timeout: 30000 });
      await new Promise(r => setTimeout(r, 5000));

      const logs = await page.evaluate(() => {
        const el = document.querySelector('pre, code, .logs, .terminal') || document.body;
        return el.innerText;
      });

      const lines = logs.split('\n').filter(l => l.trim()).slice(-35);
      console.log('--- LATEST LOGS ---');
      lines.forEach(l => console.log(l));

      const directModeLogged = lines.some(l => l.includes('IMPORTER DATABASE MODE: DIRECT'));
      const runnerPoolLogged = lines.some(l => l.includes('shared chapter runner pool (8 slots'));

      if (directModeLogged && runnerPoolLogged) {
        console.log('\n✅ DIRECT RUNTIME AND 8 RUNNER SLOTS CONFIRMED ON DISCLOUD!');
        break;
      }

      await new Promise(r => setTimeout(r, 10000));
    }

  } finally {
    await browser.close();
  }
}

main().catch(console.error);
