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

// Parse command line arguments
// Usage: node scripts/set_discloud_config.mjs --workers 12 --bandwidth-mb 6
const args = process.argv.slice(2);
let targetWorkers = 8;
let targetBandwidthMb = 4.0;
let targetPoolMax = 2;
let action = 'rebuild'; // Default to rebuild so new .env is always baked in

let targetMediaConcurrency = 16;

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--workers' && args[i+1]) {
    targetWorkers = parseInt(args[i+1], 10);
    i++;
  } else if (args[i] === '--bandwidth-mb' && args[i+1]) {
    targetBandwidthMb = parseFloat(args[i+1]);
    i++;
  } else if (args[i] === '--pool-max' && args[i+1]) {
    targetPoolMax = parseInt(args[i+1], 10);
    i++;
  } else if (args[i] === '--media-concurrency' && args[i+1]) {
    targetMediaConcurrency = parseInt(args[i+1], 10);
    i++;
  } else if (args[i] === '--rebuild') {
    action = 'rebuild';
  }
}

const targetRateBytes = Math.round(targetBandwidthMb * 1024 * 1024);

const envMap = {
  NODE_ENV: 'production',
  STORAGE_PROVIDER: 'direct_telegram',
  UPLOAD_RATE_LIMIT_BYTES_PER_SEC: String(targetRateBytes),
  MAX_CONCURRENT_CHAPTERS: String(targetWorkers),
  TELEGRAM_MEDIA_CONCURRENCY: String(targetMediaConcurrency),
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
  YUGABYTE_SSL_CERT: 'certs/yugabyte-root.crt',
  DIRECT_DB_POOL_MAX: String(targetPoolMax),
  DEFAULT_HOST_RATE_PER_SECOND: '5.0',
  BUFFERED_PAGE_CONCURRENCY: '160',
  DOWNLOAD_INFLIGHT_CONCURRENCY: '64'
};

const ENV_CONTENT = Object.entries(envMap)
  .map(([k, v]) => `${k}=${v}`)
  .join('\n');

async function main() {
  console.log(`Setting Discloud configuration: Workers=${targetWorkers}, Bandwidth=${targetBandwidthMb}MB/s (${targetRateBytes} B/s), Action=${action}...`);

  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  try {
    const page = await browser.newPage();
    await page.setCookie(DISCLOUD_COOKIE);

    // 1. Navigate to Env Page
    console.log('Navigating to Discloud env page...');
    await page.goto('https://discloud.com/dashboard/app/1788873398156/env', { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 1000));

    // Open "Colar .env" modal
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

    await page.focus('textarea.imp-textarea');
    await page.evaluate((text) => {
      const ta = document.querySelector('textarea.imp-textarea');
      const setter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value')?.set;
      setter?.call(ta, text);
      ta.dispatchEvent(new Event('input', { bubbles: true }));
      ta.dispatchEvent(new Event('change', { bubbles: true }));
    }, ENV_CONTENT);

    await new Promise(r => setTimeout(r, 600));

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

    await new Promise(r => setTimeout(r, 1000));

    // Click Salvar
    await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const saveBtn = buttons.find(b => b.innerText.trim() === 'Salvar' && b.className.includes('primary'));
      if (saveBtn) saveBtn.click();
    });
    console.log('Saved updated environment variables.');
    await new Promise(r => setTimeout(r, 4000));

    // 2. Navigate to app page to restart or rebuild
    await page.goto('https://discloud.com/dashboard/app/1788873398156', { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));

    if (action === 'rebuild') {
      console.log('Triggering Rebuild...');
      await page.evaluate(() => {
        const buttons = Array.from(document.querySelectorAll('button'));
        const btn = buttons.find(b => b.textContent.trim().toLowerCase() === 'rebuild' || b.textContent.trim().toLowerCase().includes('rebuild'));
        if (btn) btn.click();
      });
      await new Promise(r => setTimeout(r, 1500));
      await page.evaluate(() => {
        const modal = document.querySelector('.v4modal-panel, [role="dialog"], .modal');
        if (modal) {
          const modalBtns = Array.from(modal.querySelectorAll('button'));
          const confirmBtn = modalBtns.find(b => b.innerText.trim().toLowerCase() === 'rebuild');
          if (confirmBtn) confirmBtn.click();
        }
      });
      console.log('Rebuild triggered, waiting 35s...');
      await new Promise(r => setTimeout(r, 35000));
    } else {
      console.log('Triggering Reiniciar...');
      await page.evaluate(() => {
        const buttons = Array.from(document.querySelectorAll('button'));
        const btn = buttons.find(b => b.textContent.trim().toLowerCase() === 'reiniciar' || b.textContent.trim().toLowerCase().includes('reiniciar'));
        if (btn) btn.click();
      });
      console.log('Restart triggered, waiting 15s...');
      await new Promise(r => setTimeout(r, 15000));
    }

    // Verify logs
    for (let attempt = 1; attempt <= 10; attempt++) {
      console.log(`Checking logs (attempt ${attempt}/10)...`);
      await page.goto('https://discloud.com/dashboard/app/1788873398156/logs', { waitUntil: 'domcontentloaded', timeout: 30000 });
      await new Promise(r => setTimeout(r, 4000));

      const logs = await page.evaluate(() => {
        const el = document.querySelector('pre, code, .logs, .terminal') || document.body;
        return el.innerText;
      });

      const lines = logs.split('\n').filter(l => l.trim()).slice(-30);
      const targetString = `shared chapter runner pool (${targetWorkers} slots for target concurrency ${targetWorkers})`;
      const runnerSlotsMatch = lines.find(l => l.includes(targetString));
      const directMatch = lines.some(l => l.includes('IMPORTER DATABASE MODE: DIRECT'));

      if (runnerSlotsMatch && directMatch) {
        console.log(`✅ Success: Found expected runner pool configuration: ${runnerSlotsMatch}`);
        break;
      }

      await new Promise(r => setTimeout(r, 5000));
    }

  } finally {
    await browser.close();
  }
}

main().catch(console.error);
