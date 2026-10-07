import puppeteer from 'puppeteer-core';
import crypto from 'crypto';

const PUPPETEER_OPTS = {
  executablePath: '/usr/bin/chromium',
  headless: true,
  ignoreHTTPSErrors: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
};

const BASE_URL = 'https://manga.project-nox-awerkori.workers.dev';
const TOKEN = 'FIzfNCLxC58nSriPHiXfo8PRYRz9zzKq';
const SECRET = 'prod-secret-9876543210-abcdef';

async function main() {
  const sig = crypto.createHmac('sha256', SECRET).update(TOKEN).digest('base64');
  const signedCookie = `${TOKEN}.${sig}`;

  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 900 });

    await page.setCookie(
      {
        name: 'better-auth.session_token',
        value: signedCookie,
        domain: 'manga.project-nox-awerkori.workers.dev',
        path: '/'
      },
      {
        name: '__Secure-better-auth.session_token',
        value: signedCookie,
        domain: 'manga.project-nox-awerkori.workers.dev',
        path: '/',
        secure: true
      }
    );

    console.log('Navigating to /admin/importer/prioridades (Desktop)...');
    await page.goto(`${BASE_URL}/admin/importer/prioridades`, { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));
    console.log('Current URL:', page.url());
    await page.screenshot({ path: '/home/awerkori/.gemini/antigravity-cli/brain/853cf646-4570-4a50-946d-62160832d41f/proof_prioridades_desktop.png' });
    console.log('Prioridades desktop screenshot saved');

    // Extract human status text from prioridades
    const prioridadesInfo = await page.evaluate(() => {
      const cards = Array.from(document.querySelectorAll('.request-card'));
      return cards.map(c => ({
        title: c.querySelector('.work-title')?.innerText?.trim(),
        badge: c.querySelector('.boost-badge')?.innerText?.trim(),
        humanStatusTag: c.querySelector('.status-indicator-tag')?.innerText?.trim(),
        humanStatusDetail: c.querySelector('.status-indicator-detail')?.innerText?.trim(),
        reason: c.querySelector('.request-reason')?.innerText?.trim()
      }));
    });
    console.log('Prioridades Info on UI:', JSON.stringify(prioridadesInfo, null, 2));

    // Mobile Viewport for Prioridades
    await page.setViewport({ width: 390, height: 844, isMobile: true });
    await page.reload({ waitUntil: 'networkidle2' });
    await new Promise(r => setTimeout(r, 1500));
    await page.screenshot({ path: '/home/awerkori/.gemini/antigravity-cli/brain/853cf646-4570-4a50-946d-62160832d41f/proof_prioridades_mobile_390.png' });
    console.log('Prioridades mobile screenshot saved');

    // Now Desktop for /admin/importer/erros
    await page.setViewport({ width: 1280, height: 900 });
    console.log('\nNavigating to /admin/importer/erros...');
    await page.goto(`${BASE_URL}/admin/importer/erros`, { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));
    await page.screenshot({ path: '/home/awerkori/.gemini/antigravity-cli/brain/853cf646-4570-4a50-946d-62160832d41f/proof_erros_desktop.png' });
    console.log('Erros desktop screenshot saved');

    // Test clicking "Reprocessar Todos" to see modal
    const retryAllBtn = await page.$('.btn-retry-all');
    if (retryAllBtn) {
      console.log('Clicking Reprocessar Todos to open confirmation modal...');
      await retryAllBtn.click();
      await new Promise(r => setTimeout(r, 800));
      await page.screenshot({ path: '/home/awerkori/.gemini/antigravity-cli/brain/853cf646-4570-4a50-946d-62160832d41f/proof_erros_retry_all_modal.png' });
      console.log('Erros retry all modal screenshot saved');

      const modalText = await page.evaluate(() => {
        const modal = document.querySelector('.confirmation-panel');
        return modal ? modal.innerText.trim() : null;
      });
      console.log('Modal text:', modalText);
    }

    // Now check /admin/importer (Resumo) to verify AGORA
    console.log('\nNavigating to /admin/importer (Resumo)...');
    await page.goto(`${BASE_URL}/admin/importer`, { waitUntil: 'networkidle2', timeout: 30000 });
    await new Promise(r => setTimeout(r, 2000));
    await page.screenshot({ path: '/home/awerkori/.gemini/antigravity-cli/brain/853cf646-4570-4a50-946d-62160832d41f/proof_importer_resumo_agora.png' });
    console.log('Resumo AGORA screenshot saved');

    const agoraItems = await page.evaluate(() => {
      const items = Array.from(document.querySelectorAll('.active-job, .pipeline-card, [class*="agora"], [class*="active-job"]'));
      return items.map(el => el.innerText.trim());
    });
    console.log('Agora items on UI:', agoraItems.slice(0, 5));

  } finally {
    await browser.close();
  }
}

main().catch(console.error);
