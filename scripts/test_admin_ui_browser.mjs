import puppeteer from 'puppeteer-core';
import crypto from 'crypto';

const PUPPETEER_OPTS = {
  executablePath: '/usr/bin/chromium',
  headless: true,
  ignoreHTTPSErrors: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
};

const BASE_URL = 'https://manga.project-nox-awerkori.workers.dev';
const WORK_ID = '58b561b1-235b-45d6-979f-0c0ab7805a23';
const TOKEN = 'FIzfNCLxC58nSriPHiXfo8PRYRz9zzKq';
const SECRET = 'prod-secret-9876543210-abcdef';

async function main() {
  const sig = crypto.createHmac('sha256', SECRET).update(TOKEN).digest('base64');
  const signedCookie = `${TOKEN}.${sig}`;

  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900 });

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

  console.log('Navigating to Admin Obra Edit Page...');
  const res = await page.goto(`${BASE_URL}/admin/obras/${WORK_ID}`, { waitUntil: 'networkidle2', timeout: 30000 });
  console.log(`HTTP Status: ${res.status()}`);

  const pageTitle = await page.evaluate(() => document.title);
  console.log('Page title:', pageTitle);

  const formValues = await page.evaluate(() => {
    const titleInput = document.querySelector('input[name="title"]');
    const slugInput = document.querySelector('input[name="slug"]');
    const synopsisInput = document.querySelector('textarea[name="synopsis"]');
    return {
      title: titleInput ? titleInput.value : null,
      slug: slugInput ? slugInput.value : null,
      synopsisLength: synopsisInput ? synopsisInput.value.length : 0
    };
  });
  console.log('Form values loaded in Admin UI:', formValues);

  await page.screenshot({ path: '/home/awerkori/.gemini/antigravity-cli/brain/7e49a166-bd1d-4a8c-98cf-cef458fa3174/admin_obra_edit_ui.png' });
  console.log('Admin UI screenshot saved.');

  await browser.close();
}

main().catch(err => {
  console.error('Browser UI test error:', err);
  process.exit(1);
});
