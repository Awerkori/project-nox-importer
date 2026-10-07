import puppeteer from 'puppeteer-core';

async function test() {
  const browser = await puppeteer.launch({
    executablePath: '/usr/bin/chromium',
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  const page = await browser.newPage();
  
  const consoleLogs = [];
  page.on('console', msg => consoleLogs.push(`[${msg.type()}] ${msg.text()}`));
  page.on('pageerror', err => consoleLogs.push(`[PAGEERROR] ${err.toString()}`));

  const failedRequests = [];
  page.on('requestfailed', req => {
    failedRequests.push(`${req.method()} ${req.url()} - ${req.failure()?.errorText}`);
  });

  const responses = [];
  page.on('response', res => {
    if (res.status() >= 400) {
      responses.push(`${res.status()} ${res.url()}`);
    }
  });

  console.log('Navigating to reader...');
  const res = await page.goto('https://manga.project-nox-awerkori.workers.dev/ler/be191a44-64a7-40e9-af74-8179af591c31', {
    waitUntil: 'networkidle2',
    timeout: 30000
  });

  console.log('Status:', res.status());
  
  await new Promise(r => setTimeout(r, 4000));

  const pageState = await page.evaluate(() => {
    const noticeEl = document.querySelector('.notice');
    const images = Array.from(document.querySelectorAll('.reader-page img')).map(img => ({
      src: img.src,
      complete: img.complete,
      naturalWidth: img.naturalWidth,
      naturalHeight: img.naturalHeight,
      loading: img.loading
    }));
    const pageStack = document.querySelector('.page-stack');
    const pagesCount = document.querySelectorAll('.reader-page').length;
    return {
      noticeText: noticeEl ? noticeEl.innerText : null,
      imagesCount: images.length,
      pagesCount,
      sampleImages: images.slice(0, 3)
    };
  });

  console.log('Page State:', JSON.stringify(pageState, null, 2));
  console.log('Console Logs:', consoleLogs);
  console.log('Failed Requests:', failedRequests);
  console.log('Error Responses:', responses);

  await browser.close();
}

test().catch(console.error);
