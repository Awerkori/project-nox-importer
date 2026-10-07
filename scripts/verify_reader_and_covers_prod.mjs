import puppeteer from 'puppeteer-core';
import pg from 'pg';
import dotenv from 'dotenv';
import path from 'path';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const PUPPETEER_OPTS = {
  executablePath: '/usr/bin/chromium',
  headless: true,
  ignoreHTTPSErrors: true,
  args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--ignore-certificate-errors']
};

const BASE_URL = 'https://manga.project-nox-awerkori.workers.dev';

async function verifyDatabaseCovers() {
  console.log('\n--- 1. DATABASE AUDIT FOR COVERS & PUBLICATION BARRIER ---');
  const client = new pg.Client({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });
  await client.connect();

  const publishedNullCovers = await client.query(`
    SELECT count(*) as count
    FROM works
    WHERE published = true AND cover_id IS NULL
  `);
  const brokenPublishedMedia = await client.query(`
    SELECT count(*) as count
    FROM works w
    LEFT JOIN media m ON w.cover_id = m.id
    WHERE w.published = true AND (m.id IS NULL OR m.storage_ready = false OR m.bytes < 1500)
  `);
  const totalPublished = await client.query(`
    SELECT count(*) as count
    FROM works
    WHERE published = true
  `);

  console.log(`Total Published Works: ${totalPublished.rows[0].count}`);
  console.log(`Published Works with NULL cover_id: ${publishedNullCovers.rows[0].count}`);
  console.log(`Published Works with Broken/Incomplete Cover Media: ${brokenPublishedMedia.rows[0].count}`);

  await client.end();
  return {
    publishedNullCovers: parseInt(publishedNullCovers.rows[0].count, 10),
    brokenPublishedMedia: parseInt(brokenPublishedMedia.rows[0].count, 10),
    totalPublished: parseInt(totalPublished.rows[0].count, 10)
  };
}

async function verifyReaderAndBrowser(browser, chapterId) {
  console.log(`\n--- 2. AUDITING READER IN PRODUCTION (Chapter: ${chapterId}) ---`);
  const page = await browser.newPage();
  await page.setViewport({ width: 1280, height: 900 });

  const consoleErrors = [];
  const cspViolations = [];
  const networkErrors = [];

  page.on('console', msg => {
    if (msg.type() === 'error') {
      consoleErrors.push(msg.text());
    }
  });
  page.on('pageerror', err => {
    consoleErrors.push(err.message);
  });
  page.on('requestfailed', req => {
    networkErrors.push({ url: req.url(), failure: req.failure()?.errorText });
  });

  const url = `${BASE_URL}/ler/${chapterId}`;
  console.log(`Navigating to ${url}...`);
  const res = await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
  console.log(`HTTP Status: ${res.status()}`);

  await new Promise(r => setTimeout(r, 4000));

  // Check notice / offline message
  const noticeText = await page.evaluate(() => {
    const noticeEl = document.querySelector('.reader-notice, .notice-bar, [role="alert"]');
    return noticeEl ? noticeEl.innerText.trim() : null;
  });
  console.log('Reader notice banner:', noticeText ? `"${noticeText}"` : 'NONE (Clean!)');

  // Check pages rendering
  const pagesEvaluation = await page.evaluate(() => {
    const images = Array.from(document.querySelectorAll('img')).filter(img => 
      img.src.includes('/media/') || img.closest('.reader-page') || img.closest('.page-item') || img.classList.contains('page-img')
    );
    const loadedImages = images.filter(img => img.complete && img.naturalWidth > 0);
    return {
      totalFound: images.length,
      loadedCount: loadedImages.length,
      sampleSources: images.slice(0, 3).map(img => ({
        src: img.src,
        naturalWidth: img.naturalWidth,
        naturalHeight: img.naturalHeight,
        complete: img.complete
      }))
    };
  });
  console.log(`Pages Evaluation: Found ${pagesEvaluation.totalFound}, Loaded ${pagesEvaluation.loadedCount}`);
  console.log('Sample images:', pagesEvaluation.sampleSources);

  const screenshotPath = '/home/awerkori/.gemini/antigravity-cli/brain/7e49a166-bd1d-4a8c-98cf-cef458fa3174/reader_verified.png';
  await page.screenshot({ path: screenshotPath });
  console.log(`Reader screenshot saved to ${screenshotPath}`);

  await page.close();

  return {
    httpStatus: res.status(),
    noticeText,
    totalImages: pagesEvaluation.totalFound,
    loadedImages: pagesEvaluation.loadedCount,
    consoleErrors,
    networkErrors
  };
}

async function verifyCatalogCovers(browser) {
  console.log(`\n--- 3. AUDITING CATALOG COVERS IN PRODUCTION ---`);
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900 });

  const url = `${BASE_URL}/catalogo`;
  console.log(`Navigating to ${url}...`);
  const res = await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
  console.log(`Catalog HTTP Status: ${res.status()}`);

  await new Promise(r => setTimeout(r, 4000));

  const coversEvaluation = await page.evaluate(() => {
    const cards = Array.from(document.querySelectorAll('.work-card, a[href^="/obra/"]'));
    const covers = Array.from(document.querySelectorAll('img[src*="/media/"], .work-card img, a[href^="/obra/"] img'));
    const brokenCovers = covers.filter(img => !img.complete || img.naturalWidth === 0);
    const loadedCovers = covers.filter(img => img.complete && img.naturalWidth > 0);
    return {
      cardsCount: cards.length,
      totalCovers: covers.length,
      loadedCovers: loadedCovers.length,
      brokenCovers: brokenCovers.length,
      samples: loadedCovers.slice(0, 4).map(img => ({
        src: img.src,
        naturalWidth: img.naturalWidth,
        naturalHeight: img.naturalHeight
      }))
    };
  });

  console.log(`Catalog Cards: ${coversEvaluation.cardsCount}`);
  console.log(`Catalog Covers Total: ${coversEvaluation.totalCovers}`);
  console.log(`Catalog Covers Loaded: ${coversEvaluation.loadedCovers}`);
  console.log(`Catalog Covers Broken: ${coversEvaluation.brokenCovers}`);
  console.log('Sample Loaded Covers:', coversEvaluation.samples);

  const screenshotPath = '/home/awerkori/.gemini/antigravity-cli/brain/7e49a166-bd1d-4a8c-98cf-cef458fa3174/catalogo_verified.png';
  await page.screenshot({ path: screenshotPath });
  console.log(`Catalog screenshot saved to ${screenshotPath}`);

  await page.close();
  return coversEvaluation;
}

async function verifyHomeCovers(browser) {
  console.log(`\n--- 4. AUDITING HOME COVERS IN PRODUCTION ---`);
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 900 });

  const url = `${BASE_URL}/`;
  console.log(`Navigating to ${url}...`);
  const res = await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
  console.log(`Home HTTP Status: ${res.status()}`);

  await new Promise(r => setTimeout(r, 3000));

  const homeEvaluation = await page.evaluate(() => {
    const covers = Array.from(document.querySelectorAll('img[src*="/media/"], .release-card img, .hero img, .carousel img'));
    const brokenCovers = covers.filter(img => !img.complete || img.naturalWidth === 0);
    const loadedCovers = covers.filter(img => img.complete && img.naturalWidth > 0);
    return {
      totalCovers: covers.length,
      loadedCovers: loadedCovers.length,
      brokenCovers: brokenCovers.length,
      samples: loadedCovers.slice(0, 3).map(img => ({
        src: img.src,
        naturalWidth: img.naturalWidth,
        naturalHeight: img.naturalHeight
      }))
    };
  });

  console.log(`Home Covers Total: ${homeEvaluation.totalCovers}`);
  console.log(`Home Covers Loaded: ${homeEvaluation.loadedCovers}`);
  console.log(`Home Covers Broken: ${homeEvaluation.brokenCovers}`);

  const screenshotPath = '/home/awerkori/.gemini/antigravity-cli/brain/7e49a166-bd1d-4a8c-98cf-cef458fa3174/home_verified.png';
  await page.screenshot({ path: screenshotPath });
  console.log(`Home screenshot saved to ${screenshotPath}`);

  await page.close();
  return homeEvaluation;
}

async function main() {
  const dbResults = await verifyDatabaseCovers();
  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  try {
    const readerResult1 = await verifyReaderAndBrowser(browser, 'd9a013e5-6dc8-443b-b80c-c92ddde3c3d7');
    const readerResult2 = await verifyReaderAndBrowser(browser, 'be191a44-64a7-40e9-af74-8179af591c31');
    const catalogResult = await verifyCatalogCovers(browser);
    const homeResult = await verifyHomeCovers(browser);

    console.log('\n================ FINAL VERIFICATION SUMMARY ================');
    console.log(`DB Published Works without cover: ${dbResults.publishedNullCovers}`);
    console.log(`DB Published Works with broken cover media: ${dbResults.brokenPublishedMedia}`);
    console.log(`Reader 1 Status: ${readerResult1.httpStatus} | Loaded Images: ${readerResult1.loadedImages}/${readerResult1.totalImages} | Notice: ${readerResult1.noticeText || 'None'}`);
    console.log(`Reader 2 Status: ${readerResult2.httpStatus} | Loaded Images: ${readerResult2.loadedImages}/${readerResult2.totalImages} | Notice: ${readerResult2.noticeText || 'None'}`);
    console.log(`Catalog Broken Covers: ${catalogResult.brokenCovers} / ${catalogResult.totalCovers}`);
    console.log(`Home Broken Covers: ${homeResult.brokenCovers} / ${homeResult.totalCovers}`);
    console.log('============================================================\n');
  } finally {
    await browser.close();
  }
}

main().catch(err => {
  console.error('Verification failed:', err);
  process.exit(1);
});
