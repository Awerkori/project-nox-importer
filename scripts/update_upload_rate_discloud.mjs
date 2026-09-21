import puppeteer from 'puppeteer-core';

const DISCLOUD_COOKIE = {
  name: 'session_id',
  value: '4bc8293a4be7d5e98fe447b89d6d3bb0e12897b1b421820829d4ee8127348f2404af1f927eea5a62c186f98bc11ef7676a9f9a68',
  domain: '.discloud.com',
  path: '/'
};

async function main() {
  const browser = await puppeteer.launch({
    executablePath: '/usr/bin/chromium',
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--ignore-certificate-errors']
  });

  try {
    const page = await browser.newPage();
    await page.setCookie(DISCLOUD_COOKIE);

    console.log('Navigating to Discloud env page...');
    await page.goto('https://discloud.com/dashboard/app/1788873398156/env', { waitUntil: 'networkidle2' });
    await new Promise(r => setTimeout(r, 2000));

    console.log('Finding and updating UPLOAD_RATE_LIMIT_BYTES_PER_SEC input...');
    const updated = await page.evaluate(() => {
      const inputs = Array.from(document.querySelectorAll('input'));
      for (let i = 0; i < inputs.length; i++) {
        if (inputs[i].value === 'UPLOAD_RATE_LIMIT_BYTES_PER_SEC' && inputs[i+1]) {
          const valInput = inputs[i+1];
          const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
          setter?.call(valInput, '4194304');
          valInput.dispatchEvent(new Event('input', { bubbles: true }));
          valInput.dispatchEvent(new Event('change', { bubbles: true }));
          return true;
        }
      }
      return false;
    });

    console.log('Updated input in DOM:', updated);
    await new Promise(r => setTimeout(r, 1000));

    console.log('Clicking bottom Salvar button...');
    const saved = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button'));
      const saveBtn = buttons.find(b => b.innerText.trim() === 'Salvar' && b.className.includes('primary'));
      if (saveBtn) {
        saveBtn.click();
        return true;
      }
      return false;
    });
    console.log('Clicked Salvar:', saved);

    await new Promise(r => setTimeout(r, 5000));
    console.log('Env updated successfully on Discloud!');
  } finally {
    await browser.close();
  }
}

main().catch(console.error);
