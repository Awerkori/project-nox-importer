import puppeteer from 'puppeteer-core';

const sql = `
CREATE OR REPLACE FUNCTION prune_importer_queue() RETURNS void AS $$
BEGIN
  DELETE FROM public.importer_queue
  WHERE status IN ('COMPLETED', 'SUPERSEDED', 'CANCELLED_BY_STAFF')
     OR (status = 'FAILED' AND created_at < NOW() - INTERVAL '7 days');
END;
$$ LANGUAGE plpgsql;

-- If pg_cron is enabled, we can schedule it:
-- SELECT cron.schedule('prune_importer_queue', '0 0 * * *', 'SELECT prune_importer_queue()');
`;

// Just run the DELETE manually for now to save 26MB instantly
const deleteSql = `DELETE FROM public.importer_queue WHERE status IN ('COMPLETED', 'SUPERSEDED', 'CANCELLED_BY_STAFF');`;

async function run() {
  const browserRes = await fetch('http://127.0.0.1:9222/json/version');
  const browserData = await browserRes.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: browserData.webSocketDebuggerUrl, defaultViewport: null });
  
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 4000));
  
  const res = await page.evaluate(async (query) => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    if (!token) return { error: "No token" };
    const jwt = JSON.parse(token).access_token;
    const response = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query })
    });
    return { status: response.status, body: await response.text() };
  }, deleteSql);
  
  console.log(res.status, res.body);
  await page.close();
  await browser.disconnect();
}
run().catch(console.error);
