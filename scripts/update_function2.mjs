import puppeteer from 'puppeteer-core';
async function run() {
  const res = await fetch('http://127.0.0.1:9222/json/version');
  const { webSocketDebuggerUrl } = await res.json();
  const browser = await puppeteer.connect({ browserWSEndpoint: webSocketDebuggerUrl, defaultViewport: null });
  const page = await browser.newPage();
  await page.goto('https://supabase.com/dashboard/project/izregkwaqdygwioqzwwo/sql/new', { waitUntil: 'domcontentloaded' });
  await new Promise(r => setTimeout(r, 5000));
  
  const result = await page.evaluate(async () => {
    const token = window.localStorage.getItem('supabase.dashboard.auth.token');
    const jwt = JSON.parse(token).access_token;
    
    const query = `
      CREATE OR REPLACE FUNCTION public.update_work_latest_chapter()
      RETURNS TRIGGER AS $$
      BEGIN
        IF (TG_OP = 'INSERT') THEN
          IF NEW.published_at IS NOT NULL THEN
            UPDATE public.works
            SET latest_chapter_published_at = NEW.published_at
            WHERE id = NEW.work_id AND (latest_chapter_published_at IS NULL OR latest_chapter_published_at < NEW.published_at);
          END IF;
        ELSIF (TG_OP = 'UPDATE') THEN
          IF NEW.published_at IS DISTINCT FROM OLD.published_at THEN
            IF NEW.published_at IS NOT NULL THEN
              -- Only update if it's strictly greater, saving locks on older chapters
              UPDATE public.works
              SET latest_chapter_published_at = NEW.published_at
              WHERE id = NEW.work_id AND (latest_chapter_published_at IS NULL OR latest_chapter_published_at < NEW.published_at);
            ELSE
              -- If unpublished, we MUST recalculate
              UPDATE public.works
              SET latest_chapter_published_at = (
                SELECT MAX(published_at)
                FROM public.chapters
                WHERE work_id = NEW.work_id AND published_at IS NOT NULL
              )
              WHERE id = NEW.work_id;
            END IF;
          END IF;
        ELSIF (TG_OP = 'DELETE') THEN
          IF OLD.published_at IS NOT NULL THEN
            UPDATE public.works
            SET latest_chapter_published_at = (
              SELECT MAX(published_at)
              FROM public.chapters
              WHERE work_id = OLD.work_id AND published_at IS NOT NULL
            )
            WHERE id = OLD.work_id;
          END IF;
        END IF;
        RETURN NULL;
      END;
      $$ LANGUAGE plpgsql SECURITY DEFINER;
    `;
    const data = await fetch('https://api.supabase.com/v1/projects/izregkwaqdygwioqzwwo/database/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${jwt}` },
      body: JSON.stringify({ query })
    }).then(r => r.json());
    
    return data;
  });
  
  console.log(JSON.stringify(result, null, 2));
  await browser.disconnect();
}
run().catch(console.error);
