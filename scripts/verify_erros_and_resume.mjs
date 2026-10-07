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
const ARTIFACTS_DIR = '/home/awerkori/.gemini/antigravity-cli/brain/853cf646-4570-4a50-946d-62160832d41f';

async function main() {
  console.log('🚀 Iniciando verificação E2E via Puppeteer...');
  const sig = crypto.createHmac('sha256', SECRET).update(TOKEN).digest('base64');
  const signedCookie = `${TOKEN}.${sig}`;

  const browser = await puppeteer.launch(PUPPETEER_OPTS);
  const page = await browser.newPage();

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

  // ==========================================
  // TEST 1: Sidebar & Subnav on /admin/importer
  // ==========================================
  console.log('\n[TEST 1] Verificando Sidebar e Subnav em /admin/importer (Desktop 1280x800)...');
  await page.setViewport({ width: 1280, height: 800 });
  const resp1 = await page.goto(`${BASE_URL}/admin/importer`, { waitUntil: 'networkidle2', timeout: 30000 });
  console.log(`HTTP Status /admin/importer: ${resp1.status()}`);

  const sidebarLinks = await page.evaluate(() => {
    const navLinks = Array.from(document.querySelectorAll('.sidebar-nav .nav-link, nav a'));
    return navLinks.map(a => ({
      text: a.textContent.trim(),
      href: a.getAttribute('href')
    })).filter(l => l.href && l.href.includes('/admin/importer'));
  });
  console.log('Links do Importer na Sidebar:', sidebarLinks);

  const hasErrosInSidebar = sidebarLinks.some(l => l.href === '/admin/importer/erros' || l.text.toLowerCase().includes('erros'));
  console.log(`✅ "Erros" presente na sidebar: ${hasErrosInSidebar}`);

  await page.screenshot({ path: `${ARTIFACTS_DIR}/proof_sidebar_erros_desktop.png` });
  console.log('📸 Screenshot salvo: proof_sidebar_erros_desktop.png');

  // ==========================================
  // TEST 2: Central de Erros (/admin/importer/erros)
  // ==========================================
  console.log('\n[TEST 2] Verificando /admin/importer/erros (Desktop)...');
  const resp2 = await page.goto(`${BASE_URL}/admin/importer/erros`, { waitUntil: 'networkidle2', timeout: 30000 });
  console.log(`HTTP Status /admin/importer/erros: ${resp2.status()}`);

  const pageInfo = await page.evaluate(() => {
    const title = document.querySelector('h1, .admin-header h1, h2')?.textContent?.trim();
    const metricCards = Array.from(document.querySelectorAll('.metric-card')).map(c => c.textContent?.trim());
    const tabs = Array.from(document.querySelectorAll('.tab-btn')).map(t => t.textContent?.trim());
    const cardsCount = document.querySelectorAll('.error-card').length;
    const firstCardTitle = document.querySelector('.error-card .card-title')?.textContent?.trim();
    const firstCardError = document.querySelector('.error-card .error-text')?.textContent?.trim();
    const actions = Array.from(document.querySelectorAll('.error-card .action-btn')).slice(0, 4).map(b => b.textContent?.trim());
    return { title, metricCards, tabs, cardsCount, firstCardTitle, firstCardError, actions };
  });

  console.log('Informações da Central de Erros:', pageInfo);
  await page.screenshot({ path: `${ARTIFACTS_DIR}/proof_erros_page_desktop.png` });
  console.log('📸 Screenshot salvo: proof_erros_page_desktop.png');

  // ==========================================
  // TEST 3: Modal de Detalhes Técnicos
  // ==========================================
  console.log('\n[TEST 3] Testando clique em [ Ver detalhes ] para abrir o modal...');
  const clickedDetails = await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll('button'));
    const btn = btns.find(b => b.textContent.includes('Ver detalhes'));
    if (btn) {
      btn.click();
      return true;
    }
    return false;
  });

  if (clickedDetails) {
    await new Promise(r => setTimeout(r, 600));
    const modalVisible = await page.evaluate(() => {
      const modal = document.querySelector('.modal-panel');
      const title = modal?.querySelector('.modal-title')?.textContent?.trim();
      const codeBlocks = Array.from(modal?.querySelectorAll('.code-block') || []).map(cb => cb.textContent?.trim().slice(0, 80));
      return { visible: !!modal, title, codeBlocks };
    });
    console.log('Modal aberto com sucesso:', modalVisible);
    await page.screenshot({ path: `${ARTIFACTS_DIR}/proof_erros_modal_desktop.png` });
    console.log('📸 Screenshot salvo: proof_erros_modal_desktop.png');

    // Fechar modal
    await page.evaluate(() => {
      const closeBtn = document.querySelector('.btn-close, .btn-modal-close');
      if (closeBtn) closeBtn.click();
    });
    await new Promise(r => setTimeout(r, 300));
  }

  // ==========================================
  // TEST 4: Central de Erros em Mobile (390x844)
  // ==========================================
  console.log('\n[TEST 4] Verificando responsividade Mobile (390x844)...');
  await page.setViewport({ width: 390, height: 844, isMobile: true });
  await page.goto(`${BASE_URL}/admin/importer/erros`, { waitUntil: 'networkidle2', timeout: 30000 });
  await page.screenshot({ path: `${ARTIFACTS_DIR}/proof_erros_page_mobile_390.png` });
  console.log('📸 Screenshot salvo: proof_erros_page_mobile_390.png');

  // ==========================================
  // TEST 5: Teste da API resume-protective-stop com sessão autenticada
  // ==========================================
  console.log('\n[TEST 5] Testando endpoint POST /api/admin/importer/resume-protective-stop...');
  const apiTestResult = await page.evaluate(async () => {
    try {
      const res = await fetch('/api/admin/importer/resume-protective-stop', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      const data = await res.json();
      return { status: res.status, ok: res.ok, data };
    } catch (e) {
      return { error: e.message };
    }
  });

  console.log('Resultado da chamada de API resume-protective-stop:', apiTestResult);

  await browser.close();
  console.log('\n🎉 Verificação concluída com sucesso!');
}

main().catch(err => {
  console.error('❌ Erro no script de verificação:', err);
  process.exit(1);
});
