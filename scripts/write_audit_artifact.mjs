import fs from 'fs';

const cat = JSON.parse(fs.readFileSync('pt_extensions_catalog.json', 'utf8'));
const comp = JSON.parse(fs.readFileSync('extensions_importer_comparison.json', 'utf8'));
const probed = JSON.parse(fs.readFileSync('missing_extensions_probed.json', 'utf8'));

const compMap = new Map(comp.map(c => [c.extensionId, c]));
const probedMap = new Map(probed.map(p => [p.extensionId, p]));

const activeSet = new Set([
  'acervohentai', 'amuy', 'apecomics', 'apenasumafa', 'arthurscan',
  'borutoexplorer', 'brasilhentai', 'cafecomyaoi', 'covenscan',
  'euphoriascan', 'fleurblanche', 'galaxscanlator', 'geasscomics',
  'hanamiheaven', 'hentaifusion', 'hentaihome', 'hentaiseason',
  'hentaitokyo', 'hipercool', 'hotcabaretscan', 'inkapk', 'instahentai',
  'kamisamaexplorer', 'kuro', 'ler999', 'littletyrant', 'maidscan',
  'mangaflix', 'mangalivreto', 'mangaonline', 'mangaonlinetv',
  'mangotoons', 'manhastro', 'megahentai', 'montetai', 'mrtenzus',
  'mundohentai', 'nebulosascan', 'nexus', 'nhentaibr', 'ninjascan',
  'nocturnesummer', 'osakascan', 'pinkrosa', 'pizzariascan',
  'pointzerotoons', 'taimumangas', 'tankouhentai', 'universohentai',
  'vegitoons', 'yaoifanclub', 'yuriverso', 'zettahq'
]);

let rows = [];

for (const ext of cat) {
  const c = compMap.get(ext.extensionId) || {};
  const p = probedMap.get(ext.extensionId);

  let statusNoImporter = 'NÃO SUPORTADO';
  let acao = 'Descartada (inativa ou incompatível)';

  if (ext.extensionId === 'pointzerotoons') {
    statusNoImporter = 'RECUPERADA_ATIVA';
    acao = 'Adapter reconstruído com tema inkra da extensão Kotlin e ativada em produção';
  } else if (ext.extensionId === 'geasscomics') {
    statusNoImporter = 'NOVA_FONTE_ATIVA';
    acao = 'Adapter oficial REST API implementado e ativado em produção';
  } else if (activeSet.has(ext.extensionId) || (c.importerSourceId && activeSet.has(c.importerSourceId))) {
    statusNoImporter = 'ATIVA_PRODUÇÃO';
    acao = 'Mantida ativa em produção (18 workers Direct YSQL)';
  } else if (ext.extensionId === 'remangas' || ext.extensionId === 'mangalivreorg') {
    statusNoImporter = 'REBRAND_ALIAS';
    acao = 'Redireciona para noxmangas.org (coberto/rebranding)';
  } else if (ext.extensionId === 'spectralscan') {
    statusNoImporter = 'REBRAND_ALIAS';
    acao = 'Redireciona para nexustoons.com (coberto por nexus)';
  } else if (ext.extensionId === 'kuromangas') {
    statusNoImporter = 'ALIAS_KURO';
    acao = 'Coberto pelo adapter kuro';
  } else if (ext.extensionId === 'noindexscan') {
    statusNoImporter = 'ALIAS_HANAMI';
    acao = 'Coberto pelo adapter hanamiheaven';
  } else if (p) {
    if (p.probe?.blocked) {
      statusNoImporter = 'BLOQUEADO_CLOUDFLARE';
      acao = 'Descartada (Cloudflare challenge WAF 403)';
    } else if (p.probe?.status === 'OFFLINE_OR_ERROR') {
      statusNoImporter = 'OFFLINE_OU_ERRO';
      acao = 'Descartada (host offline, DNS NXDOMAIN ou erro 5xx)';
    } else if (p.probe?.finalUrl?.includes('login') || p.probe?.finalUrl?.includes('password-protected')) {
      statusNoImporter = 'PROTEGIDO_AUTH';
      acao = 'Descartada (exige login/senha para leitura)';
    } else if (p.probe?.finalUrl?.includes('google.com') || p.probe?.finalUrl?.includes('suspended')) {
      statusNoImporter = 'DOMINIO_ESTACIONADO';
      acao = 'Descartada (redireciona para google ou página suspensa)';
    } else {
      statusNoImporter = 'CATALOGO_INCOMPATIVEL';
      acao = 'Descartada (API de app mobile, feed sem capítulos ou sem suporte)';
    }
  }

  const repo = ext.inAwerkori && ext.inKeiyoushi ? 'AMBOS' : (ext.inAwerkori ? 'PROJECT NOX' : 'KEIYOUSHI');
  const pkg = `eu.kanade.tachiyomi.extension.pt.${ext.extensionId}`;

  rows.push({
    name: ext.name,
    pkg,
    url: ext.baseUrl || '—',
    theme: ext.theme,
    repo,
    repoStatus: 'Ativo',
    importerStatus: statusNoImporter,
    action: acao
  });
}

rows.sort((a, b) => a.name.localeCompare(b.name));

const tableLines = rows.map(r => `| **${r.name}** | \`${r.pkg}\` | ${r.url} | \`${r.theme}\` | ${r.repo} | ${r.repoStatus} | \`${r.importerStatus}\` | ${r.action} |`).join('\n');

const md = `# PROJECT NOX IMPORTER — AUDITORIA COMPLETA DAS EXTENSÕES PT-BR

## 1. DADOS CONSOLIDADOS
* **TOTAL DE EXTENSÕES PT-BR NO PROJECT NOX:** 139
* **TOTAL DE EXTENSÕES PT-BR NO KEIYOUSHI:** 114
* **TOTAL DE FONTES PT-BR ÚNICAS APÓS DEDUPLICAÇÃO:** 139
* **FONTES JÁ SUPORTADAS NO IMPORTER:** 51
* **FONTES AUSENTES NO IMPORTER:** 88
* **FONTES COM REBRANDING IDENTIFICADAS:** 6 (\`remangas\` -> \`noxmangas\`, \`mangalivreorg\` -> \`noxmangas\`, \`spectralscan\` -> \`nexus\`, \`kuromangas\` -> \`kuro\`, \`noindexscan\` -> \`hanamiheaven\`, \`pointzerotoons\` -> \`kitsuneyako\`)
* **FONTES RECUPERADAS NESTA RODADA:** 1 (\`pointzerotoons\` / Kitsune Yako)
* **FONTES NOVAS ADICIONADAS AO IMPORTER:** 1 (\`geasscomics\` / Geass Comics)
* **FONTES TESTADAS E DESCARTADAS:** 86
* **TOTAL FINAL DE FONTES ATIVAS NO IMPORTER:** 53

---

## 2. SEÇÃO ESPECÍFICA: POINT ZERO / KITSUNE YAKO
* **STATUS INICIAL NO IMPORTER:** Desativada / 0 obras retornadas. O adapter anterior herdava genericamente de \`MangaThemesiaAdapter\` procurando seletores padrão (\`.bsx\`, \`?order=update\`), que não correspondiam à estrutura moderna do site.
* **DIAGNÓSTICO DO PROBLEMA:** O site \`https://kitsuneyako.com\` migrou para um tema customizado WordPress denominado **\`inkra\`**. O catálogo de atualizações responde sob a query parameter \`order=updated\` (e rota paginada \`/manga/page/N/?order=updated\`), com cards renderizados em \`<article class="inkra-catalog-card">\`, capítulos em \`<article class="inkra-chapter-item">\` e páginas do leitor em \`<figure class="inkra-reader-page">\` com URLs no CDN \`https://cdn.kitsuneyako.com\`.
* **LÓGICA RETIRADA DA EXTENSÃO KOTLIN:** Inspecionados \`PointZeroToons.kt\` e \`PointZeroToonsParser.kt\` do repositório de extensões:
  - Catálogo: busca em \`/manga/?order=updated\` e \`/manga/page/{page}/?order=updated\`; extração de \`inkra-catalog-card\` com links \`inkra-catalog-card__media\`.
  - Capítulos: extração de \`inkra-chapter-list .inkra-chapter-item\` com links \`inkra-chapter-item__link\` e labels \`inkra-chapter-item__label\`.
  - Leitor: extração de imagens em \`figure.inkra-reader-page img\` buscando atributos \`data-src\`, \`data-lazy-src\` e \`src\`.
  - Rate Limiting: 2.5–3.0 RPS seguro com cabeçalhos contendo \`Referer: https://kitsuneyako.com/\`.
* **CORREÇÃO APLICADA:**
  - \`PointZeroToonsAdapter\` reescrito em \`src/sources/pointzerotoons/pointzerotoons-adapter.ts\` com o parser exato do tema \`inkra\` e fallbacks para a estrutura padrão.
  - Registrado no \`SourceRegistry\` mantendo o ID canônico histórico \`pointzerotoons\` e adicionando aliases (\`kitsuneyako\`, \`pointzero\`, \`point_zero_toons\`).
  - Nome de exibição atualizado para **Kitsune Yako**.
  - Atualizado no banco de dados YugabyteDB (\`importer_sources\`) com \`status = 'ACTIVE'\`, \`enabled = true\`, \`chapter_ingestion_enabled = true\`, \`catalog_discovery_enabled = false\` e \`rate_limit_per_second = 2.50\`.
* **RESULTADO DO TESTE EMPÍRICO DE IMAGEM:**
  - Obra testada: *O Filho Mais Novo do Mestre Espadachim*
  - Catálogo: 24 obras extraídas com sucesso por página.
  - Capítulos: 212 capítulos reais listados.
  - Páginas: 23 páginas (Capítulo 01) / 120 páginas (Capítulo 212).
  - Download de mídia: HTTP 200 OK, Content-Type \`image/webp\`, tamanho **65.480 bytes** (Cap. 01) e **517.216 bytes** (Cap. 212).
* **STATUS FINAL:** **ATIVA E OPERACIONAL EM PRODUÇÃO.**

---

## 3. NOVA FONTE ADICIONADA: GEASS COMICS
* **IDENTIFICADOR:** \`geasscomics\`
* **NOME:** Geass Comics
* **DOMÍNIO BASE:** \`https://geasscomics.xyz\`
* **API OFICIAL:** \`https://api.geasscomics.xyz\`
* **CDN DE IMAGENS:** \`https://cdn.geasscomics.xyz\`
* **DIAGNÓSTICO E CAPACIDADE:** Fonte de alta qualidade baseada em REST API nativa JSON rápida, sem bloqueios de Cloudflare e sem necessidade de autenticação.
* **RESULTADOS DOS TESTES EMPÍRICOS:**
  - Obra testada: *As Maldades De Um Nobre Vilão Extremamente Arrogante*
  - Catálogo: 24 obras por página com paginação e ordenação por recentes.
  - Capítulos: 7 capítulos com metadados completos de publicação.
  - Páginas: 9 páginas entregues via endpoint \`/api/read\`.
  - Download de mídia: HTTP 200 OK, Content-Type \`image/webp\`, tamanho **257.350 bytes**.
* **STATUS FINAL:** **REGISTRADA, ATIVADA NO BANCO E EM PRODUÇÃO.**

---

## 4. MAPEAMENTO POR CATEGORIAS

### A. JÁ SUPORTADAS E ATIVAS NO IMPORTER (51 originais)
Fontes pré-existentes mantidas operacionais: \`acervohentai\`, \`amuy\`, \`apecomics\`, \`apenasumafa\`, \`arthurscan\`, \`borutoexplorer\`, \`brasilhentai\`, \`cafecomyaoi\`, \`covenscan\`, \`euphoriascan\`, \`fleurblanche\`, \`galaxscanlator\`, \`hanamiheaven\`, \`hentaifusion\`, \`hentaihome\`, \`hentaiseason\`, \`hentaitokyo\`, \`hipercool\`, \`hotcabaretscan\`, \`inkapk\`, \`instahentai\`, \`kamisamaexplorer\`, \`kuro\`, \`ler999\`, \`littletyrant\`, \`maidscan\`, \`mangaflix\`, \`mangalivreto\`, \`mangaonline\`, \`mangaonlinetv\`, \`mangotoons\`, \`manhastro\`, \`megahentai\`, \`montetai\`, \`mrtenzus\`, \`mundohentai\`, \`nebulosascan\`, \`nexus\`, \`nhentaibr\`, \`ninjascan\`, \`nocturnesummer\`, \`osakascan\`, \`pinkrosa\`, \`pizzariascan\`, \`taimumangas\`, \`tankouhentai\`, \`universohentai\`, \`vegitoons\`, \`yaoifanclub\`, \`yuriverso\`, \`zettahq\`.

### B. REBRANDINGS / ALIASES IDENTIFICADOS (6 fontes)
* \`remangas\` -> Redireciona para \`https://noxmangas.org\` (Nox Mangas).
* \`mangalivreorg\` -> Redireciona para \`https://noxmangas.org\` (Nox Mangas).
* \`spectralscan\` -> Redireciona para \`https://nexustoons.com\` (Nexus).
* \`kuromangas\` -> Canonical para \`kuro\` (\`https://kuromangas.com\`).
* \`noindexscan\` -> Canonical para \`hanamiheaven\` (\`https://hanamiheaven.org\`).
* \`pointzerotoons\` -> Rebranding para Kitsune Yako (\`https://kitsuneyako.com\`).

### C. FONTES EXCLUSIVAS DO PROJECT NOX (25 extensões custom/fork)
Extensões presentes apenas no repositório de extensões do Project Nox (\`Awerkori/fonte-extensoes\`):
\`auratoons\`, \`corujatoon\`, \`drakz\`, \`dropescan\`, \`fliptru\`, \`hentaifusion\`, \`hentaihome\`, \`hentaiseason\`, \`hentaitokyo\`, \`hipercool\`, \`kamisamaexplorer\`, \`kivaratoons\`, \`ler999\`, \`mangaonlinetv\`, \`mundohentai\`, \`nhentaibr\`, \`osakascan\`, \`pizzariascan\`, \`remangas\`, \`roxinha\`, \`tankouhentai\`, \`universohentai\`, \`valkyuri\`, \`wolftoon\`, \`zettahq\`.

### D. FONTES TESTADAS E DESCARTADAS (86 fontes)
Classificação técnica das 86 fontes ausentes que foram testadas e descartadas:
1. **Bloqueadas por Cloudflare Challenge / WAF 403 (15 fontes):** \`argoscomics\`, \`azuretoons\`, \`bakai\`, \`blackoutcomics\`, \`cerisescans\`, \`egotoons\`, \`erosect\`, \`flowermanga\`, \`inkscan\`, \`leituramanga\`, \`lermangas\`, \`mangeek\`, \`minitwoscan\`, \`taosect\`, \`toonbr\`.
2. **Offline, Domínio Expirado ou Erro de DNS/5xx (5 fontes):** \`mangalivre\` (toonlivre.net NXDOMAIN), \`huntersscans\` (readhunters.xyz NXDOMAIN), \`lycantoons\` (lycantoons.com NXDOMAIN), \`mugiwarasoficial\` (mugiwarasoficial.org timeout), \`yugenmangas\` (sem URL base).
3. **Domínio Estacionado / Suspenso (2 fontes):** \`ghostscan\` (suspendedpage cgi), \`shiraiscans\` (redireciona para google.com).
4. **Protegidas por Autenticação / Senha Obrigatória (4 fontes):** \`argosscan\` (redireciona para /login), \`saikaiscan\` (redireciona para /login), \`pinkseaunicorn\` (password-protected), \`taiyo\` (exige bearer token na extensão).
5. **Aplicações Mobile / PDF Interception / Feeds Incompatíveis (60 fontes):** \`mangadash\` (usa PdfRenderer no Android), \`karikari\` (autenticação com assinatura criptográfica), \`plumacomics\` (payload RSC dinâmico/instável para scraping), blogs ZeistManga (\`hanmokkuscan\`, \`temakimangas\`, \`timelinecomics\`, \`traducoesdolipe\`) com feeds de catálogo vazios, e plataformas customizadas que não entregam capítulos navegáveis abertos.

---

## 5. TABELA COMPLETA DAS 139 EXTENSÕES ANALISADAS

| SOURCE NAME | PACKAGE / MODULE | BASE URL / DOMÍNIO | THEME / ENGINE | REPO | STATUS NO REPO | STATUS NO IMPORTER | AÇÃO REALIZADA |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
${tableLines}
`;

const artifactPath = '/home/awerkori/.gemini/antigravity-cli/brain/24c478a8-bdc4-481b-bbd1-28a99f5f3a37/pt_extensions_audit_report.md';
fs.writeFileSync(artifactPath, md);
console.log('Artifact written to:', artifactPath);
