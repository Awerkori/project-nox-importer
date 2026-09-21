import fs from 'fs';

const cat = JSON.parse(fs.readFileSync('pt_extensions_catalog.json', 'utf8'));
const comp = JSON.parse(fs.readFileSync('extensions_importer_comparison.json', 'utf8'));
const probed = JSON.parse(fs.readFileSync('missing_extensions_probed.json', 'utf8'));

const compMap = new Map(comp.map(c => [c.extensionId, c]));
const probedMap = new Map(probed.map(p => [p.extensionId, p]));

// Active sources in importer now:
const activeInImporter = new Set([
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
  const c = compMap.get(ext.id) || {};
  const p = probedMap.get(ext.id);

  let statusNoImporter = 'NÃO CADASTRADO';
  let acao = 'Descartada (inativa/incompatível)';

  if (activeInImporter.has(ext.id) || (c.importerSourceId && activeInImporter.has(c.importerSourceId))) {
    statusNoImporter = 'ATIVO_PRODUÇÃO';
    acao = 'Mantida ativa em produção';
  }

  if (ext.id === 'pointzerotoons') {
    statusNoImporter = 'RECUPERADO_ATIVO';
    acao = 'Adapter reescrito (tema inkra) e reativado em produção';
  } else if (ext.id === 'geasscomics') {
    statusNoImporter = 'NOVO_ATIVO';
    acao = 'Adapter criado via REST API e ativado em produção';
  } else if (ext.id === 'remangas' || ext.id === 'mangalivreorg') {
    statusNoImporter = 'REBRAND_ALIAS';
    acao = 'Redireciona para noxmangas.org (coberto/rebranding)';
  } else if (ext.id === 'spectralscan') {
    statusNoImporter = 'REBRAND_ALIAS';
    acao = 'Redireciona para nexustoons.com (coberto por nexus)';
  } else if (ext.id === 'kuromangas') {
    statusNoImporter = 'ALIAS_KURO';
    acao = 'Coberto pelo adapter kuro';
  } else if (ext.id === 'noindexscan') {
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
      acao = 'Descartada (API protegida por app, feed vazio ou sem suporte)';
    }
  }

  const repo = ext.inAwerkori && ext.inKeiyoushi ? 'AMBOS' : (ext.inAwerkori ? 'PROJECT NOX' : 'KEIYOUSHI');

  rows.push({
    name: ext.name,
    pkg: ext.pkg,
    url: ext.baseUrl,
    theme: ext.theme,
    repo,
    repoStatus: 'Ativo',
    importerStatus: statusNoImporter,
    action: acao
  });
}

// Sort alphabetically by name
rows.sort((a, b) => a.name.localeCompare(b.name));

console.log(`Generated ${rows.length} rows.`);

let md = '| SOURCE NAME | PACKAGE / MODULE | BASE URL / DOMÍNIO | THEME / ENGINE | REPO | STATUS NO REPO | STATUS NO IMPORTER | AÇÃO REALIZADA |\n';
md += '| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n';

for (const r of rows) {
  md += `| **${r.name}** | \`${r.pkg}\` | ${r.url} | \`${r.theme}\` | ${r.repo} | ${r.repoStatus} | \`${r.importerStatus}\` | ${r.action} |\n`;
}

fs.writeFileSync('audit_pt_extensions_table.md', md);
console.log('Saved audit_pt_extensions_table.md');
