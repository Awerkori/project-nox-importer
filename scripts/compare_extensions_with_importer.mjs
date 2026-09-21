import fs from 'node:fs';
import pg from 'pg';
import dotenv from 'dotenv';
import { SourceRegistry } from '../build/sources/registry.js';
import { HostRateLimiter } from '../build/core/rate-limiter.js';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const DB_CONFIG = {
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
};

async function main() {
  const catalog = JSON.parse(fs.readFileSync('pt_extensions_catalog.json'));
  console.log(`Loaded ${catalog.length} PT extensions from catalog.`);

  const client = new pg.Client(DB_CONFIG);
  await client.connect();

  const dbSourcesRes = await client.query('SELECT id, name, base_url, status, enabled FROM importer_sources');
  const dbSourcesMap = new Map();
  for (const s of dbSourcesRes.rows) {
    dbSourcesMap.set(s.id, s);
    // Also map by domain
    try {
      const host = new URL(s.base_url).hostname.replace(/^www\./, '');
      dbSourcesMap.set(`host:${host}`, s);
    } catch {}
  }

  const rateLimiter = new HostRateLimiter(5.0);
  const registry = new SourceRegistry(rateLimiter);

  const comparison = [];

  for (const ext of catalog) {
    let host = '';
    try {
      if (ext.baseUrl) host = new URL(ext.baseUrl).hostname.replace(/^www\./, '');
    } catch {}

    // Check direct ID match
    let dbSource = dbSourcesMap.get(ext.extensionId);
    let matchedBy = 'DIRECT_ID';

    // Check aliases
    if (!dbSource && ext.extensionId === 'pointzerotoons') {
      dbSource = dbSourcesMap.get('pointzerotoons');
    }
    if (!dbSource && ext.extensionId === 'kuromangas') {
      dbSource = dbSourcesMap.get('kuro');
      matchedBy = 'ALIAS_KURO';
    }
    if (!dbSource && (ext.extensionId === 'nexusmangas' || ext.extensionId === 'nexus_toons')) {
      dbSource = dbSourcesMap.get('nexus');
      matchedBy = 'ALIAS_NEXUS';
    }
    if (!dbSource && ext.extensionId === 'noindexscan') {
      dbSource = dbSourcesMap.get('hanamiheaven');
      matchedBy = 'ALIAS_HANAMIHEAVEN';
    }
    if (!dbSource && host) {
      const byHost = dbSourcesMap.get(`host:${host}`);
      if (byHost) {
        dbSource = byHost;
        matchedBy = `DOMAIN_MATCH (${byHost.id})`;
      }
    }

    // Check adapter in registry
    let adapter = registry.get(ext.extensionId);
    if (!adapter && dbSource) {
      adapter = registry.get(dbSource.id);
    }

    comparison.push({
      extensionId: ext.extensionId,
      name: ext.name,
      baseUrl: ext.baseUrl,
      theme: ext.theme,
      versionCode: ext.versionCode,
      inAwerkori: ext.inAwerkori,
      inKeiyoushi: ext.inKeiyoushi,
      inImporterDb: !!dbSource,
      importerSourceId: dbSource ? dbSource.id : null,
      importerStatus: dbSource ? dbSource.status : 'NOT_IN_DB',
      importerEnabled: dbSource ? dbSource.enabled : false,
      hasAdapter: !!adapter,
      adapterName: adapter ? adapter.constructor.name : null,
      matchMethod: dbSource ? matchedBy : 'NONE'
    });
  }

  await client.end();

  fs.writeFileSync('extensions_importer_comparison.json', JSON.stringify(comparison, null, 2));
  console.log(`Saved comparison to extensions_importer_comparison.json`);

  const inDb = comparison.filter(c => c.inImporterDb);
  const notInDb = comparison.filter(c => !c.inImporterDb);
  const hasAdapter = comparison.filter(c => c.hasAdapter);
  const missingAdapter = comparison.filter(c => !c.hasAdapter);

  console.log('\n--- EXTENSION COMPARISON SUMMARY ---');
  console.log(`Total PT extensions: ${comparison.length}`);
  console.log(`Matched in Importer DB: ${inDb.length}`);
  console.log(`NOT in Importer DB: ${notInDb.length}`);
  console.log(`Has Adapter in Registry: ${hasAdapter.length}`);
  console.log(`Missing Adapter: ${missingAdapter.length}`);
}

main().catch(console.error);
