import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const { Client } = pg;
const DB_CONFIG = {
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
};

const TO_ACTIVATE = [
  'kuro',
  'nocturnesummer',
  'tankouhentai',
  'osakascan',
  'acervohentai',
  'amuy',
  'arthurscan',
  'inkapk',
  'mangaonline',
  'yaoifanclub',
  'yuriverso'
];

const TO_REMOVE = [
  'pointzerotoons',
  'tiamanhwa',
  'toonlivre',
  'nexus_toons'
];

async function main() {
  const client = new Client(DB_CONFIG);
  await client.connect();

  console.log('--- STARTING SOURCE RECOVERY & CLEANUP TRANSACTION ---');

  // 1. Snapshot BEFORE
  const beforeRes = await client.query(`
    SELECT status, enabled, COUNT(*)::int as count
    FROM importer_sources
    GROUP BY status, enabled
    ORDER BY status, enabled
  `);
  console.log('BEFORE counts by status/enabled:');
  console.table(beforeRes.rows);

  // 2. Activate the 11 recovered sources
  const activateRes = await client.query(`
    UPDATE importer_sources
    SET
      status = 'ACTIVE',
      enabled = true,
      chapter_ingestion_enabled = true,
      catalog_discovery_enabled = false,
      blocked_reason = NULL,
      blocked_details = '{}'::jsonb,
      cooldown_until = NULL,
      updated_at = NOW()
    WHERE id = ANY($1)
    RETURNING id, name, status, enabled, chapter_ingestion_enabled, catalog_discovery_enabled, cooldown_until
  `, [TO_ACTIVATE]);

  console.log(`\nActivated ${activateRes.rowCount} sources:`);
  console.table(activateRes.rows);

  // 3. Remove the 4 dead / unsupported / duplicate sources
  const removeRes = await client.query(`
    DELETE FROM importer_sources
    WHERE id = ANY($1)
    RETURNING id, name
  `, [TO_REMOVE]);

  console.log(`\nRemoved ${removeRes.rowCount} sources from importer_sources:`);
  console.table(removeRes.rows);

  // 4. Snapshot AFTER
  const afterRes = await client.query(`
    SELECT status, enabled, chapter_ingestion_enabled, catalog_discovery_enabled, COUNT(*)::int as count
    FROM importer_sources
    GROUP BY status, enabled, chapter_ingestion_enabled, catalog_discovery_enabled
    ORDER BY status, enabled
  `);
  console.log('\nAFTER counts by status/enabled/ingestion/discovery:');
  console.table(afterRes.rows);

  // 5. Total count of active and disabled sources
  const totalActive = await client.query(`SELECT COUNT(*)::int as count FROM importer_sources WHERE status = 'ACTIVE' AND enabled = true`);
  const totalDisabled = await client.query(`SELECT COUNT(*)::int as count FROM importer_sources WHERE status != 'ACTIVE' OR enabled = false`);
  const totalSources = await client.query(`SELECT COUNT(*)::int as count FROM importer_sources`);

  console.log('\n======================================================================');
  console.log('FINAL IMPORTER SOURCES SUMMARY');
  console.log('======================================================================');
  console.log(`TOTAL SOURCES IN IMPORTER: ${totalSources.rows[0].count}`);
  console.log(`ACTIVE PRODUCTION SOURCES: ${totalActive.rows[0].count}`);
  console.log(`DISABLED SOURCES:          ${totalDisabled.rows[0].count}`);
  console.log('======================================================================');

  await client.end();
}

main().catch(console.error);
