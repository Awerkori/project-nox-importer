import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  // Let us check what importer_sources status is!
  const srcRes = await pool.query(`
    SELECT id, enabled, chapter_ingestion_enabled, status, cooldown_until
    FROM importer_sources
    WHERE chapter_ingestion_enabled = true;
  `);
  console.log("Sources with chapter_ingestion_enabled:");
  console.table(srcRes.rows);

  await pool.end();
}

main().catch(console.error);
