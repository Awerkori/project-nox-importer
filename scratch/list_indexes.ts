import { Pool } from 'pg';
import dotenv from 'dotenv';
dotenv.config();

async function main() {
  const pool = new Pool({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433'),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });
  
  try {
    const res = await pool.query(`SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'importer_queue'`);
    console.log(res.rows.map(r => r.indexdef).join('\n'));
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}
main();
