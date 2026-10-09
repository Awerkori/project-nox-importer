import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
});

async function main() {
  const res = await pool.query(`
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'importer_queue';
  `);
  console.table(res.rows);
  pool.end();
}
main().catch(console.error);
