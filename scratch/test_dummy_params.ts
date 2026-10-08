import { Pool } from 'pg';
import * as dotenv from 'dotenv';
dotenv.config();

const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

pool.query(`
  WITH dummy_params AS (SELECT $1::text[], $2::text[])
  SELECT 1 WHERE 1=1
`, [null, null])
  .then(res => { console.table(res.rows); pool.end(); })
  .catch(err => { console.error(err); pool.end(); });
