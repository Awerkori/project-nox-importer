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
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename IN ('chapters', 'importer_confirmed_gaps')
`).then(res => { console.log(res.rows); pool.end(); })
  .catch(err => { console.error(err); pool.end(); });
