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
WHERE indexname LIKE 'idx_importer_queue_p1%'
`).then(res => { console.log(res.rows); pool.end(); })
