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

pool.query("SELECT id, status, task_type, payload->>'error' as error, extract(epoch from (now() - updated_at)) as age_sec FROM importer_queue WHERE status IN ('FAILED', 'RETRY') AND updated_at > now() - interval '5 minutes' ORDER BY updated_at DESC LIMIT 10").then(r => { console.table(r.rows); pool.end(); });
