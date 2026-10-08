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

pool.query("SELECT id, status, task_type, attempts, last_error FROM importer_queue WHERE status = 'FAILED' ORDER BY updated_at DESC LIMIT 5").then(r => { console.dir(r.rows, {depth: null}); pool.end(); });
