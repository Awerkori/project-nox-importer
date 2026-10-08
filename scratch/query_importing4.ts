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

pool.query("SELECT id, source, task_type, extract(epoch from (now() - updated_at)) as update_age, extract(epoch from (now() - (payload->>'startedAt')::timestamp)) as start_age FROM importer_queue WHERE status = 'IMPORTING'").then(r => { console.table(r.rows); pool.end(); });
