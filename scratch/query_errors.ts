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

pool.query("SELECT data->>'message' as msg, data->>'stack' as stack FROM importer_diagnostic_telemetry WHERE type = 'error' ORDER BY created_at DESC LIMIT 5").then(r => { console.table(r.rows); pool.end(); }).catch(e => console.log(e));
