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

pool.query("SELECT id, status, enabled, cooldown_until, blocked_details FROM importer_sources WHERE enabled = true AND status IN ('ACTIVE', 'COOLDOWN', 'PROBING', 'DEGRADED')").then(r => { console.table(r.rows); pool.end(); }).catch(e => console.log(e));
