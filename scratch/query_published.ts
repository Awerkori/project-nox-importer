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

pool.query("SELECT count(*), extract(epoch from (now() - max(published_at))) as max_age_sec FROM chapters WHERE published_at > now() - interval '5 minutes'").then(r => { console.table(r.rows); pool.end(); });
