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

pool.query("SELECT created_at, (data->'acquire'->'sqlExecMs'->>'p50')::numeric as sql_p50, (data->'acquire'->'queriesCount'->>'p50')::numeric as queries_p50 FROM importer_diagnostic_telemetry WHERE type = 'engine_heartbeat' ORDER BY created_at DESC LIMIT 10").then(r => { console.table(r.rows); pool.end(); });
