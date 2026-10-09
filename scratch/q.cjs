require('dotenv').config();
const { Pool } = require('pg');
const pool = new Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

pool.query("SELECT value FROM settings WHERE key = 'importer_heartbeat'")
  .then(res => { console.log(JSON.stringify(JSON.parse(res.rows[0].value), null, 2)); return pool.end(); })
  .catch(console.error);
