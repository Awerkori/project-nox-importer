import pg from "pg";
import fs from "fs";

const envVars = Object.fromEntries(
  fs.readFileSync("/home/awerkori/.config/project-nox/yugabyte.env", "utf8")
    .split("\n")
    .filter(l => l.includes("=") && !l.startsWith("#"))
    .map(l => [l.split("=")[0].trim(), l.substring(l.indexOf("=") + 1).trim().replace(/^['"]|['"]$/g, "")])
);

const pool = new pg.Pool({
  host: envVars.YUGABYTE_HOST,
  port: parseInt(envVars.YUGABYTE_PORT || "5433", 10),
  user: envVars.YUGABYTE_USER,
  password: envVars.YUGABYTE_PASSWORD,
  database: envVars.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 2,
  connectionTimeoutMillis: 10000
});

async function run() {
  const p = await pool.query(`
    SELECT
      c.id,
      w.title,
      c.number,
      c.published_at,
      EXTRACT(EPOCH FROM (NOW() - c.published_at)) / 60 as mins_ago
    FROM chapters c
    JOIN works w ON c.work_id = w.id
    WHERE c.published_at IS NOT NULL
    ORDER BY c.published_at DESC
    LIMIT 10;
  `);
  console.log("Recent published chapters:", p.rows);

  const stats = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '5 minutes') as pub_5m,
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '15 minutes') as pub_15m,
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '30 minutes') as pub_30m,
      COUNT(*) FILTER (WHERE published_at >= NOW() - INTERVAL '60 minutes') as pub_60m
    FROM chapters;
  `);
  console.log("Rolling published counts:", stats.rows[0]);
  await pool.end();
}

run().catch(console.error);
