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
  const t0 = performance.now();
  const res = await pool.query(`
    EXPLAIN ANALYZE
    WITH staged_works AS (
      SELECT work_id, MIN(chapter_sort_key) as min_staged
      FROM importer_chapter_mappings
      WHERE status = 'STAGED' AND work_id IS NOT NULL
      GROUP BY work_id
    )
    SELECT sw.work_id
    FROM staged_works sw
    WHERE sw.min_staged <= COALESCE((
      SELECT MAX(number) FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL
    ), -1) + 1.05
    OR NOT EXISTS (
      SELECT 1 FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL
    )
    LIMIT 40;
  `);
  console.log("Query duration:", (performance.now() - t0).toFixed(2), "ms");
  console.log(res.rows.map(r => r["QUERY PLAN"]).join("\n"));

  const dataRes = await pool.query(`
    WITH staged_works AS (
      SELECT work_id, MIN(chapter_sort_key) as min_staged
      FROM importer_chapter_mappings
      WHERE status = 'STAGED' AND work_id IS NOT NULL
      GROUP BY work_id
    )
    SELECT sw.work_id
    FROM staged_works sw
    WHERE sw.min_staged <= COALESCE((
      SELECT MAX(number) FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL
    ), -1) + 1.05
    OR NOT EXISTS (
      SELECT 1 FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL
    )
    LIMIT 40;
  `);
  console.log("Returned publishable works:", dataRes.rows);

  await pool.end();
}

run().catch(console.error);
