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
  const res = await pool.query(`
    WITH staged_works AS (
      SELECT m.work_id, MIN(m.chapter_sort_key) as min_staged, count(*) as staged_cnt
      FROM importer_chapter_mappings m
      WHERE m.status = 'STAGED' AND m.work_id IS NOT NULL
      GROUP BY m.work_id
    )
    SELECT 
      sw.work_id,
      w.title,
      sw.staged_cnt,
      sw.min_staged,
      COALESCE((SELECT MAX(number) FROM chapters c WHERE c.work_id = sw.work_id AND c.published_at IS NOT NULL), -1) as max_pub
    FROM staged_works sw
    JOIN works w ON sw.work_id = w.id
    ORDER BY sw.staged_cnt DESC;
  `);
  console.log("All staged works count:", res.rows.length);
  for (const r of res.rows) {
    const diff = (parseFloat(r.min_staged) - parseFloat(r.max_pub)).toFixed(2);
    console.log(`${r.title ? r.title.slice(0, 30).padEnd(30) : 'Untitled'.padEnd(30)} | staged: ${r.staged_cnt.toString().padStart(3)} | max_pub: ${r.max_pub.toString().padStart(7)} | min_staged: ${r.min_staged.toString().padStart(7)} | gap: ${diff}`);
  }
  await pool.end();
}

run().catch(console.error);
