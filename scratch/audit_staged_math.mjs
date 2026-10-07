import dotenv from "dotenv";
import pg from "pg";
dotenv.config({ path: "/home/awerkori/.Projects/project-nox-importer/.env" });
const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || "5433", 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});
await client.connect();

// Current total STAGED
const currentStagedRes = await client.query(`
  SELECT count(*)::int as total
  FROM importer_chapter_mappings
  WHERE status = 'STAGED';
`);
const currentStaged = currentStagedRes.rows[0].total;

// Age breakdown of current STAGED
const ageRes = await client.query(`
  SELECT 
    count(*)::int as total,
    count(*) FILTER (WHERE updated_at <= NOW() - INTERVAL '5 minutes')::int as older_5m,
    count(*) FILTER (WHERE updated_at <= NOW() - INTERVAL '15 minutes')::int as older_15m,
    count(*) FILTER (WHERE updated_at <= NOW() - INTERVAL '30 minutes')::int as older_30m,
    count(*) FILTER (WHERE updated_at <= NOW() - INTERVAL '60 minutes')::int as older_60m,
    min(updated_at) as oldest_staged_at
  FROM importer_chapter_mappings
  WHERE status = 'STAGED';
`);

// Chapters with chapters table published in the last 15 minutes that have a mapping
const publishedFromStagedRes = await client.query(`
  SELECT count(*)::int as cnt
  FROM chapters c
  JOIN importer_chapter_mappings m ON m.chapter_id = c.id
  WHERE c.published_at >= NOW() - INTERVAL '15 minutes'
    AND c.created_at < c.published_at - INTERVAL '10 seconds';
`);

// Chapters directly published on ingest (created_at close to published_at)
const directPublishedRes = await client.query(`
  SELECT count(*)::int as cnt
  FROM chapters c
  JOIN importer_chapter_mappings m ON m.chapter_id = c.id
  WHERE c.published_at >= NOW() - INTERVAL '15 minutes'
    AND c.created_at >= c.published_at - INTERVAL '10 seconds';
`);

// Works currently with STAGED chapters
const worksStagedRes = await client.query(`
  SELECT w.id, w.title, count(m.id)::int as staged_cnt, min(m.chapter_sort_key) as min_key, max(m.chapter_sort_key) as max_key
  FROM importer_chapter_mappings m
  JOIN works w ON w.id = m.work_id
  WHERE m.status = 'STAGED'
  GROUP BY w.id, w.title
  ORDER BY staged_cnt DESC;
`);

console.log("Current STAGED total:", currentStaged);
console.log("STAGED Age breakdown:", ageRes.rows[0]);
console.log("Published from STAGED (15m):", publishedFromStagedRes.rows[0].cnt);
console.log("Published directly on ingest (15m):", directPublishedRes.rows[0].cnt);
console.log("Works with STAGED:", worksStagedRes.rows);

await client.end();
