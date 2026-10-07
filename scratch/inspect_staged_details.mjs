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

const chs = await client.query(`
  SELECT m.id, m.work_id, w.title, m.chapter_number, m.chapter_sort_key, m.status as map_status, m.updated_at as map_updated,
         c.id as chapter_id, c.published_at, c.created_at as ch_created
  FROM importer_chapter_mappings m
  JOIN works w ON w.id = m.work_id
  LEFT JOIN chapters c ON c.work_id = m.work_id AND c.number = m.chapter_number
  WHERE (w.title ILIKE '%Mestre Supremo%' AND m.chapter_number = '311.00')
     OR (w.title ILIKE '%Desire BL%Ex Sponsor%' AND m.chapter_number = '155.00')
     OR (w.title ILIKE '%Slime Life%' AND m.chapter_number = '19.00')
     OR (w.title ILIKE '%Pico Marcial%' AND m.chapter_number = '269.00')
     OR (w.title ILIKE '%Berserk%' AND m.chapter_number IN ('99.00', '100.00', '101.00'));
`);

console.log(JSON.stringify(chs.rows, null, 2));

await client.end();
