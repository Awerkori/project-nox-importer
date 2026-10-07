import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: true, ca: fs.readFileSync('./config/root.crt').toString() }
});

async function main() {
  const client = await pool.connect();
  try {
    const stagedRes = await client.query(`
      SELECT m.id, m.work_id, w.title, m.chapter_sort_key, m.source, m.status
      FROM importer_chapter_mappings m
      JOIN works w ON m.work_id = w.id
      WHERE m.status = 'STAGED'
      ORDER BY m.work_id, m.chapter_sort_key ASC;
    `);

    const staged = stagedRes.rows;
    console.log(`Total STAGED mappings: ${staged.length}`);

    // Preload for each distinct work_id:
    // 1. max published chapter number
    // 2. all published chapter numbers
    // 3. all queue entries
    // 4. all mappings with is_gap
    const workIds = [...new Set(staged.map(s => s.work_id))];

    const pubRes = await client.query(`
      SELECT work_id, number
      FROM chapters
      WHERE work_id = ANY($1::uuid[]) AND published_at IS NOT NULL;
    `, [workIds]);

    const queueRes = await client.query(`
      SELECT (payload->>'workId') as work_id, chapter_sort_key, status
      FROM importer_queue
      WHERE (payload->>'workId') = ANY($1);
    `, [workIds]);

    const gapRes = await client.query(`
      SELECT work_id, chapter_sort_key, is_gap
      FROM importer_chapter_mappings
      WHERE work_id = ANY($1::uuid[]) AND is_gap = true;
    `, [workIds]);

    const pubMap = new Map();
    for (const r of pubRes.rows) {
      if (!pubMap.has(r.work_id)) pubMap.set(r.work_id, new Set());
      pubMap.get(r.work_id).add(parseFloat(r.number));
    }

    const queueMap = new Map();
    for (const r of queueRes.rows) {
      const key = `${r.work_id}:${parseFloat(r.chapter_sort_key)}`;
      queueMap.set(key, r.status);
    }

    const gapMap = new Map();
    for (const r of gapRes.rows) {
      const key = `${r.work_id}:${parseFloat(r.chapter_sort_key)}`;
      gapMap.set(key, true);
    }

    let ready = 0;
    let blockedTemp = 0;
    let blockedRealGap = 0;
    let predQueued = 0;
    let predImporting = 0;

    for (const s of staged) {
      const workPubs = pubMap.get(s.work_id) || new Set();
      const sortKey = parseFloat(s.chapter_sort_key);

      // What is the immediate predecessor?
      // Predecessor is sortKey - 1 (or the largest missing number < sortKey)
      let predecessorKey = sortKey - 1;
      
      // If sortKey <= 1, predecessor is 0 or none
      if (sortKey <= 1) {
        ready++;
        continue;
      }

      // Check if all chapters 1..sortKey-1 are published or real gaps
      let firstMissing = null;
      for (let k = 1; k < sortKey; k++) {
        if (!workPubs.has(k)) {
          const isGap = gapMap.get(`${s.work_id}:${k}`);
          if (!isGap) {
            firstMissing = k;
            break;
          }
        }
      }

      if (firstMissing === null) {
        ready++;
      } else {
        const qStatus = queueMap.get(`${s.work_id}:${firstMissing}`);
        if (qStatus === 'IMPORTING') {
          predImporting++;
        } else if (qStatus === 'QUEUED') {
          predQueued++;
        } else if (gapMap.get(`${s.work_id}:${firstMissing}`)) {
          blockedRealGap++;
        } else {
          // If in RETRY, BLOCKED_BY_UPSTREAM, FAILED, or waiting in source cooldown
          blockedTemp++;
        }
      }
    }

    console.log(`STAGED NOW: ${staged.length}`);
    console.log(`READY: ${ready}`);
    console.log(`BLOCKED TEMPORARY: ${blockedTemp}`);
    console.log(`BLOCKED BY REAL GAP: ${blockedRealGap}`);
    console.log(`PREDECESSOR QUEUED: ${predQueued}`);
    console.log(`PREDECESSOR IMPORTING: ${predImporting}`);
    console.log(`TOTAL: ${ready + blockedTemp + blockedRealGap + predQueued + predImporting}`);

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
