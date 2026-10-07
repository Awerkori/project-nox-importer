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
    console.log("=== STAGED BACKLOG DETAILED BREAKDOWN AUDIT ===");

    // 1. STAGED NOW vs 15M AGO
    const totalNowRes = await client.query(`
      SELECT count(*) as count FROM importer_chapter_mappings WHERE status = 'STAGED';
    `);
    const stagedNow = parseInt(totalNowRes.rows[0].count, 10);

    const staged15mRes = await client.query(`
      SELECT count(*) as count FROM importer_chapter_mappings 
      WHERE status = 'STAGED' AND updated_at <= NOW() - INTERVAL '15 minutes';
    `);
    // Alternatively, let's see how many were published in the last 15m from staged:
    const publishedLast15mRes = await client.query(`
      SELECT count(*) as count FROM chapters 
      WHERE published_at >= NOW() - INTERVAL '15 minutes';
    `);

    // Let's get all STAGED chapters with their work and sort_key
    const stagedRowsRes = await client.query(`
      SELECT 
        m.id,
        m.work_id,
        w.title as work_title,
        m.chapter_sort_key,
        m.source,
        m.status,
        m.updated_at
      FROM importer_chapter_mappings m
      JOIN works w ON m.work_id = w.id
      WHERE m.status = 'STAGED'
      ORDER BY m.work_id, m.chapter_sort_key ASC;
    `);
    const stagedRows = stagedRowsRes.rows;

    let readyCount = 0;
    let predecessorQueuedCount = 0;
    let predecessorImportingCount = 0;
    let blockedTemporaryCount = 0;
    let blockedByRealGapCount = 0;

    // Group staged by work
    const workMap = new Map();
    for (const row of stagedRows) {
      if (!workMap.has(row.work_id)) {
        workMap.set(row.work_id, []);
      }
      workMap.get(row.work_id).push(row);
    }

    // For each work, inspect the barrier and predecessor state
    for (const [workId, chapters] of workMap.entries()) {
      // Get published chapters for this work
      const pubRes = await client.query(`
        SELECT number FROM chapters WHERE work_id = $1::uuid AND published_at IS NOT NULL ORDER BY number ASC;
      `, [workId]);
      const publishedNumbers = new Set(pubRes.rows.map(r => parseFloat(r.number)));
      const maxPublished = pubRes.rows.length > 0 ? Math.max(...pubRes.rows.map(r => parseFloat(r.number))) : -1;

      // Get queue status for all jobs of this work
      const queueRes = await client.query(`
        SELECT chapter_sort_key, status, last_error, attempts
        FROM importer_queue
        WHERE (payload->>'workId') = $1
      `, [workId]);
      const queueStatusMap = new Map();
      for (const q of queueRes.rows) {
        queueStatusMap.set(parseFloat(q.chapter_sort_key), q);
      }

      // Check all mappings for this work to find gaps
      const mapRes = await client.query(`
        SELECT chapter_sort_key, status, is_gap
        FROM importer_chapter_mappings
        WHERE work_id = $1::uuid
      `, [workId]);
      const mappingMap = new Map();
      for (const m of mapRes.rows) {
        mappingMap.set(parseFloat(m.chapter_sort_key), m);
      }

      for (const ch of chapters) {
        const sortKey = parseFloat(ch.chapter_sort_key);

        // Predecessor is sortKey - 1 (or lowest missing number before sortKey)
        // Check if there is any missing number < sortKey that is not published
        // If sortKey is contiguous (e.g. maxPublished == sortKey - 1 or all < sortKey are published/gaps):
        // Then it is READY!
        let missingPredecessors = [];
        for (let k = 1; k < sortKey; k++) {
          if (!publishedNumbers.has(k)) {
            // Is it marked as gap?
            const m = mappingMap.get(k);
            if (!m || !m.is_gap) {
              missingPredecessors.push(k);
            }
          }
        }

        if (missingPredecessors.length === 0) {
          readyCount++;
        } else {
          // Look at the earliest missing predecessor
          const firstMissing = missingPredecessors[0];
          const q = queueStatusMap.get(firstMissing);

          if (q) {
            if (q.status === 'IMPORTING') {
              predecessorImportingCount++;
            } else if (q.status === 'QUEUED') {
              predecessorQueuedCount++;
            } else if (q.status === 'RETRY' || q.status === 'BLOCKED_BY_UPSTREAM') {
              blockedTemporaryCount++;
            } else if (q.status === 'FAILED') {
              blockedTemporaryCount++;
            } else {
              blockedTemporaryCount++;
            }
          } else {
            // Not in queue, check if marked gap or truly missing
            const m = mappingMap.get(firstMissing);
            if (m && m.is_gap) {
              blockedByRealGapCount++;
            } else {
              blockedTemporaryCount++;
            }
          }
        }
      }
    }

    console.log(`STAGED NOW: ${stagedNow}`);
    console.log(`STAGED 15M AGO (approx based on 813 before): 813`);
    console.log(`DELTA: ${stagedNow - 813} (${stagedNow < 813 ? 'Draining' : 'Growing'})`);
    console.log(`READY: ${readyCount}`);
    console.log(`BLOCKED TEMPORARY: ${blockedTemporaryCount}`);
    console.log(`BLOCKED BY REAL GAP: ${blockedByRealGapCount}`);
    console.log(`PREDECESSOR QUEUED: ${predecessorQueuedCount}`);
    console.log(`PREDECESSOR IMPORTING: ${predecessorImportingCount}`);
    console.log(`SUM CHECKS: ${readyCount + blockedTemporaryCount + blockedByRealGapCount + predecessorQueuedCount + predecessorImportingCount} vs ${stagedNow}`);

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
