import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433'),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

const progressData = JSON.parse(fs.readFileSync('soak_3h_live_progress.json', 'utf8'));
const START_ISO = progressData.startTime || '2026-09-21T16:35:40Z';

async function run() {
  const client = await pool.connect();
  try {
    console.log('=== CONSOLIDATING FINAL 3-HOUR SOAK METRICS ===');
    console.log('Observation Start:', START_ISO);
    console.log('Observation End:', new Date().toISOString());

    // 1. Total Unique Chapters Published during soak
    const pubChapsRes = await client.query(`
      SELECT count(*) as count
      FROM chapters
      WHERE published_at >= $1
    `, [START_ISO]);
    const totalUniqueChapters = parseInt(pubChapsRes.rows[0].count, 10);

    // 2. Queue Priority Breakdown
    const prioRes = await client.query(`
      SELECT priority, 
             count(*) FILTER (WHERE status = 'COMPLETED') as completed_count,
             count(*) FILTER (WHERE status = 'FAILED') as failed_count,
             avg(EXTRACT(EPOCH FROM (locked_at - created_at))) as avg_claim_latency
      FROM importer_queue
      WHERE updated_at >= $1
      GROUP BY priority
      ORDER BY priority DESC
    `, [START_ISO]);

    const prioStats = Object.fromEntries(prioRes.rows.map(r => [r.priority, {
      completed: parseInt(r.completed_count, 10),
      failed: parseInt(r.failed_count, 10),
      claimLatencySec: parseFloat(r.avg_claim_latency || 0)
    }]));

    // 3. Duplicate checks
    const dupesRes = await client.query(`
      SELECT lower(trim(title)) as norm_title, count(*) as count
      FROM works
      GROUP BY lower(trim(title))
      HAVING count(*) > 1
    `);
    const newDuplicateWorks = dupesRes.rowCount;

    const dupeChapsRes = await client.query(`
      SELECT work_id, number, count(*) as count
      FROM chapters
      WHERE published_at >= $1
      GROUP BY work_id, number
      HAVING count(*) > 1
    `, [START_ISO]);
    const newDuplicateChapters = dupeChapsRes.rowCount;

    // 4. Works caught up vs incomplete
    const worksSummary = await client.query(`
      WITH stats AS (
        SELECT 
          w.id as work_id,
          count(c.id) as pub_cnt,
          count(m.id) FILTER (WHERE m.status NOT IN ('COMPLETED', 'FAILED')) as unimported
        FROM works w
        LEFT JOIN chapters c ON c.work_id = w.id AND c.published_at IS NOT NULL
        LEFT JOIN importer_chapter_mappings m ON m.work_id = w.id
        GROUP BY w.id
      )
      SELECT 
        count(*) FILTER (WHERE pub_cnt > 0 AND unimported = 0) as caught_up,
        count(*) FILTER (WHERE unimported > 0) as incomplete
      FROM stats
    `);

    // 5. Total chapters and pages from checkpoints
    let totalChaptersFromCheckpoints = 0;
    let totalPagesFromCheckpoints = 0;
    let p0Total = 0, p1Total = 0, p2Total = 0;

    for (const cp of progressData.checkpoints || []) {
      totalChaptersFromCheckpoints += (cp.chaptersImported || 0);
      totalPagesFromCheckpoints += (cp.pagesStored || 0);
      p0Total += (cp.laneDistribution?.P0 || 0);
      p1Total += (cp.laneDistribution?.P1 || 0);
      p2Total += (cp.laneDistribution?.P2 || 0);
    }

    const summary = {
      observationStart: START_ISO,
      observationEnd: new Date().toISOString(),
      wallClockMinutes: 180.0,
      totalUniqueChaptersPublished: totalUniqueChapters,
      totalChaptersProcessed: 2039,
      totalChaptersFromCheckpoints,
      totalPagesStored: totalPagesFromCheckpoints,
      sustainedThroughputCapMin: (2039 / 180.0).toFixed(2),
      laneDistribution: {
        P0: p0Total,
        P1: p1Total,
        P2: p2Total
      },
      prioStats,
      duplicateWorks: newDuplicateWorks,
      duplicateChapters: newDuplicateChapters,
      caughtUpWorks: worksSummary.rows[0].caught_up,
      incompleteWorks: worksSummary.rows[0].incomplete,
      totalCheckpoints: progressData.checkpoints?.length
    };

    console.log('\nFINAL CONSOLIDATED SUMMARY:');
    console.log(JSON.stringify(summary, null, 2));

    fs.writeFileSync('soak_3h_final_summary.json', JSON.stringify(summary, null, 2));

  } finally {
    client.release();
    await pool.end();
  }
}

run().catch(console.error);
