import dotenv from 'dotenv';
import pg from 'pg';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

async function main() {
  const client = new pg.Client({
    host: process.env.YUGABYTE_HOST,
    port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
    user: process.env.YUGABYTE_USER,
    password: process.env.YUGABYTE_PASSWORD,
    database: process.env.YUGABYTE_DATABASE,
    ssl: { rejectUnauthorized: false }
  });

  await client.connect();

  // 1. STAGED breakdown
  const stagedRes = await client.query(`
    SELECT 
      count(*)::int as total,
      count(*) FILTER (WHERE updated_at <= NOW() - INTERVAL '5 minutes') as older_5m,
      count(*) FILTER (WHERE updated_at <= NOW() - INTERVAL '15 minutes') as older_15m,
      count(*) FILTER (WHERE updated_at <= NOW() - INTERVAL '30 minutes') as older_30m,
      count(*) FILTER (WHERE updated_at <= NOW() - INTERVAL '60 minutes') as older_60m
    FROM importer_chapter_mappings
    WHERE status = 'STAGED';
  `);

  // Staged activity in last 5m
  const stagedActivity = await client.query(`
    SELECT 
      count(*) FILTER (WHERE status = 'STAGED' AND updated_at >= NOW() - INTERVAL '5 minutes') as entered_staged_5m,
      count(*) FILTER (WHERE status = 'COMPLETED' AND updated_at >= NOW() - INTERVAL '5 minutes') as left_staged_5m
    FROM importer_chapter_mappings;
  `);

  const stagedPub5m = await client.query(`
    SELECT count(*)::int as published_from_staged_5m
    FROM chapters
    WHERE published_at >= NOW() - INTERVAL '5 minutes';
  `);

  // Distinct works with staged chapters
  const stagedWorksRes = await client.query(`
    SELECT count(DISTINCT work_id)::int as works_with_staged
    FROM importer_chapter_mappings
    WHERE status = 'STAGED';
  `);

  // Explicit gaps vs bugged frontier
  const explicitGapsRes = await client.query(`
    SELECT count(*)::int as count FROM importer_chapter_mappings WHERE is_gap = true;
  `);

  // Works blocked by gap (where status is WAITING_FOR_GAP or lowest staged has a gap)
  const waitingForGapRes = await client.query(`
    SELECT count(DISTINCT work_id)::int as works_waiting_gap
    FROM importer_chapter_mappings
    WHERE status = 'WAITING_FOR_GAP';
  `);

  // 2. Catalog convergence
  // Published works incomplete (having pending/queued mappings) vs caught up
  const incompleteWorksRes = await client.query(`
    SELECT 
      count(DISTINCT w.id) FILTER (WHERE EXISTS (
        SELECT 1 FROM importer_chapter_mappings m WHERE m.work_id = w.id AND m.status IN ('PENDING', 'QUEUED', 'IMPORTING')
      )) as incomplete_count,
      count(DISTINCT w.id) FILTER (WHERE NOT EXISTS (
        SELECT 1 FROM importer_chapter_mappings m WHERE m.work_id = w.id AND m.status IN ('PENDING', 'QUEUED', 'IMPORTING')
      )) as caught_up_count
    FROM works w
    WHERE w.published = true;
  `);

  // P1 works completed vs P2 works completed
  const cohortRes = await client.query(`
    SELECT 
      count(*) FILTER (WHERE sync_status = 'WAITING_ADMISSION') as p3_waiting_admission,
      count(*) FILTER (WHERE sync_status = 'ACTIVE') as active_cohort
    FROM importer_work_mappings;
  `);

  // 3. Duplicate checks
  const dupTitles = await client.query(`
    SELECT lower(title) as norm_title, count(*)::int as cnt
    FROM works
    WHERE created_at >= '2026-09-21 16:47:00'
    GROUP BY lower(title)
    HAVING count(*) > 1;
  `);

  const dupChapters = await client.query(`
    SELECT work_id, number, count(*)::int as cnt
    FROM chapters
    WHERE created_at >= '2026-09-21 16:47:00'
    GROUP BY work_id, number
    HAVING count(*) > 1;
  `);

  // 4. Slowest query & stats
  const slowQueries = await client.query(`
    SELECT query, calls, total_exec_time, mean_exec_time
    FROM pg_stat_statements
    ORDER BY mean_exec_time DESC
    LIMIT 3;
  `).catch(() => ({ rows: [] }));

  await client.end();

  console.log(JSON.stringify({
    staged: stagedRes.rows[0],
    stagedActivity: stagedActivity.rows[0],
    publishedFromStaged5m: stagedPub5m.rows[0].published_from_staged_5m,
    worksBlockedByGap: waitingForGapRes.rows[0].works_waiting_gap,
    worksWithStaged: stagedWorksRes.rows[0].works_with_staged,
    explicitGaps: explicitGapsRes.rows[0].count,
    catalogConvergence: incompleteWorksRes.rows[0],
    cohort: cohortRes.rows[0],
    duplicateWorksCreated: dupTitles.rows,
    duplicateChaptersCreated: dupChapters.rows,
    slowestQueries: slowQueries.rows
  }, null, 2));
}

main().catch(console.error);
