import pg from 'pg';
import dotenv from 'dotenv';
dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

import { DirectSupabaseClient } from '../build/db/direct-supabase-client.js';
import { PublicationBarrier } from '../build/core/publication.js';
import { confirmUpstreamGapInterval } from '../build/core/gap-validator.js';

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false },
  max: 5,
  connectionTimeoutMillis: 10000,
});

async function main() {
  console.log('============================================================');
  console.log('PROJECT NOX — CANONICAL GAP RECONCILIATION & CASCADE RUNNER');
  console.log('============================================================\n');

  const supabaseClient = new DirectSupabaseClient(pool);
  const publicationBarrier = new PublicationBarrier(supabaseClient);

  // 1. Initial snapshot
  const beforeStagedRes = await pool.query("SELECT COUNT(*) as cnt FROM importer_chapter_mappings WHERE status = 'STAGED'");
  const beforeWaitingRes = await pool.query("SELECT COUNT(*) as cnt FROM importer_chapter_mappings WHERE status = 'WAITING_FOR_GAP'");
  const beforeGapsRes = await pool.query("SELECT COUNT(*) as cnt FROM importer_confirmed_gaps");

  const stagedBefore = parseInt(beforeStagedRes.rows[0].cnt, 10);
  const waitingBefore = parseInt(beforeWaitingRes.rows[0].cnt, 10);
  const gapsBefore = parseInt(beforeGapsRes.rows[0].cnt, 10);

  console.log(`[BEFORE SNAPSHOT]`);
  console.log(`  STAGED MAPPINGS:         ${stagedBefore}`);
  console.log(`  WAITING_FOR_GAP:         ${waitingBefore}`);
  console.log(`  CONFIRMED GAPS IN DB:    ${gapsBefore}\n`);

  // 2. Query distinct works with STAGED or WAITING_FOR_GAP mappings
  const worksRes = await pool.query(`
    SELECT DISTINCT m.work_id, w.title, m.source
    FROM importer_chapter_mappings m
    JOIN works w ON w.id = m.work_id
    WHERE m.status IN ('STAGED', 'WAITING_FOR_GAP')
    ORDER BY w.title ASC;
  `);

  console.log(`Found ${worksRes.rows.length} works with STAGED/WAITING_FOR_GAP mappings to evaluate.\n`);

  const confirmedGapsCreated = [];
  const altSourcePredecessors = [];
  const queuePredecessors = [];
  const publishedWorks = [];
  const unresolvedWorks = [];

  for (const row of worksRes.rows) {
    const { work_id: workId, title, source: primarySource } = row;

    try {
      // Find max published chapter
      const maxPubRes = await pool.query(
        "SELECT COALESCE(MAX(number), -1) as max_pub FROM chapters WHERE work_id = $1::uuid AND published_at IS NOT NULL",
        [workId]
      );
      const rawMax = maxPubRes.rows[0]?.max_pub;
      const maxPub = rawMax !== null && rawMax !== undefined ? parseFloat(rawMax) : -1;

      // Find lowest sort key in STAGED/WAITING_FOR_GAP
      const stagedListRes = await pool.query(`
        SELECT chapter_id, chapter_sort_key, chapter_number, status, source
        FROM importer_chapter_mappings
        WHERE work_id = $1::uuid AND status IN ('STAGED', 'WAITING_FOR_GAP')
        ORDER BY chapter_sort_key ASC;
      `, [workId]);

      if (stagedListRes.rows.length === 0) continue;

      const firstStaged = stagedListRes.rows[0];
      const minStaged = parseFloat(firstStaged.chapter_sort_key);

      // Check if there is an upstream gap before minStaged
      if (minStaged > maxPub + 1.05) {
        const gapStart = maxPub >= 0 ? Math.floor(maxPub) + 1 : 1;
        const gapEnd = Math.floor(minStaged - 0.001);

        if (gapStart > gapEnd) {
          console.log(`Work "${title}" has fractional gap without integer missing chapters (maxPub=${maxPub}, minStaged=${minStaged}). Running cascade.`);
          const cascadeCount = await publicationBarrier.runCascade(workId, 200);
          if (cascadeCount > 0) {
            console.log(`  🚀 CASCADE: ${cascadeCount} chapters published for "${title}"!`);
            publishedWorks.push({ title, workId, cascadeCount });
          }
          continue;
        }

        console.log(`Evaluating work: "${title}" (${workId})`);
        console.log(`  maxPub: ${maxPub}, minStaged: ${minStaged} -> Gap interval: [${gapStart}..${gapEnd}] on ${primarySource}`);

        const gapResult = await confirmUpstreamGapInterval(pool, {
          workId,
          startSortKey: gapStart,
          endSortKey: gapEnd,
          primarySource,
          reason: `RECONCILIATION_AUDITED_GAP_${gapStart}_TO_${gapEnd}`,
        });

        if (gapResult.confirmed) {
          console.log(`  ✅ CONFIRMED STRUCTURAL GAP: [${gapStart}..${gapEnd}] registered in importer_confirmed_gaps.`);
          confirmedGapsCreated.push({
            workId,
            title,
            gapStart,
            gapEnd,
            primarySource,
          });

          // Unblock WAITING_FOR_GAP to STAGED
          await pool.query(`
            UPDATE importer_chapter_mappings
            SET status = 'STAGED', updated_at = NOW()
            WHERE work_id = $1::uuid AND status = 'WAITING_FOR_GAP';
          `, [workId]);

          // Trigger publication barrier cascade
          const cascadeCount = await publicationBarrier.runCascade(workId, 200);
          console.log(`  🚀 CASCADE RESULT: ${cascadeCount} chapters published for "${title}"!`);
          if (cascadeCount > 0) {
            publishedWorks.push({ title, workId, cascadeCount });
          }
        } else if (gapResult.alternativeSourceFound) {
          const alt = gapResult.alternativeSourceFound;
          if (gapResult.reason.includes('JOB_EXISTS_IN_QUEUE')) {
            console.log(`  ℹ️ Predecessor exists in queue: Ch ${alt.chapterSortKey} on ${alt.source}. Boosting to priority 95.`);
            queuePredecessors.push({ workId, title, chapterSortKey: alt.chapterSortKey, source: alt.source });
            await pool.query(`
              UPDATE importer_queue
              SET priority = 95, next_run_at = NOW(), updated_at = NOW()
              WHERE (payload->>'workId') = $1
                AND chapter_sort_key = $2
                AND status IN ('QUEUED', 'RETRY');
            `, [workId, alt.chapterSortKey]);
          } else {
            console.log(`  🔍 Predecessor found in alternative source: Ch ${alt.chapterSortKey} on ${alt.source}. Prioritizing.`);
            altSourcePredecessors.push({ workId, title, chapterSortKey: alt.chapterSortKey, source: alt.source });
            await pool.query(`
              UPDATE importer_queue
              SET priority = 95, next_run_at = NOW(), updated_at = NOW()
              WHERE (payload->>'workId') = $1
                AND chapter_sort_key = $2
                AND status IN ('QUEUED', 'RETRY', 'PAUSED_BY_STAFF');
            `, [workId, alt.chapterSortKey]);
          }
        } else {
          console.log(`  ⚠️ UNRESOLVED: ${gapResult.reason}`);
          unresolvedWorks.push({ workId, title, reason: gapResult.reason });
        }
      } else {
        // Gap already resolved or consecutive (minStaged <= maxPub + 1.05)
        console.log(`Work "${title}" has consecutive chapters (maxPub=${maxPub}, minStaged=${minStaged}). Running cascade.`);
        const cascadeCount = await publicationBarrier.runCascade(workId, 200);
        if (cascadeCount > 0) {
          console.log(`  🚀 CASCADE: ${cascadeCount} chapters published for "${title}"!`);
          publishedWorks.push({ title, workId, cascadeCount });
        }
      }
    } catch (err) {
      console.error(`Error processing work "${title}" (${workId}):`, err.message);
    }
  }

  // 3. Final snapshot
  const afterStagedRes = await pool.query("SELECT COUNT(*) as cnt FROM importer_chapter_mappings WHERE status = 'STAGED'");
  const afterWaitingRes = await pool.query("SELECT COUNT(*) as cnt FROM importer_chapter_mappings WHERE status = 'WAITING_FOR_GAP'");
  const afterGapsRes = await pool.query("SELECT COUNT(*) as cnt FROM importer_confirmed_gaps");

  const stagedAfter = parseInt(afterStagedRes.rows[0].cnt, 10);
  const waitingAfter = parseInt(afterWaitingRes.rows[0].cnt, 10);
  const gapsAfter = parseInt(afterGapsRes.rows[0].cnt, 10);

  console.log('\n============================================================');
  console.log('FINAL RECONCILIATION REPORT (SECTION 13)');
  console.log('============================================================');
  console.log(`STAGED BEFORE:                     ${stagedBefore}`);
  console.log(`STAGED AFTER:                      ${stagedAfter} (Reduction: -${stagedBefore - stagedAfter})`);
  console.log(`WAITING_FOR_GAP BEFORE:            ${waitingBefore}`);
  console.log(`WAITING_FOR_GAP AFTER:             ${waitingAfter} (Reduction: -${waitingBefore - waitingAfter})`);
  console.log(`CONFIRMED GAPS CRIADOS:            ${confirmedGapsCreated.length}`);
  console.log(`PREDECESSORES EM OUTRA FONTE:      ${altSourcePredecessors.length}`);
  console.log(`PREDECESSORES PRIORIZADOS NA FILA: ${queuePredecessors.length}`);
  console.log(`TOTAL OBRAS COM CASCADE PUBLICADO: ${publishedWorks.length}`);
  console.log(`TOTAL CAPÍTULOS PUBLICADOS:        ${publishedWorks.reduce((acc, w) => acc + w.cascadeCount, 0)}`);
  console.log(`OBRAS COM GAP ILEGÍTIMO/PENDENTE:  ${unresolvedWorks.length}`);
  console.log('============================================================\n');

  if (confirmedGapsCreated.length > 0) {
    console.log('Confirmed Gaps Detail:');
    console.table(confirmedGapsCreated);
  }

  if (altSourcePredecessors.length > 0) {
    console.log('Alternative Source Predecessors Detail:');
    console.table(altSourcePredecessors);
  }

  if (unresolvedWorks.length > 0) {
    console.log('Unresolved Works Detail:');
    console.table(unresolvedWorks);
  }

  await pool.end();
}

main().catch((err) => {
  console.error('Fatal error during reconciliation:', err);
  process.exit(1);
});
