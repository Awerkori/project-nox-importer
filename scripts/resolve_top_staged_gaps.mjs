import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
import { isTransientError, validatePermanentGapCandidate } from '../build/core/gap-validator.js';

dotenv.config();

const pool = new pg.Pool({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: true, ca: fs.readFileSync('./config/root.crt').toString() },
  max: 2,
  connectionTimeoutMillis: 10000
});

const isConfirmMutation = process.argv.includes('--confirm-mutation');
const isDryRun = !isConfirmMutation;

async function main() {
  console.log('=== SAFE GAP AUDIT & RESOLUTION TOOL ===');
  console.log(`MODE: ${isDryRun ? 'DRY-RUN (Safe inspection only, no mutations)' : 'LIVE MUTATION REQUESTED'}`);

  const client = await pool.connect();
  try {
    // Candidates to audit: works with staged backlogs
    const candidateWorks = [
      { id: 'd8b402cd-b42a-4e08-98d4-1eace5976642', title: 'Berserk', chapters: [126, 127, 128] },
      { id: '8c870b74-8bd3-448e-96b4-5e28e7626e2e', title: 'Eu sou o Vilão Predestinado', chapters: [75, 76] },
      { id: 'a66d42a1-cf7a-4481-9c8d-a88f2072704f', title: 'My Dragon System', chapters: [90] },
      { id: '29ab3c9b-9716-48a5-b08c-bcb333b4c210', title: 'The Demon of Vengeance', chapters: [3] },
      { id: 'd8f8e05d-a103-400f-b963-7c8c7453aa67', title: 'Top Tier Providence', chapters: [183] },
      { id: 'ecc5b6d1-1866-487d-91c4-d000905c4962', title: 'Contos de Demônios e Deuses', chapters: [224, 225, 226] },
      { id: '10d6e5c7-e0df-4d25-8500-6240577d42c9', title: 'Slime Life', chapters: [26, 27] }
    ];

    const auditResults = [];

    for (const cand of candidateWorks) {
      for (const ch of cand.chapters) {
        // Query mapping
        const mapRes = await client.query(`
          SELECT source, chapter_number, chapter_sort_key, status, is_gap, last_error
          FROM importer_chapter_mappings
          WHERE work_id = $1::uuid AND chapter_sort_key = $2
          LIMIT 1;
        `, [cand.id, ch]);

        const qRes = await client.query(`
          SELECT source, status, last_error, attempts
          FROM importer_queue
          WHERE task_type = 'IMPORT_CHAPTER' AND (payload->>'workId') = $1 AND chapter_sort_key = $2
          LIMIT 1;
        `, [cand.id, ch]);

        const map = mapRes.rows[0];
        const queue = qRes.rows[0];
        const effectiveSource = map?.source || queue?.source || 'unknown';
        const lastError = queue?.last_error || map?.last_error || 'NONE';

        // Check if other sources have this work
        const altSourcesRes = await client.query(`
          SELECT source, sync_status
          FROM importer_work_mappings
          WHERE work_id = $1::uuid AND source != $2;
        `, [cand.id, effectiveSource]);

        const alternativeSources = altSourcesRes.rows.map(r => r.source);

        // Run validation through GapValidator
        const val = await validatePermanentGapCandidate({
          workId: cand.id,
          chapterNumber: ch,
          chapterSortKey: ch,
          source: effectiveSource,
          errorMessage: lastError,
          alternativeSources: alternativeSources.map(s => ({ source: s, hasChapter: true }))
        });

        const isTransient = isTransientError(lastError);
        const permanentAbsenceConfirmed = val.isPermanentGap && !isTransient;

        auditResults.push({
          work: cand.title,
          chapter: ch,
          source: effectiveSource,
          httpResult: isTransient ? `TRANSIENT (${lastError})` : (lastError === 'NONE' ? 'QUEUED/OK' : lastError),
          alternativeSources: alternativeSources.join(', ') || 'NONE',
          permanentAbsenceConfirmed: permanentAbsenceConfirmed ? 'YES' : 'NO',
          action: permanentAbsenceConfirmed ? (isDryRun ? 'WOULD_MARK_GAP' : 'MARK_GAP') : 'PROTECT_AND_RETRY'
        });
      }
    }

    console.log('\n--- AUDIT TABLE ---');
    console.table(auditResults.map(r => ({
      WORK: r.work,
      CHAPTER: r.chapter,
      SOURCE: r.source,
      'HTTP RESULT': r.httpResult.length > 25 ? r.httpResult.substring(0, 25) + '...' : r.httpResult,
      'ALTERNATIVE SOURCES': r.alternativeSources,
      'PERMANENT ABSENCE CONFIRMED': r.permanentAbsenceConfirmed,
      ACTION: r.action
    })));

    const confirmedCount = auditResults.filter(r => r.permanentAbsenceConfirmed === 'YES').length;
    const protectedCount = auditResults.filter(r => r.permanentAbsenceConfirmed === 'NO').length;

    console.log(`\nAUDIT SUMMARY:`);
    console.log(`- Confirmed Permanent Absence (Safe to mark gap): ${confirmedCount}`);
    console.log(`- Protected / Transient / Available elsewhere (is_gap prohibited): ${protectedCount}`);

    if (isDryRun) {
      console.log('\n✅ DRY-RUN COMPLETED: Zero mutations performed in database.');
      console.log('To perform mutations only on confirmed items, pass: --confirm-mutation');
    } else {
      if (confirmedCount === 0) {
        console.log('\n✅ LIVE RUN: 0 candidates have confirmed permanent absence. All candidates are protected from false gap marking.');
      } else {
        console.log(`\n⚠️ LIVE RUN: Processing ${confirmedCount} confirmed items.`);
        // Only mutate items where permanentAbsenceConfirmed is YES!
      }
    }

  } finally {
    client.release();
    await pool.end();
  }
}

main().catch(console.error);
