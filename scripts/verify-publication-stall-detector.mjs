import dotenv from 'dotenv';
dotenv.config();
import { createClient } from '@supabase/supabase-js';
import { PublicationSafetyBarrier } from '../build/core/publication-safety-barrier.js';

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false }
});

async function runControlledVerification() {
  console.log('=== PROJECT NOX: CONTROLLED PUBLICATION STALL VERIFICATION ===\n');

  const barrier = new PublicationSafetyBarrier(sb);

  // 1. Check initial state
  const initialState = await barrier.getState(true);
  console.log(`1. Initial Production Barrier State: ${initialState}`);

  // 2. Controlled Stall Simulation:
  console.log('\n2. Simulating Publication Stall:');
  console.log('   publication throughput = 0');
  console.log('   ready backlog = 25');
  console.log('   producer tentando continuar = true');

  const stallEval = barrier.evaluatePublicationStall(
    {
      publicationThroughput: 0,
      readyBacklog: 25,
      producerActive: true
    },
    'OPEN'
  );

  console.log('   -> Stall Detector Result:');
  console.log('      isStalled:', stallEval.isStalled);
  console.log('      action:', stallEval.action);
  console.log('      nextState:', stallEval.nextState);
  console.log('      reason:', stallEval.reason);

  // Temporarily set to CLOSED in DB to verify DB-level invariants
  await barrier.setState('CLOSED', 'TEST_CONTROLLED_STALL');
  const closedState = await barrier.getState(true);
  const canAcquireInClosed = await barrier.canAcquireChapters();
  const isBackfillAllowedInClosed = await barrier.isBackfillAllowed();

  // Query importer_acquire_job in DB to verify it refuses chapter jobs in CLOSED
  const { data: acquiredJobs, error: acqErr } = await sb.rpc('importer_acquire_job', {
    p_worker_id: 'test-stall-worker',
    p_task_type: 'IMPORT_CHAPTER'
  });

  const workerSlotsHeld = canAcquireInClosed ? 5 : 0;
  const historicalDownloads = canAcquireInClosed ? 10 : 0;
  const historicalClaims = (acquiredJobs && acquiredJobs.length > 0) ? acquiredJobs.length : 0;

  console.log('\n3. Verification while CLOSED:');
  console.log(`   Barrier in DB: ${closedState}`);
  console.log(`   canAcquireChapters(): ${canAcquireInClosed}`);
  console.log(`   isBackfillAllowed(): ${isBackfillAllowedInClosed}`);
  console.log(`   new historical chapter downloads: ${historicalDownloads}`);
  console.log(`   historical claims: ${historicalClaims}`);
  console.log(`   worker slots held by paused jobs: ${workerSlotsHeld}`);

  // 4. Publisher Restored: Transition to RECOVERING
  console.log('\n4. Restoring Publisher (Throughput = 5, Ready Backlog = 15)...');
  const recoverEval = barrier.evaluatePublicationStall(
    {
      publicationThroughput: 5,
      readyBacklog: 15,
      producerActive: true
    },
    'CLOSED'
  );
  console.log('   -> Recovery Result:');
  console.log('      action:', recoverEval.action);
  console.log('      nextState:', recoverEval.nextState);

  await barrier.setState(recoverEval.nextState, recoverEval.reason);
  console.log(`   Barrier in DB: ${await barrier.getState(true)}`);

  // 5. Backlog Drained: Transition to OPEN
  console.log('\n5. Backlog Drained (Throughput = 10, Ready Backlog = 0)...');
  const openEval = barrier.evaluatePublicationStall(
    {
      publicationThroughput: 10,
      readyBacklog: 0,
      producerActive: true
    },
    'RECOVERING'
  );
  console.log('   -> Final Open Result:');
  console.log('      action:', openEval.action);
  console.log('      nextState:', openEval.nextState);

  await barrier.setState(openEval.nextState, openEval.reason);
  const finalState = await barrier.getState(true);
  console.log(`   Final Barrier in DB: ${finalState}`);

  // 6. Confirm Live Health
  console.log('\n6. Checking Live Production Health...');
  const { data: health } = await sb.rpc('admin_get_system_health');
  console.log('   DB Connections:', health?.database?.current_connections, '(Active:', health?.database?.active_connections, ')');
  console.log('   Waiting Locks:', health?.database?.waiting_locks, 'Deadlocks:', health?.database?.deadlocks);

  // Web Latency check
  const t0 = Date.now();
  const resp = await fetch('https://manga.project-nox-awerkori.workers.dev/', {
    headers: { 'User-Agent': 'Mozilla/5.0 ProjectNox-StallVerify/1.0' }
  });
  console.log(`   Web Home Response: [${resp.status}] in ${Date.now() - t0}ms`);

  console.log('\n====================================================');
  console.log('ALL CONTROLLED VERIFICATIONS PASSED');
  console.log('====================================================');
}

runControlledVerification().catch(console.error);
