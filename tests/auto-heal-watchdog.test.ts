import { describe, it, expect, vi, beforeEach } from 'vitest';

process.env.YUGABYTE_PASSWORD = process.env.YUGABYTE_PASSWORD || 'mock-ci-password';

import {
  AutoHealWatchdog,
  type HealthPanelMetrics,
  type AutoRestartRecord,
} from '../src/core/auto-heal-watchdog.js';
import { ImporterEngine } from '../src/core/engine.js';
import { diagnostics } from '../src/core/diagnostics.js';

describe('AutoHealWatchdog — Autonomous Recovery & Liveness Hardening (Casos A a N)', () => {
  let mockPool: any;
  let mockScheduler: any;
  let mockAdmissionController: any;
  let mockProtectiveSentinel: any;
  let onControlledRestart: any;

  beforeEach(() => {
    mockPool = {
      query: vi.fn(),
    };
    mockScheduler = {
      syncInFlightCountsFromDb: vi.fn().mockResolvedValue(undefined),
      reloadActiveWorks: vi.fn().mockResolvedValue(undefined),
    };
    mockAdmissionController = {
      runAdmissionCycle: vi.fn().mockResolvedValue(undefined),
    };
    mockProtectiveSentinel = {
      evaluateAutoResume: vi.fn().mockResolvedValue(undefined),
    };
    onControlledRestart = vi.fn().mockResolvedValue(undefined);
  });

  // =========================================================================
  // CASO A: Eligible backlog + active works stale + zero claim => watchdog recupera
  // =========================================================================
  it('Caso A: eligible backlog + active works stale + zero claim => watchdog recupera sem restart prematuro', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // 22 minutes without progress (between 20m and 30m)
    const progressAgeSec = 22 * 60;

    mockPool.query.mockImplementation((sql: string, params?: any[]) => {
      // 1. Timestamps query
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '1320',
              completed_age: String(progressAgeSec),
              fresh_age: String(progressAgeSec),
              started_15m: '0',
              completed_15m: '0',
              fresh_15m: '0',
            },
          ],
        };
      }
      // 2. Queue counts query
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '50', importing_cnt: '0', retry_cnt: '2' }] };
      }
      // 3. Staged unique
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '10' }] };
      }
      // 4. Scheduler state active works (stale: 0 queued, 0 in flight)
      if (sql.includes("key = 'active_works'")) {
        const staleWorks = [
          { workId: 'w-stale-1', workTitle: 'Stale Work 1', queuedChapters: 0, inFlightChapters: 0 },
        ];
        return { rows: [{ value: JSON.stringify(staleWorks) }] };
      }
      // Check claimable jobs for work
      if (sql.includes("payload->>'workId'")) {
        return { rows: [{ count: '0' }] }; // 0 claimable in DB
      }
      // 5. Protective stop settings
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      // 6. Auto-restarts settings
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      // Update importer_queue or settings
      return { rows: [] };
    });

    const metrics = await watchdog.evaluateCycle();

    // Verification:
    // 1. Status is identified as STALLED (not HEALTHY)
    expect(metrics.status).toBe('STALLED');
    // 2. Level 1 or Level 2 recovery was triggered
    expect(metrics.autoHealState).toMatch(/LEVEL_1_LIGHT_RECONCILIATION|LEVEL_2_STUCK_STATE_AUDIT/);
    // 3. Admission cycle was called to bring in fresh work
    expect(mockAdmissionController.runAdmissionCycle).toHaveBeenCalled();
    // 4. Controlled restart was NOT called because stall is <30m
    expect(onControlledRestart).not.toHaveBeenCalled();
  });

  // =========================================================================
  // CASO B: Expired importing lease + worker morto => job recuperado com segurança
  // =========================================================================
  it('Caso B: expired importing lease + worker morto => job recuperado para QUEUED preservando attempts', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    const updateCalls: { sql: string; params?: any[] }[] = [];
    mockPool.query.mockImplementation((sql: string, params?: any[]) => {
      updateCalls.push({ sql, params });
      if (sql.includes('UPDATE importer_queue')) {
        return {
          rows: [
            { id: 'job-stuck-1', task_type: 'IMPORT_CHAPTER', source: 'kuro', chapter_sort_key: 5 },
          ],
        };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const mockMetrics: HealthPanelMetrics = {
      status: 'STALLED',
      autoHealState: 'LEVEL_2_STUCK_STATE_AUDIT',
      lastStartedAgeSec: 1500,
      lastCompletedAgeSec: 1500,
      lastFreshVisibleAgeSec: 1500,
      startedLast15m: 0,
      completedLast15m: 0,
      freshLast15m: 0,
      eligibleJobs: 20,
      claimableWorks: 0,
      activeWorksCount: 0,
      zombieWorksCount: 0,
      importingCount: 1,
      retryCount: 0,
      stagedUnique: 0,
      lastAutoHealAt: null,
      autoRestartCount1h: 0,
      circuitBreakerOpen: false,
      protectiveStopActive: false,
      rssMb: 120,
      pid: 1234,
      timestamp: new Date().toISOString(),
    };

    await watchdog.runLevel2StuckStateAudit(mockMetrics);

    // Verify:
    // 1. UPDATE query targeted IMPORTING jobs with expired leases
    const reclaimQuery = updateCalls.find((c) => c.sql.includes('UPDATE importer_queue'));
    expect(reclaimQuery).toBeDefined();
    expect(reclaimQuery!.sql).toContain("status = 'QUEUED'");
    expect(reclaimQuery!.sql).toContain('locked_by = NULL');
    expect(reclaimQuery!.sql).toContain('lease_expires_at = NULL');
    expect(reclaimQuery!.sql).toContain("status = 'IMPORTING'");
    // Verify attempt counter is NOT incremented or wiped
    expect(reclaimQuery!.sql).not.toContain('attempts = attempts + 1');
    expect(reclaimQuery!.sql).not.toContain('attempts = 0');
  });

  // =========================================================================
  // CASO C: Zero eligible jobs => IDLE, sem restart
  // =========================================================================
  it('Caso C: zero eligible jobs => IDLE, sem falso stall e sem restart', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // 5 hours without progress (18000s) but queue is completely empty
    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '18000',
              completed_age: '18000',
              fresh_age: '18000',
              started_15m: '0',
              completed_15m: '0',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '0' }] };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.evaluateCycle();

    expect(metrics.status).toBe('IDLE');
    expect(metrics.autoHealState).toBe('IDLE');
    expect(onControlledRestart).not.toHaveBeenCalled();
  });

  // =========================================================================
  // CASO D: Protective stop legítimo => PAUSED_BY_PROTECTION, sem auto-heal agressivo
  // =========================================================================
  it('Caso D: protective stop manual recente => PAUSED_BY_PROTECTION, sem restart agressivo', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // 40 minutes without progress, but staff manual stop is active
    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '2400',
              completed_age: '2400',
              fresh_age: '2400',
              started_15m: '0',
              completed_15m: '0',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '100', importing_cnt: '0', retry_cnt: '5' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '0' }] };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return {
          rows: [
            {
              value: JSON.stringify({
                active: true,
                reason: 'Manual staff pause for infrastructure maintenance',
                classification: 'MANUAL_STOP',
                triggered_at: new Date().toISOString(),
              }),
            },
          ],
        };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.evaluateCycle();

    expect(metrics.status).toBe('PAUSED_BY_PROTECTION');
    expect(onControlledRestart).not.toHaveBeenCalled();
  });

  // =========================================================================
  // CASO E: Source outage geral => circuit breaker tripwire, não entra em restart loop
  // =========================================================================
  it('Caso E: source outage geral com >=3 restarts em 1h => AUTO-RECOVERY CIRCUIT OPEN, sem restart infinito', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    const now = Date.now();
    // 3 recent restarts in the last 40 minutes
    const pastRestarts: AutoRestartRecord[] = [
      { timestamp: new Date(now - 35 * 60 * 1000).toISOString(), reason: 'CRITICAL_STALL', progressAgeSec: 1800, eligibleJobs: 100 },
      { timestamp: new Date(now - 20 * 60 * 1000).toISOString(), reason: 'CRITICAL_STALL', progressAgeSec: 1800, eligibleJobs: 100 },
      { timestamp: new Date(now - 5 * 60 * 1000).toISOString(), reason: 'CRITICAL_STALL', progressAgeSec: 1800, eligibleJobs: 100 },
    ];

    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '2000',
              completed_age: '2000',
              fresh_age: '2000',
              started_15m: '0',
              completed_15m: '0',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '100', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '0' }] };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify(pastRestarts) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.evaluateCycle();

    expect(metrics.status).toBe('CRITICAL_STALL');
    expect(metrics.circuitBreakerOpen).toBe(true);
    expect(metrics.autoHealState).toBe('CIRCUIT_OPEN');
    // Controlled restart MUST be blocked by circuit breaker
    expect(onControlledRestart).not.toHaveBeenCalled();
  });

  // =========================================================================
  // CASO F: Process alive + 4h simulated no progress => detecta muito antes de 4h
  // =========================================================================
  it('Caso F: processo vivo mas 4h sem progresso => watchdog detecta em <=15m e escala para CRITICAL_STALL em <=30m', () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // Subcase 1: at 5 minutes
    const status5m = watchdog.determineHealthStatus({
      eligibleJobs: 500,
      importingCount: 2,
      lastCompletedAgeSec: 300,
      lastFreshVisibleAgeSec: 300,
      protectiveStopActive: false,
    });
    expect(status5m).toBe('HEALTHY');

    // Subcase 2: at 12 minutes (> 10m)
    const status12m = watchdog.determineHealthStatus({
      eligibleJobs: 500,
      importingCount: 2,
      lastCompletedAgeSec: 720,
      lastFreshVisibleAgeSec: 720,
      protectiveStopActive: false,
    });
    expect(status12m).toBe('DEGRADED');

    // Subcase 3: at 16 minutes (> 15m)
    const status16m = watchdog.determineHealthStatus({
      eligibleJobs: 500,
      importingCount: 2,
      lastCompletedAgeSec: 960,
      lastFreshVisibleAgeSec: 960,
      protectiveStopActive: false,
    });
    expect(status16m).toBe('STALLED');

    // Subcase 4: at 32 minutes (>= 30m)
    const status32m = watchdog.determineHealthStatus({
      eligibleJobs: 500,
      importingCount: 2,
      lastCompletedAgeSec: 1920,
      lastFreshVisibleAgeSec: 1920,
      protectiveStopActive: false,
    });
    expect(status32m).toBe('CRITICAL_STALL');

    // Subcase 5: at 4 hours (14,400s) - NEVER healthy, firmly CRITICAL_STALL
    const status4h = watchdog.determineHealthStatus({
      eligibleJobs: 500,
      importingCount: 0,
      lastCompletedAgeSec: 14400,
      lastFreshVisibleAgeSec: 14400,
      protectiveStopActive: false,
    });
    expect(status4h).toBe('CRITICAL_STALL');
  });

  // =========================================================================
  // CASO G: Persistent cross-process 15m cooldown (DB has restart 3m ago -> restart blocked)
  // =========================================================================
  it('Caso G: persistent cross-process 15m cooldown => restart bloqueado mesmo em novo processo', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    const now = Date.now();
    // Simulate restart recorded in DB 3 minutes ago by previous process
    const pastRestarts: AutoRestartRecord[] = [
      {
        timestamp: new Date(now - 3 * 60 * 1000).toISOString(),
        reason: 'CRITICAL_STALL',
        progressAgeSec: 2100,
        eligibleJobs: 100,
      },
    ];

    // Stall condition: 35 minutes without progress, 100 eligible jobs
    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '2100',
              completed_age: '2100',
              fresh_age: '2100',
              started_15m: '0',
              completed_15m: '0',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '100', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '0' }] };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify(pastRestarts) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.evaluateCycle();

    // Verify:
    // 1. Status is CRITICAL_STALL
    expect(metrics.status).toBe('CRITICAL_STALL');
    // 2. Controlled restart was BLOCKED by persistent 15m cooldown
    expect(onControlledRestart).not.toHaveBeenCalled();
    // 3. Auto-heal state did NOT transition to LEVEL_3_RESTART_PENDING
    expect(metrics.autoHealState).not.toBe('LEVEL_3_RESTART_PENDING');
  });

  // =========================================================================
  // CASO H: Processing saudável mas publicação travada (>30m) com novos capítulos
  // =========================================================================
  it('Caso H: processing saudável mas publicação travada (>30m) com novos capítulos => detecta PUBLICATION_STALL / CRITICAL_STALL', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // Completed 120s ago (2m ago - healthy processing)
    // Fresh visible 2100s ago (35m ago - stalled publication)
    // Recent completions are NOT dedupe only (actual new chapters completed)
    // Staged backlog exists (5 staged chapters stuck)
    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '120',
              completed_age: '120',
              fresh_age: '2100',
              started_15m: '10',
              completed_15m: '10',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('recent_total')) {
        return { rows: [{ recent_total: '10', recent_dedupe: '0' }] };
      }
      if (sql.includes('recent_providers')) {
        return { rows: [{ recent_providers: '10' }] };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '50', importing_cnt: '2', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '5' }] };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      return { rows: [] };
    });

    // Simulate that Level 1 and Level 2 reconciliations were already attempted during previous cycles of the 35m stall
    const now = Date.now();
    (watchdog as any).lastLevel1At = now;
    (watchdog as any).lastLevel2At = now;

    const metrics = await watchdog.evaluateCycle();

    // Verify multidimensional health detection:
    expect(metrics.processingHealth).toBe('HEALTHY');
    expect(metrics.publicationHealth).toBe('CRITICAL_STALL');
    expect(metrics.status).toBe('CRITICAL_STALL');
    // Publication stall was NOT masked by active chapter completions!
    expect(onControlledRestart).toHaveBeenCalled();
  });

  // =========================================================================
  // CASO I: Dedupe false-positive protection (ALREADY_CANONICAL => NO_FRESH_EXPECTED)
  // =========================================================================
  it('Caso I: processamento ativo com conclusões ALREADY_CANONICAL / dedupe => NO_FRESH_EXPECTED, status HEALTHY sem falso alarme', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // Completed 120s ago (2m ago - healthy processing)
    // Fresh visible 2100s ago (35m ago)
    // BUT all recent completions were CANONICAL_ALREADY_SATISFIED / dedupe
    // Staged backlog is 0
    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '120',
              completed_age: '120',
              fresh_age: '2100',
              started_15m: '10',
              completed_15m: '10',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('recent_completed_jobs') || sql.includes('recent_total')) {
        return {
          rows: [
            { classification: 'ALREADY_CANONICAL', cnt: '10' },
            { recent_total: '10', recent_dedupe: '10' },
          ],
        };
      }
      if (sql.includes('recent_providers')) {
        return { rows: [{ recent_providers: '0' }] };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '50', importing_cnt: '2', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '0' }] };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.evaluateCycle();

    // Verify dedupe false-positive protection:
    expect(metrics.processingHealth).toBe('HEALTHY');
    expect(metrics.publicationHealth).toBe('NO_FRESH_EXPECTED');
    expect(metrics.status).toBe('HEALTHY');
    expect(metrics.autoHealState).toMatch(/MONITORING|RECOVERED/);
    expect(onControlledRestart).not.toHaveBeenCalled();
  });

  // =========================================================================
  // CASO J: Truly graceful bounded restart (drain, pool close, timeout <=10s, exit)
  // =========================================================================
  it('Caso J: graceful bounded restart => drain de in-flight, pool fechado, timeout <=10s, exit(1)', async () => {
    const mockSupabase: any = {
      from: vi.fn(),
      rpc: vi.fn(),
      query: vi.fn(),
      getPool: vi.fn().mockReturnValue(mockPool),
    };
    const mockStorage: any = {
      uploadFile: vi.fn(),
      getPublicUrl: vi.fn(),
    };
    const mockRegistry: any = {};
    const mockRateLimiter: any = {};
    const mockConfig: any = {
      WORKER_ID: 'test-worker',
      MAX_CONCURRENT_CHAPTERS: 8,
      TESTED_CONCURRENCY_CEILING: 18,
    };
    const mockQueue: any = {};
    const mockDedupe: any = {};
    const mockCheckpoint: any = {};
    const mockReconciler: any = {};
    const mockPublicationBarrier: any = {};
    const mockSafetyBarrier: any = {};
    const mockSentinel: any = {
      evaluateAutoResume: vi.fn(),
    };

    const engine = new ImporterEngine(
      mockSupabase,
      mockStorage,
      mockRegistry,
      mockRateLimiter,
      mockConfig,
      mockQueue,
      mockDedupe,
      mockCheckpoint,
      mockReconciler,
      mockPublicationBarrier,
      mockSafetyBarrier,
      mockSentinel
    );

    let exitCode: number | null = null;
    engine.setExitHandlerForTest((code) => {
      exitCode = code;
    });

    // Simulate in-flight active jobs in diagnostics that drain quickly
    diagnostics.registerJob({
      jobId: 'drain-test-job-1',
      taskType: 'IMPORT_CHAPTER',
      source: 'test-src',
    });
    expect(diagnostics.getActiveJobsCount()).toBeGreaterThan(0);

    // Simulate draining the job after 300ms
    setTimeout(() => {
      diagnostics.unregisterJob('drain-test-job-1');
    }, 300);

    const t0 = Date.now();
    await engine.initiateControlledSelfRestart('Test graceful shutdown');
    const elapsedMs = Date.now() - t0;

    // Verify graceful bounded sequence:
    // 1. In-flight jobs drained to 0
    expect(diagnostics.getActiveJobsCount()).toBe(0);
    // 2. Loops stopped
    expect((engine as any).stopSignal).toBe(true);
    expect((engine as any).abortController.signal.aborted).toBe(true);
    // 3. Exit code 1
    expect(exitCode).toBe(1);
    // 4. Hard safety limit: total shutdown elapsed time MUST be <= 10000ms
    expect(elapsedMs).toBeLessThanOrEqual(10000);
  });

  // =========================================================================
  // CASO K: Zero eligible, zero importing, zero staged => status === 'IDLE'
  // =========================================================================
  it('Caso K: zero eligible, zero importing, zero staged => status IDLE, zero falso alarme e zero restart', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // 5 hours without progress (18000s) but queue and staged are completely empty
    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '18000',
              completed_age: '18000',
              fresh_age: '18000',
              started_15m: '0',
              completed_15m: '0',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '0' }] };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.evaluateCycle();

    expect(metrics.status).toBe('IDLE');
    expect(metrics.publicationHealth).toBe('NO_FRESH_EXPECTED');
    expect(metrics.autoHealState).toBe('IDLE');
    expect(metrics.publishableStaged).toBe(0);
    expect(onControlledRestart).not.toHaveBeenCalled();

    // Verify evaluateMultidimensionalHealth directly
    const evalResult = watchdog.evaluateMultidimensionalHealth({
      eligibleJobs: 0,
      importingCount: 0,
      publishableStaged: 0,
      lastCompletedAgeSec: 18000,
      lastFreshVisibleAgeSec: 18000,
      protectiveStopActive: false,
    });
    expect(evalResult.status).toBe('IDLE');
    expect(evalResult.publicationHealth).toBe('NO_FRESH_EXPECTED');
  });

  // =========================================================================
  // CASO L: Eligible=0, importing=0, staged publishable=5, freshAge=2100s (>30m) => CRITICAL_STALL (NÃO IDLE)
  // =========================================================================
  it('Caso L: eligible=0, importing=0, staged publishable=5, freshAge > 30m => status CRITICAL_STALL (NÃO IDLE) e sweep acionado', async () => {
    const mockPublicationBarrier: any = {
      sweepStagedPublications: vi.fn().mockResolvedValue(3),
    };

    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      publicationBarrier: mockPublicationBarrier,
      onControlledRestart,
    });

    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '120',
              completed_age: '120',
              fresh_age: '2100', // 35m > 30m
              started_15m: '0',
              completed_15m: '0',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '5' }] };
      }
      if (sql.includes('publishable_staged') || sql.includes('staged_works')) {
        return { rows: [{ publishable_staged: '5' }] };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.evaluateCycle();

    // MUST NOT be IDLE because publishable staged chapters exist!
    expect(metrics.status).not.toBe('IDLE');
    expect(metrics.status).toBe('CRITICAL_STALL');
    expect(metrics.publicationHealth).toBe('CRITICAL_STALL');
    expect(metrics.publishableStaged).toBe(5);

    // Verify Level 1 recovery triggered sweepStagedPublications safely
    expect(mockPublicationBarrier.sweepStagedPublications).toHaveBeenCalledWith(40, 6);
  });

  // =========================================================================
  // CASO M: Eligible=0, importing=0, publishable=0, waitingPredecessor=5 => publicationHealth='NO_FRESH_EXPECTED', status='IDLE'
  // =========================================================================
  it('Caso M: eligible=0, importing=0, publishable=0, waitingPredecessor=5 => publicationHealth NO_FRESH_EXPECTED, status IDLE, sem restart', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // Fresh age > 30m, but all staged chapters are blocked waiting for predecessors (publishable = 0)
    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '2400',
              completed_age: '2400',
              fresh_age: '2400', // 40m
              started_15m: '0',
              completed_15m: '0',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '5' }] };
      }
      if (sql.includes('publishable_staged') || sql.includes('staged_works')) {
        return { rows: [{ publishable_staged: '0' }] }; // 0 publishable!
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.evaluateCycle();

    // Since publishable = 0, no publication stall can be attributed to watchdog:
    expect(metrics.publishableStaged).toBe(0);
    expect(metrics.publicationHealth).toBe('NO_FRESH_EXPECTED');
    expect(metrics.status).toBe('IDLE');
    expect(metrics.autoHealState).toBe('IDLE');
    expect(onControlledRestart).not.toHaveBeenCalled();
  });

  // =========================================================================
  // CASO N: Correlated dedupe classification with mixed jobs (Work 1 dedupe + Work 2 fresh expected)
  // =========================================================================
  it('Caso N: correlated dedupe com jobs mistos (dedupe + fresh_expected) => não silencia stall legítimo, publicationHealth STALLED', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // Processing active (completed 120s ago), but fresh age is 1200s (20m, degraded/stalled)
    // Mixed completions: 30 jobs dedupe/canonical, but 20 jobs FRESH_EXPECTED (not published yet)
    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '120',
              completed_age: '120',
              fresh_age: '1200', // 20m
              started_15m: '10',
              completed_15m: '10',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '50', importing_cnt: '2', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '0' }] };
      }
      if (sql.includes('recent_completed_jobs') || sql.includes('correlated')) {
        return {
          rows: [
            { classification: 'ALREADY_CANONICAL', cnt: '20' },
            { classification: 'DEDUPE_SOURCE', cnt: '10' },
            { classification: 'FRESH_EXPECTED', cnt: '20' },
          ],
        };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.evaluateCycle();

    // Correlated breakdown must identify freshExpected > 0
    expect(metrics.recentCorrelatedBreakdown).toBeDefined();
    expect(metrics.recentCorrelatedBreakdown!.alreadyCanonical).toBe(20);
    expect(metrics.recentCorrelatedBreakdown!.dedupeSource).toBe(10);
    expect(metrics.recentCorrelatedBreakdown!.freshExpected).toBe(20);

    // Because freshExpected > 0, dedupe-only protection MUST NOT apply!
    expect(metrics.publicationHealth).toBe('STALLED');
    expect(metrics.status).toBe('STALLED');
    expect(metrics.autoHealState).toMatch(/LEVEL_1_LIGHT_RECONCILIATION|LEVEL_2_STUCK_STATE_AUDIT/);
  });

  // =========================================================================
  // CASO O: eligible=0, importing=0, publishable=0, waiting=0, stuck=5 => NÃO IDLE
  // =========================================================================
  it('Caso O: eligible=0, importing=0, publishable=0, waiting=0, stuck=5 => status NÃO é IDLE', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    const res = watchdog.evaluateMultidimensionalHealth({
      eligibleJobs: 0,
      importingCount: 0,
      lastCompletedAgeSec: 1800,
      lastFreshVisibleAgeSec: 1800,
      protectiveStopActive: false,
      publishableStaged: 0,
      waitingPredecessorStaged: 0,
      stuckStaged: 5,
    });

    expect(res.status).not.toBe('IDLE');
    expect(['DEGRADED', 'STALLED', 'CRITICAL_STALL']).toContain(res.status);
  });

  // =========================================================================
  // CASO P: Work A staged aguardando predecessor inexistente; Work B possui 50 RETRY => Work A continua STUCK
  // =========================================================================
  it('Caso P: Work A staged sem predecessor ativo; Work B possui 50 RETRY => Work A continua STUCK, retries de B não mascaram A', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '60',
              completed_age: '60',
              fresh_age: '1200',
              started_15m: '10',
              completed_15m: '10',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '50', importing_cnt: '0', retry_cnt: '50' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '2' }] };
      }
      // Staged classification CTE
      if (sql.includes('staged_works')) {
        // Work A has staged chapter 2, no active queue, predecessor not satisfied
        return {
          rows: [
            {
              work_id: 'work-a',
              frontier_sort_key: '2.0000',
              total_staged_chapters: '2',
              max_published: null,
              has_predecessor_in_mapping: true,
              has_predecessor_in_queue: false,
              has_active_queue: false, // Work A has ZERO active queue jobs!
              is_frontier_publishable: 0,
            },
          ],
        };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.collectTelemetry(true);

    expect(metrics.publishableStaged).toBe(0);
    expect(metrics.stuckStaged).toBe(2);
    expect(metrics.waitingPredecessorStaged).toBe(0);
  });

  // =========================================================================
  // CASO Q: Obra sem capítulos publicados, staged = 1, 2, 3, 4 => somente frontier (1) é ACTIONABLE
  // =========================================================================
  it('Caso Q: obra nova sem publicados com staged 1,2,3,4 => apenas frontier inicial é ACTIONABLE (1 publishable, 3 waiting)', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '60',
              completed_age: '60',
              fresh_age: '60',
              started_15m: '10',
              completed_15m: '10',
              fresh_15m: '10',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '4' }] };
      }
      // Staged classification CTE
      if (sql.includes('staged_works')) {
        // Work has 4 staged chapters, frontier is 1, max_published is null, no predecessor in mapping/queue
        return {
          rows: [
            {
              work_id: 'work-new',
              frontier_sort_key: '1.0000',
              total_staged_chapters: '4',
              max_published: null,
              has_predecessor_in_mapping: false,
              has_predecessor_in_queue: false,
              has_active_queue: false,
              is_frontier_publishable: 1, // Only frontier is publishable!
            },
          ],
        };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.collectTelemetry(true);

    expect(metrics.publishableStaged).toBe(1); // ONLY 1! Not 4!
    expect(metrics.waitingPredecessorStaged).toBe(3); // 2, 3, 4 waiting for frontier
    expect(metrics.stuckStaged).toBe(0);
  });

  // =========================================================================
  // CASO R: heurística max+1 permitiria candidato, mas PublicationSafetyBarrier real bloqueia => PUBLISHABLE = 0, sem falso restart
  // =========================================================================
  it('Caso R: barreira real bloqueia (state=CLOSED) => PUBLISHABLE = 0, sem falso restart', async () => {
    const mockSafetyBarrier = {
      getState: vi.fn().mockResolvedValue('CLOSED'),
    };

    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      safetyBarrier: mockSafetyBarrier as any,
      onControlledRestart,
    });

    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '120',
              completed_age: '120',
              fresh_age: '2000',
              started_15m: '5',
              completed_15m: '5',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '1' }] };
      }
      if (sql.includes('staged_works')) {
        // Query heuristic would consider frontier publishable
        return {
          rows: [
            {
              work_id: 'work-blocked',
              frontier_sort_key: '5.0000',
              total_staged_chapters: '1',
              max_published: '4.0000',
              has_predecessor_in_mapping: false,
              has_predecessor_in_queue: false,
              has_active_queue: false,
              is_frontier_publishable: 1,
            },
          ],
        };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.collectTelemetry(true);

    // Because safetyBarrier state is CLOSED, publishableStaged is forced to 0!
    expect(metrics.publishableStaged).toBe(0);
    expect(metrics.waitingPredecessorStaged).toBe(1);

    // Evaluate cycle should NOT initiate restart because publishable = 0, eligible = 0, stuck = 0
    await watchdog.evaluateCycle();
    expect(onControlledRestart).not.toHaveBeenCalled();
  });

  // =========================================================================
  // CASO S: waiting predecessor verdadeiro na mesma obra com job RETRY válido => WAITING_PREDECESSOR, não STUCK
  // =========================================================================
  it('Caso S: waiting predecessor verdadeiro na mesma obra com job RETRY ativo => WAITING_PREDECESSOR, não STUCK', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '60',
              completed_age: '60',
              fresh_age: '60',
              started_15m: '5',
              completed_15m: '5',
              fresh_15m: '5',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '1', importing_cnt: '0', retry_cnt: '1' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '1' }] };
      }
      if (sql.includes('staged_works')) {
        // Work has staged chapter 5, but predecessor chapter 4 is active in queue with RETRY
        return {
          rows: [
            {
              work_id: 'work-s',
              frontier_sort_key: '5.0000',
              total_staged_chapters: '1',
              max_published: '3.0000',
              has_predecessor_in_mapping: true,
              has_predecessor_in_queue: true, // Chapter 4 in queue with RETRY!
              has_active_queue: true, // Work S has active queue jobs!
              is_frontier_publishable: 0,
            },
          ],
        };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.collectTelemetry(true);

    expect(metrics.publishableStaged).toBe(0);
    expect(metrics.waitingPredecessorStaged).toBe(1);
    expect(metrics.stuckStaged).toBe(0);
  });

  // =========================================================================
  // CASO T: Work A staged #5 com job ativo #8 (sem predecessor #4) => STUCK_STAGED, não WAITING_PREDECESSOR
  // =========================================================================
  it('Caso T: Work A staged #5 com job ativo posterior (#8) e sem predecessor ativo (#4) => STUCK_STAGED, não WAITING_PREDECESSOR', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '60',
              completed_age: '60',
              fresh_age: '60',
              started_15m: '5',
              completed_15m: '5',
              fresh_15m: '5',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '1', importing_cnt: '1', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '1' }] };
      }
      if (sql.includes('staged_works')) {
        // Work A has staged #5, active job #8, but NO predecessor in queue/mapping (#4)
        return {
          rows: [
            {
              work_id: 'work-a',
              frontier_sort_key: '5.0000',
              total_staged_chapters: '1',
              max_published: '3.0000',
              has_predecessor_in_mapping: false,
              has_predecessor_in_queue: false, // chapter 8 in queue is NOT a predecessor of 5!
              is_frontier_publishable: 0,
            },
          ],
        };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.collectTelemetry(true);

    expect(metrics.publishableStaged).toBe(0);
    expect(metrics.waitingPredecessorStaged).toBe(0); // MUST NOT be WAITING!
    expect(metrics.stuckStaged).toBe(1); // MUST be STUCK!
  });

  // =========================================================================
  // CASO U: 80 obras staged, LIMIT 40 inspecionadas => residual não é convertido em WAITING, UNCLASSIFIED explícito
  // =========================================================================
  it('Caso U: 80 obras staged, LIMIT 40 classificadas => capítulos residuais permanecem UNCLASSIFIED e não são absorvidos em WAITING', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '60',
              completed_age: '60',
              fresh_age: '60',
              started_15m: '5',
              completed_15m: '5',
              fresh_15m: '5',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '80' }] };
      }
      if (sql.includes('staged_works')) {
        // 40 works classified (40 chapters total)
        const rows = [];
        for (let i = 1; i <= 40; i++) {
          rows.push({
            work_id: `work-${i}`,
            frontier_sort_key: '2.0000',
            total_staged_chapters: '1',
            max_published: '1.0000',
            has_predecessor_in_mapping: false,
            has_predecessor_in_queue: true,
            is_frontier_publishable: 0,
          });
        }
        return { rows };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.collectTelemetry(true);

    expect(metrics.publishableStaged).toBe(0);
    expect(metrics.waitingPredecessorStaged).toBe(40); // Only the 40 verified!
    expect(metrics.classifiedStaged).toBe(40);
    expect(metrics.unclassifiedStaged).toBe(40); // 80 - 40 = 40 explicitly unclassified!
    expect(metrics.stagedUnique).toBe(80);

    // IDLE is NOT permitted with unclassifiedStaged > 0 even with eligible=0 and importing=0
    expect(metrics.status).not.toBe('IDLE');
  });

  // =========================================================================
  // CASO V: Stuck A existe há 29m e resolve; Stuck B aparece agora => idade de B começa em 0, sem CRITICAL_STALL
  // =========================================================================
  it('Caso V: Stuck A existe há 29m e resolve, Stuck B aparece agora => relógio de B começa do zero, sem falso CRITICAL_STALL', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // Simulate Work A had been stuck for 29 minutes
    const nowMs = Date.now();
    watchdog.setStuckIdentity('work-a:5.0000', nowMs - 29 * 60 * 1000);

    // Now Work A is resolved! Only Work B is returned as stuck
    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '60',
              completed_age: '60',
              fresh_age: '120',
              started_15m: '5',
              completed_15m: '5',
              fresh_15m: '5',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '1' }] };
      }
      if (sql.includes('staged_works')) {
        return {
          rows: [
            {
              work_id: 'work-b',
              frontier_sort_key: '10.0000',
              total_staged_chapters: '1',
              max_published: '8.0000',
              has_predecessor_in_mapping: false,
              has_predecessor_in_queue: false,
              is_frontier_publishable: 0,
            },
          ],
        };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.collectTelemetry(true);

    expect(metrics.stuckStaged).toBe(1);
    // Work A was removed, Work B's age is ~0 seconds
    const bAge = watchdog.getStuckIdentityAge('work-b:10.0000');
    expect(bAge).toBeLessThan(5);
    // Status must NOT be CRITICAL_STALL or STALLED; it should be DEGRADED
    expect(metrics.status).toBe('DEGRADED');
  });

  // =========================================================================
  // CASO W: SafetyBarrier global OPEN, mas chapter barrier individual bloqueia => NÃO classificado como ACTIONABLE
  // =========================================================================
  it('Caso W: SafetyBarrier global OPEN, mas chapter barrier individual bloqueia => capítulo NÃO é classificado como ACTIONABLE', async () => {
    const mockSafetyBarrier = {
      getState: vi.fn().mockResolvedValue('OPEN'),
    };
    const mockPublicationBarrier = {
      checkBarrier: vi.fn().mockResolvedValue({
        canPublish: false,
        reason: 'PREDECESSOR_UNPUBLISHED',
        blockingCount: 1,
        blockingSortKeys: [4.0],
      }),
    };

    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      safetyBarrier: mockSafetyBarrier as any,
      publicationBarrier: mockPublicationBarrier as any,
      onControlledRestart,
    });

    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '60',
              completed_age: '60',
              fresh_age: '60',
              started_15m: '5',
              completed_15m: '5',
              fresh_15m: '5',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '1' }] };
      }
      if (sql.includes('staged_works')) {
        // Query heuristic thinks frontier is publishable (e.g. 5 <= 4 + 1.05)
        return {
          rows: [
            {
              work_id: 'work-w',
              frontier_sort_key: '5.0000',
              total_staged_chapters: '1',
              max_published: '4.0000',
              has_predecessor_in_mapping: false,
              has_predecessor_in_queue: false,
              is_frontier_publishable: 1,
            },
          ],
        };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.collectTelemetry(true);

    // Canonical chapter barrier blocked it => NOT ACTIONABLE!
    expect(mockPublicationBarrier.checkBarrier).toHaveBeenCalledWith('work-w', 5);
    expect(metrics.publishableStaged).toBe(0);
    expect(metrics.stuckStaged).toBe(1);
  });

  // =========================================================================
  // CASO X: Keyset pagination determinística progride entre ciclos e faz wrap
  // =========================================================================
  it('Caso X: keyset pagination determinística progride entre ciclos (LIMIT 40) e faz wrap ao final', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // 120 works in total across 3 pages: 1-40, 41-80, 81-120
    const generateWorkRows = (start: number, count: number) => {
      const rows = [];
      for (let i = 0; i < count; i++) {
        const idx = start + i;
        rows.push({
          work_id: `work-${idx.toString().padStart(3, '0')}`,
          frontier_sort_key: `${idx}.0000`,
          total_staged_chapters: '1',
          max_published: `${idx - 1}.0000`,
          has_predecessor_in_mapping: false,
          has_predecessor_in_queue: false,
          is_frontier_publishable: 1,
        });
      }
      return rows;
    };

    mockPool.query.mockImplementation((sql: string, params?: any[]) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '60',
              completed_age: '60',
              fresh_age: '60',
              started_15m: '10',
              completed_15m: '10',
              fresh_15m: '10',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '120' }] };
      }
      if (sql.includes('staged_works')) {
        const cursorSortKey = params?.[0];
        const cursorWorkId = params?.[1];

        if (cursorSortKey === null || cursorSortKey === undefined) {
          // Page 1: works 1 to 40
          return { rows: generateWorkRows(1, 40) };
        } else if (cursorSortKey === 40) {
          // Page 2: works 41 to 80
          return { rows: generateWorkRows(41, 40) };
        } else if (cursorSortKey === 80) {
          // Page 3: works 81 to 120
          return { rows: generateWorkRows(81, 40) };
        } else {
          // End: 0 rows
          return { rows: [] };
        }
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    // Cycle 1: initial null cursor -> classifies 1-40 -> advances cursor to (40, 'work-040')
    expect(watchdog.getClassificationCursor()).toBeNull();
    const m1 = await watchdog.collectTelemetry(true);
    expect(m1.classifiedThisCycle).toBe(40);
    expect(m1.unclassifiedStaged).toBe(80);
    const c1 = watchdog.getClassificationCursor();
    expect(c1).toEqual({ lastFrontierSortKey: 40, lastWorkId: 'work-040' });

    // Cycle 2: passes cursor (40, 'work-040') -> classifies 41-80 -> advances cursor to (80, 'work-080')
    const m2 = await watchdog.collectTelemetry(true);
    expect(m2.classifiedThisCycle).toBe(40);
    const c2 = watchdog.getClassificationCursor();
    expect(c2).toEqual({ lastFrontierSortKey: 80, lastWorkId: 'work-080' });

    // Cycle 3: passes cursor (80, 'work-080') -> classifies 81-120 -> advances cursor to (120, 'work-120')
    const m3 = await watchdog.collectTelemetry(true);
    expect(m3.classifiedThisCycle).toBe(40);
    const c3 = watchdog.getClassificationCursor();
    expect(c3).toEqual({ lastFrontierSortKey: 120, lastWorkId: 'work-120' });

    // Cycle 4: reaches end (<40 rows) -> wraps cursor back to null
    const m4 = await watchdog.collectTelemetry(true);
    expect(m4.classifiedThisCycle).toBe(0);
    expect(watchdog.getClassificationCursor()).toBeNull();
  });

  // =========================================================================
  // CASO Y: unclassifiedStaged isolado NÃO pode causar restart (DEGRADED / CLASSIFICATION_PENDING)
  // =========================================================================
  it('Caso Y: unclassifiedStaged isolado com publishable=0 e stuck=0 NÃO causa restart (status DEGRADED, publicationHealth DEGRADED)', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    // Simulating 500 staged unique, but 0 publishable, 0 stuck, 0 eligible, 0 importing
    // and lastFreshVisibleAge > 30 min (e.g. 7200s = 2h)
    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '7200',
              completed_age: '7200',
              fresh_age: '7200',
              started_15m: '0',
              completed_15m: '0',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '500' }] };
      }
      if (sql.includes('staged_works')) {
        // Page 1 returns 40 works all waiting predecessor (has_predecessor_in_queue = true)
        // publishable = 0, stuck = 0, waiting = 40, unclassified = 460
        const rows = [];
        for (let i = 1; i <= 40; i++) {
          rows.push({
            work_id: `work-wait-${i}`,
            frontier_sort_key: `${i}.0000`,
            total_staged_chapters: '1',
            max_published: `${i - 2}.0000`,
            has_predecessor_in_mapping: true,
            has_predecessor_in_queue: true,
            is_frontier_publishable: 0,
          });
        }
        return { rows };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    const metrics = await watchdog.evaluateCycle();

    expect(metrics.publishableStaged).toBe(0);
    expect(metrics.stuckStaged).toBe(0);
    expect(metrics.unclassifiedStaged).toBe(460);
    expect(metrics.status).toBe('DEGRADED');
    expect(metrics.publicationHealth).toBe('DEGRADED');
    expect(onControlledRestart).not.toHaveBeenCalled();
  });

  // =========================================================================
  // CASO Z: stuckAge sobrevive à paginação (off-page persistence)
  // =========================================================================
  it('Caso Z: stuckAge sobrevive à paginação — identidade de obra presa continua com relógio acumulado mesmo fora da página atual', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    let currentCycle = 1;

    mockPool.query.mockImplementation((sql: string, params?: any[]) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '60',
              completed_age: '60',
              fresh_age: '60',
              started_15m: '5',
              completed_15m: '5',
              fresh_15m: '5',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '80' }] };
      }
      if (sql.includes('importer_chapter_mappings') && sql.includes('ANY')) {
        // DB confirms work-stuck-a is still staged in DB!
        return { rows: [{ key: 'work-stuck-a:10.0000' }] };
      }
      if (sql.includes('staged_works')) {
        if (currentCycle === 1) {
          // Cycle 1: work-stuck-a is on page 1 and is stuck
          return {
            rows: [
              {
                work_id: 'work-stuck-a',
                frontier_sort_key: '10.0000',
                total_staged_chapters: '1',
                max_published: '8.0000',
                has_predecessor_in_mapping: false,
                has_predecessor_in_queue: false,
                is_frontier_publishable: 0,
              },
            ],
          };
        } else {
          // Cycle 2: page 2 has other works, work-stuck-a is NOT on page 2
          return {
            rows: [
              {
                work_id: 'work-page-2',
                frontier_sort_key: '50.0000',
                total_staged_chapters: '1',
                max_published: '49.0000',
                has_predecessor_in_mapping: false,
                has_predecessor_in_queue: false,
                is_frontier_publishable: 1,
              },
            ],
          };
        }
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    // Cycle 1: work-stuck-a detected
    currentCycle = 1;
    await watchdog.collectTelemetry(true);
    expect(watchdog.getTrackedStuckKeys()).toContain('work-stuck-a:10.0000');

    // Simulate 25 minutes elapsed
    const originalDetectedAt = Date.now() - 25 * 60 * 1000;
    (watchdog as any).stuckIdentities.set('work-stuck-a:10.0000', originalDetectedAt);

    // Cycle 2: cursor advanced, work-stuck-a is NOT in the 40 works of this cycle
    currentCycle = 2;
    const m2 = await watchdog.collectTelemetry(true);

    // work-stuck-a survived because it's still staged in DB!
    expect(watchdog.getTrackedStuckKeys()).toContain('work-stuck-a:10.0000');
    const age = watchdog.getStuckIdentityAge('work-stuck-a:10.0000');
    expect(age).toBeGreaterThanOrEqual(25 * 60);
    expect(m2.stuckStagedAgeSec).toBeGreaterThanOrEqual(25 * 60);
  });

  // =========================================================================
  // CASO AA: Eviction de stuck acontece apenas com comprovação
  // =========================================================================
  it('Caso AA: Eviction de stuck acontece apenas com comprovação (quando Mapping deixa de ser STAGED no DB)', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    let workAIsStagedInDb = true;

    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '60',
              completed_age: '60',
              fresh_age: '60',
              started_15m: '5',
              completed_15m: '5',
              fresh_15m: '5',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: workAIsStagedInDb ? '1' : '0' }] };
      }
      if (sql.includes('importer_chapter_mappings') && sql.includes('ANY')) {
        // If still staged, returns key. If published/resolved, returns empty!
        return { rows: workAIsStagedInDb ? [{ key: 'work-resolved-a:10.0000' }] : [] };
      }
      if (sql.includes('staged_works')) {
        if (workAIsStagedInDb) {
          return {
            rows: [
              {
                work_id: 'work-resolved-a',
                frontier_sort_key: '10.0000',
                total_staged_chapters: '1',
                max_published: '8.0000',
                has_predecessor_in_mapping: false,
                has_predecessor_in_queue: false,
                is_frontier_publishable: 0,
              },
            ],
          };
        } else {
          return { rows: [] };
        }
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      return { rows: [] };
    });

    // Step 1: Work is stuck
    await watchdog.collectTelemetry(true);
    expect(watchdog.getTrackedStuckKeys()).toContain('work-resolved-a:10.0000');

    // Step 2: Work gets published (leaves STAGED status in DB)
    workAIsStagedInDb = false;

    // Step 3: Next telemetry cycle audits tracked stuck keys against DB -> finds work is no longer staged -> evicts it!
    await watchdog.collectTelemetry(true);
    expect(watchdog.getTrackedStuckKeys()).not.toContain('work-resolved-a:10.0000');
    expect(watchdog.getStuckIdentityAge('work-resolved-a:10.0000')).toBe(0);
  });

  // =========================================================================
  // CASO BB: Circuit breaker tripwire transitions autotuner to SURVIVAL mode (concurrency = 1)
  // =========================================================================
  it('Caso BB: circuit breaker tripwire transitions autotuner to SURVIVAL mode (concurrency = 1)', async () => {
    const mockAutotuner: any = {
      setCapacity: vi.fn(),
    };

    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      autotuner: mockAutotuner,
      onControlledRestart,
    });

    const now = Date.now();
    const pastRestarts: AutoRestartRecord[] = [
      { timestamp: new Date(now - 35 * 60 * 1000).toISOString(), reason: 'CRITICAL_STALL', progressAgeSec: 1800, eligibleJobs: 100 },
      { timestamp: new Date(now - 20 * 60 * 1000).toISOString(), reason: 'CRITICAL_STALL', progressAgeSec: 1800, eligibleJobs: 100 },
      { timestamp: new Date(now - 5 * 60 * 1000).toISOString(), reason: 'CRITICAL_STALL', progressAgeSec: 1800, eligibleJobs: 100 },
    ];

    mockPool.query.mockImplementation((sql: string) => {
      if (sql.includes('started_age')) {
        return {
          rows: [
            {
              started_age: '2000',
              completed_age: '2000',
              fresh_age: '2000',
              started_15m: '0',
              completed_15m: '0',
              fresh_15m: '0',
            },
          ],
        };
      }
      if (sql.includes('eligible_cnt')) {
        return { rows: [{ eligible_cnt: '100', importing_cnt: '0', retry_cnt: '0' }] };
      }
      if (sql.includes('staged_unique')) {
        return { rows: [{ staged_unique: '0' }] };
      }
      if (sql.includes("key = 'active_works'")) {
        return { rows: [{ value: JSON.stringify([]) }] };
      }
      if (sql.includes("key = 'importer_protective_stop'")) {
        return { rows: [{ value: JSON.stringify({ active: false }) }] };
      }
      if (sql.includes("key = 'importer_auto_restarts'")) {
        return { rows: [{ value: JSON.stringify(pastRestarts) }] };
      }
      return { rows: [] };
    });

    // Simulate prior level attempts so Level 3 evaluates
    (watchdog as any).lastLevel1At = now;

    const metrics = await watchdog.evaluateCycle();

    expect(metrics.status).toBe('CRITICAL_STALL');
    expect(metrics.circuitBreakerOpen).toBe(true);
    expect(metrics.autoHealState).toBe('CIRCUIT_OPEN');
    expect(metrics.noProgressReason).toBe('RESTART_CIRCUIT_BREAKER_OPEN');
    expect(onControlledRestart).not.toHaveBeenCalled();
    expect(mockAutotuner.setCapacity).toHaveBeenCalledWith(
      1,
      'SURVIVAL',
      expect.stringContaining('Restart storm circuit breaker open')
    );
  });
});
