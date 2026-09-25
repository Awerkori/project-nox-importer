import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  AutoHealWatchdog,
  type HealthPanelMetrics,
  type AutoRestartRecord,
} from '../src/core/auto-heal-watchdog.js';
import { ImporterEngine } from '../src/core/engine.js';
import { diagnostics } from '../src/core/diagnostics.js';

describe('AutoHealWatchdog — Autonomous Recovery & Liveness Hardening (Casos A a J)', () => {
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
      if (sql.includes('recent_total')) {
        return { rows: [{ recent_total: '10', recent_dedupe: '10' }] };
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
});
