import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';
import { GlobalStorageRateLimiter } from './rate-limiter.js';

export type WatchdogStatus =
  | 'HEALTHY_IDLE'
  | 'HEALTHY_ACTIVE'
  | 'STALLED'
  | 'WORKER_OFFLINE'
  | 'DEGRADED_RATE_LIMITED';

export interface IngestionWatchdogInput {
  upstreamHasNewChapters: boolean;
  activeWorkerCount: number;
  pendingJobs: number;
  processingJobs: number;
  completedJobs: number;
  failedJobs: number;
  stalledJobs: number;
  rateLimitCooldownActive: boolean;
  rateLimitWaitMs: number;
  discoveryStarvation?: boolean;
  overdueDiscoverySourcesCount?: number;
  activeDiscoveryJobs?: number;
}

export interface WatchdogEvaluation {
  status: WatchdogStatus;
  scenarioName: string;
  description: string;
  alertNeeded: boolean;
  actionRequired: 'NONE' | 'WAIT_COOLDOWN' | 'SPAWN_WORKER' | 'RECOVER_STALLED';
  details: IngestionWatchdogInput;
  timestamp: string;
}

export class IngestionWatchdog {
  private logger = new Logger('IngestionWatchdog');

  constructor(
    private supabase?: SupabaseClient,
    private rateLimiter?: GlobalStorageRateLimiter
  ) {}

  /**
   * Pure evaluation of ingestion health based on input metrics.
   * Accurately determines which operational scenario the system is in.
   */
  public evaluate(input: IngestionWatchdogInput): WatchdogEvaluation {
    const timestamp = new Date().toISOString();

    // SCENARIO 5: Telegram rate limit backoff is active (DEGRADED_RATE_LIMITED)
    if (input.rateLimitCooldownActive || input.rateLimitWaitMs > 0) {
      return {
        status: 'DEGRADED_RATE_LIMITED',
        scenarioName: 'Scenario 5: Telegram 429 Rate Limit Cooldown',
        description: `Storage Bridge is in Telegram 429 cooldown (${Math.ceil(input.rateLimitWaitMs / 1000)}s remaining). Normal backoff behavior, do not panic or reset.`,
        alertNeeded: false,
        actionRequired: 'WAIT_COOLDOWN',
        details: input,
        timestamp,
      };
    }

    // SCENARIO 4: Backlog exists but 0 workers active (WORKER_OFFLINE)
    if (input.activeWorkerCount === 0 && (input.pendingJobs > 0 || input.stalledJobs > 0)) {
      return {
        status: 'WORKER_OFFLINE',
        scenarioName: 'Scenario 4: Concurrency Zero / Worker Offline',
        description: `Pending or stalled jobs exist (${input.pendingJobs} pending, ${input.stalledJobs} stalled) but active worker count is 0.`,
        alertNeeded: true,
        actionRequired: 'SPAWN_WORKER',
        details: input,
        timestamp,
      };
    }

    // SCENARIO 3: Stalled jobs or upstream has new chapters but queue is stalled
    if (input.stalledJobs > 0) {
      return {
        status: 'STALLED',
        scenarioName: 'Scenario 3: CASO B - Stalled Ingestion Jobs',
        description: `Stalled jobs detected in queue (${input.stalledJobs} jobs). Worker leases expired or failed to report progress.`,
        alertNeeded: true,
        actionRequired: 'RECOVER_STALLED',
        details: input,
        timestamp,
      };
    }

    // SCENARIO 3B: DISCOVERY_STARVATION (active sources overdue for discovery while chapters backlog starves queue)
    if (
      input.discoveryStarvation ||
      ((input.overdueDiscoverySourcesCount ?? 0) > 0 && (input.activeDiscoveryJobs ?? 0) === 0 && input.pendingJobs > 50)
    ) {
      return {
        status: 'STALLED',
        scenarioName: 'Scenario 3B: DISCOVERY_STARVATION - Discovery Starved by Backlog',
        description: `Discovery starvation detected: ${input.overdueDiscoverySourcesCount ?? 1} active source(s) overdue for discovery, but 0 discovery jobs active while queue has ${input.pendingJobs} pending tasks.`,
        alertNeeded: true,
        actionRequired: 'RECOVER_STALLED',
        details: input,
        timestamp,
      };
    }

    if (input.upstreamHasNewChapters && input.pendingJobs === 0 && input.processingJobs === 0) {
      return {
        status: 'STALLED',
        scenarioName: 'Scenario 3: CASO B - Discovery Gap / Pipeline Stalled',
        description: 'New chapters detected in upstream sources, but importer queue is idle/empty without active discovery.',
        alertNeeded: true,
        actionRequired: 'RECOVER_STALLED',
        details: input,
        timestamp,
      };
    }

    // SCENARIO 2: Healthy active ingestion
    if (input.processingJobs > 0 || (input.pendingJobs > 0 && input.activeWorkerCount > 0)) {
      return {
        status: 'HEALTHY_ACTIVE',
        scenarioName: 'Scenario 2: Healthy Active Ingestion',
        description: `Actively processing chapters (${input.processingJobs} processing, ${input.pendingJobs} pending, ${input.activeWorkerCount} active workers).`,
        alertNeeded: false,
        actionRequired: 'NONE',
        details: input,
        timestamp,
      };
    }

    // SCENARIO 1: CASO A - Healthy Idle
    // No new chapters upstream, queue is clean, workers alive or standby
    return {
      status: 'HEALTHY_IDLE',
      scenarioName: 'Scenario 1: CASO A - Healthy Idle',
      description: 'Upstream sources have no new chapters. Queue is completely drained. Zero false alerts, no reset or restarts needed.',
      alertNeeded: false,
      actionRequired: 'NONE',
      details: input,
      timestamp,
    };
  }

  /**
   * Queries Supabase and local rateLimiter state to generate a real-time watchdog evaluation.
   */
  public async checkLiveState(upstreamHasNewChapters: boolean = false): Promise<WatchdogEvaluation> {
    if (!this.supabase) {
      throw new Error('Supabase client required for checkLiveState');
    }

    const now = Date.now();

    // 1. Query active / importing jobs
    const { data: activeRows, error: activeErr } = await this.supabase
      .from('importer_queue')
      .select('status, locked_by, lease_expires_at')
      .in('status', ['IMPORTING', 'PROCESSING']);

    if (activeErr) {
      this.logger.error('Failed to query active rows in importer_queue', activeErr);
      throw activeErr;
    }

    let processing = 0;
    let stalled = 0;
    const activeWorkers = new Set<string>();

    for (const row of activeRows || []) {
      const leaseExpiry = row.lease_expires_at ? new Date(row.lease_expires_at).getTime() : 0;
      const isExpired = leaseExpiry > 0 && leaseExpiry < now;

      if (isExpired) {
        stalled++;
      } else {
        processing++;
        if (row.locked_by) activeWorkers.add(row.locked_by);
      }
    }

    // 2. Count pending jobs
    const { count: pendingCount, error: pendingErr } = await this.supabase
      .from('importer_queue')
      .select('*', { count: 'exact', head: true })
      .in('status', ['QUEUED', 'PENDING', 'RETRY']);

    if (pendingErr) {
      this.logger.error('Failed to count pending rows in importer_queue', pendingErr);
      throw pendingErr;
    }

    // 3. Count completed jobs
    const { count: completedCount } = await this.supabase
      .from('importer_queue')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'COMPLETED');

    // 4. Count failed jobs
    const { count: failedCount } = await this.supabase
      .from('importer_queue')
      .select('*', { count: 'exact', head: true })
      .eq('status', 'FAILED');

    const rateLimitCooldownActive = this.rateLimiter ? this.rateLimiter.isBlocked() : false;
    const rateLimitWaitMs = this.rateLimiter ? this.rateLimiter.getBlockedRemainingMs() : 0;

    // 5. Query active discovery jobs and sources overdue for discovery
    const { count: activeDiscoveries } = await this.supabase
      .from('importer_queue')
      .select('*', { count: 'exact', head: true })
      .in('task_type', ['DISCOVER_WORKS', 'SYNC_WORK'])
      .in('status', ['IMPORTING', 'PROCESSING']);

    const { data: activeSources } = await this.supabase
      .from('importer_sources')
      .select('id, last_sync_at, sync_interval_minutes')
      .eq('status', 'ACTIVE')
      .eq('enabled', true);

    let overdueSourcesCount = 0;
    for (const s of activeSources || []) {
      const lastSync = s.last_sync_at ? new Date(s.last_sync_at).getTime() : 0;
      const intervalMs = (s.sync_interval_minutes || 30) * 60 * 1000;
      if (now - lastSync > intervalMs * 2) {
        overdueSourcesCount++;
      }
    }

    const input: IngestionWatchdogInput = {
      upstreamHasNewChapters,
      activeWorkerCount: activeWorkers.size,
      pendingJobs: pendingCount || 0,
      processingJobs: processing,
      completedJobs: completedCount || 0,
      failedJobs: failedCount || 0,
      stalledJobs: stalled,
      rateLimitCooldownActive,
      rateLimitWaitMs,
      activeDiscoveryJobs: activeDiscoveries || 0,
      overdueDiscoverySourcesCount: overdueSourcesCount,
      discoveryStarvation: overdueSourcesCount > 0 && (activeDiscoveries || 0) === 0 && (pendingCount || 0) > 50,
    };

    return this.evaluate(input);
  }
}
