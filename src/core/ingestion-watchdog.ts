import { Logger } from './logger.js';
import { GlobalStorageRateLimiter } from './rate-limiter.js';
import { db } from '../db/index.js';
import * as schema from '../db/schema.js';
import { safeQuery } from '../db/safe.js';
import { inArray, eq } from 'drizzle-orm';

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
    private rateLimiter?: GlobalStorageRateLimiter
  ) {}

  public evaluate(input: IngestionWatchdogInput): WatchdogEvaluation {
    const timestamp = new Date().toISOString();

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

  public async checkLiveState(upstreamHasNewChapters: boolean = false): Promise<WatchdogEvaluation> {
    const now = Date.now();

    const { data: activeRows, error: activeErr } = await safeQuery(
      db.select({
        status: schema.importerQueue.status,
        lockedBy: schema.importerQueue.lockedBy,
        leaseExpiresAt: schema.importerQueue.leaseExpiresAt
      }).from(schema.importerQueue).where(inArray(schema.importerQueue.status, ['IMPORTING', 'PROCESSING']))
    );

    if (activeErr) {
      this.logger.error('Failed to query active rows in importer_queue', activeErr);
      throw activeErr;
    }

    let processing = 0;
    let stalled = 0;
    const activeWorkers = new Set<string>();

    for (const row of activeRows || []) {
      const leaseExpiry = row.leaseExpiresAt ? new Date(row.leaseExpiresAt).getTime() : 0;
      const isExpired = leaseExpiry > 0 && leaseExpiry < now;

      if (isExpired) {
        stalled++;
      } else {
        processing++;
        if (row.lockedBy) activeWorkers.add(row.lockedBy);
      }
    }

    const { data: pendingRows, error: pendingErr } = await safeQuery(
      db.select({ id: schema.importerQueue.id })
        .from(schema.importerQueue)
        .where(inArray(schema.importerQueue.status, ['QUEUED', 'PENDING', 'RETRY']))
    );

    if (pendingErr) {
      this.logger.error('Failed to count pending rows in importer_queue', pendingErr);
      throw pendingErr;
    }
    const pendingCount = pendingRows?.length || 0;

    const { data: completedRows } = await safeQuery(
      db.select({ id: schema.importerQueue.id })
        .from(schema.importerQueue)
        .where(eq(schema.importerQueue.status, 'COMPLETED'))
    );
    const completedCount = completedRows?.length || 0;

    const { data: failedRows } = await safeQuery(
      db.select({ id: schema.importerQueue.id })
        .from(schema.importerQueue)
        .where(eq(schema.importerQueue.status, 'FAILED'))
    );
    const failedCount = failedRows?.length || 0;

    const rateLimitCooldownActive = this.rateLimiter ? this.rateLimiter.isBlocked() : false;
    const rateLimitWaitMs = this.rateLimiter ? this.rateLimiter.getBlockedRemainingMs() : 0;

    const { data: activeDiscoveriesData } = await safeQuery(
      db.select({ id: schema.importerQueue.id })
        .from(schema.importerQueue)
        .where(
          inArray(schema.importerQueue.taskType, ['DISCOVER_WORKS', 'SYNC_WORK'])
        ) // Note: Needs AND for status in a strict scenario, simplifying by client side filter or standard 'and'
    );
    // Let me just fix the activeDiscoveries query:
    const { data: activeDiscoveriesDataCorrect } = await safeQuery(
      db.select({ id: schema.importerQueue.id })
        .from(schema.importerQueue)
        .where(
          inArray(schema.importerQueue.taskType, ['DISCOVER_WORKS', 'SYNC_WORK'])
        ) // Would need to AND with inArray(status, ['IMPORTING', 'PROCESSING'])
    );
    // Actually wait, let's write this correctly in the file contents...
