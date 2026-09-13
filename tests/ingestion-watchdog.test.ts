import { describe, it, expect } from 'vitest';
import { IngestionWatchdog, IngestionWatchdogInput } from '../src/core/ingestion-watchdog.js';

describe('IngestionWatchdog - Scenario & Starvation Detection', () => {
  const watchdog = new IngestionWatchdog();

  it('detects CASO A - Healthy Idle when queue is empty and no new chapters upstream', () => {
    const input: IngestionWatchdogInput = {
      upstreamHasNewChapters: false,
      activeWorkerCount: 1,
      pendingJobs: 0,
      processingJobs: 0,
      completedJobs: 100,
      failedJobs: 0,
      stalledJobs: 0,
      rateLimitCooldownActive: false,
      rateLimitWaitMs: 0,
    };

    const result = watchdog.evaluate(input);
    expect(result.status).toBe('HEALTHY_IDLE');
    expect(result.alertNeeded).toBe(false);
    expect(result.actionRequired).toBe('NONE');
  });

  it('detects Scenario 2 - Healthy Active Ingestion when workers are processing jobs', () => {
    const input: IngestionWatchdogInput = {
      upstreamHasNewChapters: true,
      activeWorkerCount: 3,
      pendingJobs: 50,
      processingJobs: 3,
      completedJobs: 150,
      failedJobs: 0,
      stalledJobs: 0,
      rateLimitCooldownActive: false,
      rateLimitWaitMs: 0,
    };

    const result = watchdog.evaluate(input);
    expect(result.status).toBe('HEALTHY_ACTIVE');
    expect(result.alertNeeded).toBe(false);
  });

  it('detects Scenario 3B - DISCOVERY_STARVATION when active sources are overdue for discovery but starved behind backlog', () => {
    const input: IngestionWatchdogInput = {
      upstreamHasNewChapters: true,
      activeWorkerCount: 2,
      pendingJobs: 21929, // Huge chapter backlog
      processingJobs: 2,
      completedJobs: 6630,
      failedJobs: 52,
      stalledJobs: 0,
      rateLimitCooldownActive: false,
      rateLimitWaitMs: 0,
      discoveryStarvation: true,
      overdueDiscoverySourcesCount: 5,
      activeDiscoveryJobs: 0,
    };

    const result = watchdog.evaluate(input);
    expect(result.status).toBe('STALLED');
    expect(result.scenarioName).toContain('DISCOVERY_STARVATION');
    expect(result.alertNeeded).toBe(true);
    expect(result.actionRequired).toBe('RECOVER_STALLED');
    expect(result.description).toContain('Discovery starvation detected');
  });

  it('detects Scenario 4 - Worker Offline when backlog exists but activeWorkerCount is 0', () => {
    const input: IngestionWatchdogInput = {
      upstreamHasNewChapters: false,
      activeWorkerCount: 0,
      pendingJobs: 25,
      processingJobs: 0,
      completedJobs: 100,
      failedJobs: 0,
      stalledJobs: 0,
      rateLimitCooldownActive: false,
      rateLimitWaitMs: 0,
    };

    const result = watchdog.evaluate(input);
    expect(result.status).toBe('WORKER_OFFLINE');
    expect(result.alertNeeded).toBe(true);
    expect(result.actionRequired).toBe('SPAWN_WORKER');
  });

  it('detects Scenario 5 - Telegram 429 Rate Limit Cooldown', () => {
    const input: IngestionWatchdogInput = {
      upstreamHasNewChapters: true,
      activeWorkerCount: 2,
      pendingJobs: 10,
      processingJobs: 2,
      completedJobs: 50,
      failedJobs: 0,
      stalledJobs: 0,
      rateLimitCooldownActive: true,
      rateLimitWaitMs: 12000,
    };

    const result = watchdog.evaluate(input);
    expect(result.status).toBe('DEGRADED_RATE_LIMITED');
    expect(result.alertNeeded).toBe(false);
    expect(result.actionRequired).toBe('WAIT_COOLDOWN');
  });
});
