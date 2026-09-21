import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';

export interface ProtectiveStopInfo {
  active: boolean;
  reason?: string | null;
  details?: any;
  triggered_at?: string | null;
  resumed_at?: string | null;
  resumed_by?: string | null;
}

export interface SentinelThresholds {
  // Pre-SLA latency guard rails (triggered BEFORE user SLA is breached)
  homeTtfbPreSlaMs: number;    // 210ms (vs 250ms SLA)
  readerTtfbPreSlaMs: number;  // 130ms (vs 150ms SLA)
  mediaTtfbPreSlaMs: number;   // 105ms (vs 120ms SLA)
  
  // Infrastructure tripwires
  ysqlConnTripwire: number;    // 12 connections (vs 13 limit)
  maxRssMb: number;            // 440MB (vs 512MB container limit)
  maxEventLoopLagMs: number;   // 350ms
  maxTelegramFloodWaitSec: number; // 60s
}

export const DEFAULT_SENTINEL_THRESHOLDS: SentinelThresholds = {
  homeTtfbPreSlaMs: 210,
  readerTtfbPreSlaMs: 130,
  mediaTtfbPreSlaMs: 105,
  ysqlConnTripwire: 12,
  maxRssMb: 440,
  maxEventLoopLagMs: 350,
  maxTelegramFloodWaitSec: 60,
};

export class ProtectiveSentinel {
  private logger = new Logger('ProtectiveSentinel');
  private cachedInfo: ProtectiveStopInfo = { active: false };
  private lastFetchMs = 0;
  private cacheTtlMs = 3000; // 3 second cache
  private isRunning = false;
  private stopSignal = false;
  private consecutivePreSlaViolations = new Map<string, number>();

  constructor(
    private supabase: SupabaseClient,
    private thresholds: SentinelThresholds = DEFAULT_SENTINEL_THRESHOLDS,
    private siteUrl?: string
  ) {}

  /**
   * Checks whether the protective stop is currently active.
   * Reads from database 'settings' table with a 3s TTL cache.
   */
  async isProtectiveStopActive(): Promise<boolean> {
    const info = await this.getProtectiveStopInfo();
    return info.active;
  }

  /**
   * Retrieves full protective stop details from the settings table.
   */
  async getProtectiveStopInfo(forceFresh = false): Promise<ProtectiveStopInfo> {
    const now = Date.now();
    if (!forceFresh && now - this.lastFetchMs < this.cacheTtlMs) {
      return this.cachedInfo;
    }

    try {
      const { data, error } = await this.supabase
        .from('settings')
        .select('value')
        .eq('key', 'importer_protective_stop')
        .maybeSingle();

      if (!error && data?.value) {
        try {
          const parsed = typeof data.value === 'string' ? JSON.parse(data.value) : data.value;
          this.cachedInfo = {
            active: Boolean(parsed.active),
            reason: parsed.reason || null,
            details: parsed.details || null,
            triggered_at: parsed.triggered_at || null,
            resumed_at: parsed.resumed_at || null,
            resumed_by: parsed.resumed_by || null,
          };
        } catch {
          this.cachedInfo = { active: data.value === 'true' || data.value === 'ACTIVE' };
        }
      } else {
        this.cachedInfo = { active: false };
      }
      this.lastFetchMs = now;
    } catch (err: any) {
      this.logger.warn('Failed to fetch importer_protective_stop setting', { error: err?.message });
    }

    return this.cachedInfo;
  }

  /**
   * Triggers a persistent PROTECTIVE STOP.
   * Halts all new job claims, allows in-flight jobs to safely drain,
   * keeps publication barrier alive, and requires manual resumption.
   */
  async triggerProtectiveStop(reason: string, details: any): Promise<void> {
    const nowIso = new Date().toISOString();
    const payload: ProtectiveStopInfo = {
      active: true,
      reason,
      details,
      triggered_at: nowIso,
    };

    this.cachedInfo = payload;
    this.lastFetchMs = Date.now();

    this.logger.error(
      `🚨 [PROTECTIVE_STOP TRIGGERED] ${reason}. Halting new claims immediately. In-flight jobs will safely drain. Manual resumption required.`,
      { reason, details, triggered_at: nowIso }
    );

    try {
      await this.supabase.from('settings').upsert({
        key: 'importer_protective_stop',
        value: JSON.stringify(payload),
      });
    } catch (dbErr: any) {
      this.logger.error('Failed to persist importer_protective_stop to database', { error: dbErr?.message });
    }
  }

  /**
   * Resumes normal operation (intended for explicit manual/staff resumption).
   */
  async resumeProtectiveStop(resumedBy = 'manual_staff'): Promise<void> {
    const nowIso = new Date().toISOString();
    const payload: ProtectiveStopInfo = {
      active: false,
      resumed_at: nowIso,
      resumed_by: resumedBy,
    };

    this.cachedInfo = payload;
    this.lastFetchMs = Date.now();

    this.logger.info(`[PROTECTIVE_STOP RESUMED] Importer resumed by ${resumedBy}.`, { resumed_at: nowIso });

    try {
      await this.supabase.from('settings').upsert({
        key: 'importer_protective_stop',
        value: JSON.stringify(payload),
      });
    } catch (dbErr: any) {
      this.logger.error('Failed to clear importer_protective_stop in database', { error: dbErr?.message });
    }
  }

  /**
   * Background sentinel watchdog loop.
   * Probes pre-SLA metrics every 15s. If pre-SLA stress is detected, trips PROTECTIVE_STOP.
   */
  startWatchdogLoop(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.stopSignal = false;

    void (async () => {
      this.logger.info('Pre-SLA Sentinel watchdog loop started', {
        thresholds: this.thresholds,
        siteUrl: this.siteUrl || '(not configured)',
      });

      // Initial startup grace period (15s)
      await new Promise((r) => setTimeout(r, 15_000));

      while (!this.stopSignal) {
        try {
          // If already stopped, just refresh state and wait
          const isActive = await this.isProtectiveStopActive();
          if (!isActive) {
            await this.evaluatePreSlaGuardRails();
          }
        } catch (err: any) {
          this.logger.warn('Error during Sentinel pre-SLA evaluation', { error: err?.message });
        }

        await new Promise((r) => setTimeout(r, 15_000));
      }
      this.isRunning = false;
    })();
  }

  stop(): void {
    this.stopSignal = true;
  }

  /**
   * Evaluates all Pre-SLA guard rails.
   */
  async evaluatePreSlaGuardRails(): Promise<void> {
    // 1. RAM Check (rssMb > 440MB of 512MB)
    const mem = diagnostics.getMemorySnapshot();
    if (mem.rssMb >= this.thresholds.maxRssMb) {
      await this.triggerProtectiveStop(
        `Pre-SLA RAM Tripwire Exceeded: ${mem.rssMb}MB >= ${this.thresholds.maxRssMb}MB (512MB limit)`,
        { rssMb: mem.rssMb, heapUsedMb: mem.heapUsedMb }
      );
      return;
    }

    // 2. Event Loop Lag Check (> 350ms)
    const lagMetrics = (diagnostics as any).lagMonitor?.getMetrics?.() || { avgLagMs: 0 };
    if (lagMetrics.avgLagMs >= this.thresholds.maxEventLoopLagMs) {
      await this.triggerProtectiveStop(
        `Pre-SLA Event Loop Lag Tripwire Exceeded: ${lagMetrics.avgLagMs}ms >= ${this.thresholds.maxEventLoopLagMs}ms`,
        { avgLagMs: lagMetrics.avgLagMs }
      );
      return;
    }

    // 3. YSQL Connection Tripwire (>= 12 of 13)
    try {
      const { data: connData, error: connErr } = await this.supabase.rpc('importer_active_connections_count');
      let activeConns = 0;
      if (!connErr && typeof connData === 'number') {
        activeConns = connData;
      } else {
        // Fallback query via pg_stat_activity if RPC is not installed
        const { data: rawData } = await this.supabase
          .from('pg_stat_activity' as any)
          .select('pid', { count: 'exact', head: true });
        // If not accessible via supabase client, skip this step
      }

      if (activeConns >= this.thresholds.ysqlConnTripwire) {
        await this.triggerProtectiveStop(
          `Pre-SLA YSQL Connection Tripwire Exceeded: ${activeConns} active connections >= ${this.thresholds.ysqlConnTripwire} (limit 13)`,
          { activeConnections: activeConns, tripwire: this.thresholds.ysqlConnTripwire }
        );
        return;
      }
    } catch {
      // Non-fatal if table/rpc is restricted
    }

    // 4. Site Latency Probes (Home > 210ms, Reader > 130ms, Media > 105ms)
    if (this.siteUrl) {
      await this.probeSiteLatency('home', `${this.siteUrl}/`, this.thresholds.homeTtfbPreSlaMs, 250);
      await this.probeSiteLatency('reader', `${this.siteUrl}/api/health`, this.thresholds.readerTtfbPreSlaMs, 150);
    }
  }

  /**
   * Probes site route latency. Requires 2 consecutive violations before tripping to eliminate transient network blips.
   */
  private async probeSiteLatency(
    label: 'home' | 'reader' | 'media',
    url: string,
    thresholdMs: number,
    slaTargetMs: number
  ): Promise<void> {
    const t0 = performance.now();
    try {
      const res = await fetch(url, {
        method: 'GET',
        headers: { 'User-Agent': 'Project-Nox-Sentinel/1.0 (Pre-SLA Monitor)' },
        signal: AbortSignal.timeout(5000),
      });
      const ttfbMs = Math.round(performance.now() - t0);

      if (ttfbMs > thresholdMs) {
        const count = (this.consecutivePreSlaViolations.get(label) || 0) + 1;
        this.consecutivePreSlaViolations.set(label, count);

        this.logger.warn(
          `[Pre-SLA Latency Warning] Route ${label} (${url}) TTFB: ${ttfbMs}ms > threshold ${thresholdMs}ms (SLA: ${slaTargetMs}ms). Consecutive sample: ${count}/2`
        );

        if (count >= 2) {
          await this.triggerProtectiveStop(
            `Pre-SLA Latency Guard Rail Breached on ${label.toUpperCase()}: observed ${ttfbMs}ms > pre-SLA threshold ${thresholdMs}ms (SLA target: ${slaTargetMs}ms)`,
            { label, url, ttfbMs, thresholdMs, slaTargetMs, status: res.status }
          );
        }
      } else {
        this.consecutivePreSlaViolations.set(label, 0);
      }
    } catch (err: any) {
      // Transient timeout or network drop on probe: record 1 violation, do not trip immediately
      const count = (this.consecutivePreSlaViolations.get(label) || 0) + 1;
      this.consecutivePreSlaViolations.set(label, count);
      if (count >= 3) {
        await this.triggerProtectiveStop(
          `Pre-SLA Health Probe Failed on ${label.toUpperCase()} (${count} consecutive failures): ${err?.message}`,
          { label, url, error: err?.message }
        );
      }
    }
  }
}
