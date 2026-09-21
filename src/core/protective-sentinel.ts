import https from 'https';
import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';
import { getYugabytePool } from '../db/yugabyte-direct.js';

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
  private httpAgent = new https.Agent({ keepAlive: true, maxSockets: 5 });

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
      try {
        const pool = getYugabytePool();
        const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_protective_stop'");
        if (res.rows.length > 0 && res.rows[0].value) {
          const raw = res.rows[0].value;
          const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
          this.cachedInfo = {
            active: Boolean(parsed.active),
            reason: parsed.reason || null,
            details: parsed.details || null,
            triggered_at: parsed.triggered_at || null,
            resumed_at: parsed.resumed_at || null,
            resumed_by: parsed.resumed_by || null,
          };
          this.lastFetchMs = now;
          return this.cachedInfo;
        }
      } catch {
        // Fall through to supabase
      }

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
    this.consecutivePreSlaViolations.clear();

    this.logger.error(
      `🚨 [PROTECTIVE_STOP TRIGGERED] ${reason}. Halting new claims immediately. In-flight jobs will safely drain. Manual resumption required.`,
      { reason, details, triggered_at: nowIso }
    );

    try {
      const pool = getYugabytePool();
      await pool.query(
        "INSERT INTO settings (key, value) VALUES ('importer_protective_stop', $1) ON CONFLICT (key) DO UPDATE SET value = $1",
        [JSON.stringify(payload)]
      );
    } catch {
      try {
        await this.supabase.from('settings').upsert({
          key: 'importer_protective_stop',
          value: JSON.stringify(payload),
        });
      } catch (dbErr: any) {
        this.logger.error('Failed to persist importer_protective_stop to database', { error: dbErr?.message });
      }
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
    this.consecutivePreSlaViolations.clear();

    this.logger.info(`[PROTECTIVE_STOP RESUMED] Importer resumed by ${resumedBy}.`, { resumed_at: nowIso });

    try {
      const pool = getYugabytePool();
      await pool.query(
        "INSERT INTO settings (key, value) VALUES ('importer_protective_stop', $1) ON CONFLICT (key) DO UPDATE SET value = $1",
        [JSON.stringify(payload)]
      );
    } catch {
      try {
        await this.supabase.from('settings').upsert({
          key: 'importer_protective_stop',
          value: JSON.stringify(payload),
        });
      } catch (dbErr: any) {
        this.logger.error('Failed to clear importer_protective_stop in database', { error: dbErr?.message });
      }
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
          const isActive = await this.isProtectiveStopActive();
          if (!isActive) {
            await this.evaluatePreSlaGuardRails();
          } else {
            await this.evaluateAutoResume();
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
   * Evaluates whether a currently stopped importer can safely auto-resume.
   * Auto-resumes for transient edge spikes, socket hang-ups, or cleared external glitches.
   * NEVER auto-resumes manual staff stops or active ongoing degradation.
   */
  async evaluateAutoResume(): Promise<void> {
    try {
      const stopInfo = await this.getProtectiveStopInfo(true);
      if (!stopInfo.active) return;

      // Staff manual stops require staff manual resumption
      if (stopInfo.reason?.toLowerCase().includes('manual') || stopInfo.reason?.toLowerCase().includes('staff')) {
        return;
      }

      // Check current infrastructure
      const mem = diagnostics.getMemorySnapshot();
      if (mem.rssMb >= this.thresholds.maxRssMb - 40) return;

      let activeConns = 0;
      let totalConns = 0;
      try {
        const pool = getYugabytePool();
        const cRes = await pool.query(`
          SELECT count(*) as total,
                 count(*) FILTER (WHERE state = 'active') as active
          FROM pg_stat_activity
        `);
        totalConns = parseInt(cRes.rows[0]?.total || '0', 10);
        activeConns = parseInt(cRes.rows[0]?.active || '0', 10);
      } catch {}
      if (totalConns >= this.thresholds.ysqlConnTripwire || activeConns >= 8) return;

      // Quick latency probes
      if (this.siteUrl) {
        const homeProbe = await this.measureRoute(`${this.siteUrl}/`, this.thresholds.homeTtfbPreSlaMs);
        const readerProbe = await this.measureRoute(`${this.siteUrl}/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c`, this.thresholds.readerTtfbPreSlaMs);

        const isHomeHealthy = Boolean(homeProbe && homeProbe.statusCode >= 200 && homeProbe.statusCode < 400 && homeProbe.ttfbMs <= this.thresholds.homeTtfbPreSlaMs);
        const isReaderHealthy = Boolean(readerProbe && readerProbe.statusCode >= 200 && readerProbe.statusCode < 400 && readerProbe.ttfbMs <= this.thresholds.readerTtfbPreSlaMs);

        if (isHomeHealthy && isReaderHealthy) {
          this.consecutiveHealthySamples = (this.consecutiveHealthySamples || 0) + 1;
          this.logger.info(
            `[Auto-Resume Evaluation] Confirmed healthy sample ${this.consecutiveHealthySamples}/2 (Home: ${homeProbe!.ttfbMs}ms [${homeProbe!.statusCode}], Reader: ${readerProbe!.ttfbMs}ms [${readerProbe!.statusCode}], YSQL: ${activeConns}/13)`
          );

          if (this.consecutiveHealthySamples >= 2) {
            this.consecutiveHealthySamples = 0;
            this.logger.info(
              '🛡️ [AUTO-RESUME] Site and infrastructure confirmed completely healthy. Auto-resuming claims.'
            );
            await this.resumeProtectiveStop('auto_healing_sentinel_recovery');
          }
          return;
        } else {
          this.consecutiveHealthySamples = 0;
        }
      }
    } catch (err: any) {
      this.logger.warn('Failed during auto-resume evaluation', { error: err?.message });
    }
  }

  private async measureRoute(url: string, thresholdMs: number): Promise<{ ttfbMs: number; statusCode: number } | null> {
    return new Promise((resolve) => {
      const t0 = performance.now();
      const isHttps = url.startsWith('https:');
      const mod = isHttps ? https : require('http');

      const req = mod.get(
        url,
        {
          agent: false,
          headers: { 'User-Agent': 'Project-Nox-Sentinel/1.0 (Auto-Resume Probe)' },
          timeout: 4000,
        },
        (res: any) => {
          let resolved = false;
          const finish = () => {
            if (resolved) return;
            resolved = true;
            try { res.resume(); } catch {}
            resolve({
              ttfbMs: Math.round(performance.now() - t0),
              statusCode: res.statusCode || 500,
            });
          };
          res.once('data', finish);
          res.on('end', finish);
        }
      );

      req.on('error', () => resolve(null));
      req.on('timeout', () => { req.destroy(); resolve(null); });
    });
  }

  private consecutiveHealthySamples = 0;

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
      let activeConns = 0;
      try {
        const pool = getYugabytePool();
        const cRes = await pool.query('SELECT count(*) FROM pg_stat_activity');
        activeConns = parseInt(cRes.rows[0]?.count || '0', 10);
      } catch {
        const { data: connData, error: connErr } = await this.supabase.rpc('importer_active_connections_count');
        if (!connErr && typeof connData === 'number') activeConns = connData;
      }

      if (activeConns >= this.thresholds.ysqlConnTripwire) {
        await this.triggerProtectiveStop(
          `Pre-SLA YSQL Connection Tripwire Exceeded: ${activeConns} active connections >= ${this.thresholds.ysqlConnTripwire} (limit 13)`,
          { activeConnections: activeConns, tripwire: this.thresholds.ysqlConnTripwire }
        );
        return;
      }
    } catch {}

    // 4. Site Latency Probes (Home > 210ms, Reader > 130ms, Media > 105ms)
    if (this.siteUrl) {
      await this.probeSiteLatency('home', `${this.siteUrl}/`, this.thresholds.homeTtfbPreSlaMs, 250);
      await this.probeSiteLatency('reader', `${this.siteUrl}/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c`, this.thresholds.readerTtfbPreSlaMs, 150);
    }
  }

  /**
   * Probes site route latency using keep-alive connection. Requires 2 consecutive violations before tripping to eliminate transient network blips.
   */
  private async probeSiteLatency(
    label: 'home' | 'reader' | 'media',
    url: string,
    thresholdMs: number,
    slaTargetMs: number,
    isRetry = false
  ): Promise<void> {
    return new Promise<void>((resolve) => {
      const t0 = performance.now();
      const isHttps = url.startsWith('https:');
      const mod = isHttps ? https : require('http');

      const req = mod.get(
        url,
        {
          agent: false,
          headers: { 'User-Agent': 'Project-Nox-Sentinel/1.0 (Pre-SLA Monitor)' },
          timeout: 5000,
        },
        (res: any) => {
          let resolved = false;
          const finish = async () => {
            if (resolved) return;
            resolved = true;
            try {
              res.resume();
            } catch {}
            const ttfbMs = Math.round(performance.now() - t0);
            await this.handleProbeResult(label, url, ttfbMs, thresholdMs, slaTargetMs, res.statusCode || 200);
            resolve();
          };

          res.once('data', () => { void finish(); });
          res.on('end', () => { void finish(); });
        }
      );

      req.on('error', async (err: any) => {
        if (!isRetry && (err?.message?.includes('socket hang up') || err?.code === 'ECONNRESET')) {
          this.logger.info(`Transient reset on ${label} probe, retrying with fresh socket...`);
          return this.probeSiteLatency(label, url, thresholdMs, slaTargetMs, true).then(resolve);
        }
        await this.handleProbeError(label, url, err);
        resolve();
      });

      req.on('timeout', () => {
        req.destroy();
        void this.handleProbeError(label, url, new Error('Request timed out after 5000ms')).then(() => resolve());
      });
    });
  }

  private async handleProbeResult(
    label: 'home' | 'reader' | 'media',
    url: string,
    ttfbMs: number,
    thresholdMs: number,
    slaTargetMs: number,
    statusCode: number
  ): Promise<void> {
    const isStatusError = statusCode >= 500;
    const isViolating = ttfbMs > thresholdMs || isStatusError;

    if (isViolating) {
      const count = (this.consecutivePreSlaViolations.get(label) || 0) + 1;
      this.consecutivePreSlaViolations.set(label, count);

      // Check for correlated importer pressure
      const mem = diagnostics.getMemorySnapshot();
      const lagMetrics = (diagnostics as any).lagMonitor?.getMetrics?.() || { avgLagMs: 0 };
      let activeConns = 0;
      let totalConns = 0;
      try {
        const pool = getYugabytePool();
        const cRes = await pool.query(`
          SELECT count(*) as total,
                 count(*) FILTER (WHERE state = 'active') as active
          FROM pg_stat_activity
        `);
        totalConns = parseInt(cRes.rows[0]?.total || '0', 10);
        activeConns = parseInt(cRes.rows[0]?.active || '0', 10);
      } catch {}

      // True infra pressure: YSQL near exhaustion (>= 12), query pileup (active >= 8), RAM near limit (>= 380MB), or event loop blocked (>= 200ms)
      const hasInfraPressure = totalConns >= 12 || activeConns >= 8 || mem.rssMb >= 380 || lagMetrics.avgLagMs >= 200;
      const isSlaBreached = ttfbMs >= slaTargetMs;
      // Stop if:
      // 1. Confirmed backend failure (HTTP 5xx for 3+ consecutive probes), OR
      // 2. Latency exceeding SLA target for 2 consecutive probes WITH confirmed infra pressure, OR
      // 3. Pre-SLA threshold exceeded for 3+ consecutive probes WITH confirmed infra pressure
      const shouldTrip = (isStatusError && count >= 3) ||
                         (isSlaBreached && count >= 2 && hasInfraPressure) ||
                         (count >= 3 && hasInfraPressure);

      if (shouldTrip) {
        this.consecutivePreSlaViolations.set(label, 0);
        await this.triggerProtectiveStop(
          `Pre-SLA Guard Rail Breached with Correlated Importer Pressure on ${label.toUpperCase()}: observed ${ttfbMs}ms (HTTP ${statusCode}) > ${isSlaBreached ? `SLA target ${slaTargetMs}ms` : `threshold ${thresholdMs}ms`} (YSQL: ${totalConns}/13 total [${activeConns} active], RSS: ${mem.rssMb}MB, Lag: ${lagMetrics.avgLagMs}ms)`,
          { label, url, ttfbMs, thresholdMs, slaTargetMs, status: statusCode, totalConns, activeConns, rssMb: mem.rssMb, lagMs: lagMetrics.avgLagMs }
        );
      } else if (count >= 2) {
        this.logger.warn(
          `[EDGE_TRANSIENT_WARNING] Route ${label} (${url}) TTFB: ${ttfbMs}ms (HTTP ${statusCode}) > threshold ${thresholdMs}ms, but infra is within safe bounds (YSQL: ${totalConns}/13 total [${activeConns} active], RSS: ${mem.rssMb}MB, Lag: ${lagMetrics.avgLagMs}ms). Observing without tripping PROTECTIVE_STOP.`
        );
      } else {
        this.logger.warn(
          `[Pre-SLA Latency Warning] Route ${label} (${url}) TTFB: ${ttfbMs}ms (HTTP ${statusCode}) > threshold ${thresholdMs}ms (SLA: ${slaTargetMs}ms). Consecutive sample: ${count}/2`
        );
      }
    } else {
      this.consecutivePreSlaViolations.set(label, 0);
    }
  }

  private async handleProbeError(label: 'home' | 'reader' | 'media', url: string, err: any): Promise<void> {
    const count = (this.consecutivePreSlaViolations.get(label) || 0) + 1;
    this.consecutivePreSlaViolations.set(label, count);

    // Check for correlated importer pressure
    const mem = diagnostics.getMemorySnapshot();
    const lagMetrics = (diagnostics as any).lagMonitor?.getMetrics?.() || { avgLagMs: 0 };
    let activeConns = 0;
    let totalConns = 0;
    try {
      const pool = getYugabytePool();
      const cRes = await pool.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE state = 'active') as active
        FROM pg_stat_activity
      `);
      totalConns = parseInt(cRes.rows[0]?.total || '0', 10);
      activeConns = parseInt(cRes.rows[0]?.active || '0', 10);
    } catch {}

    const hasInfraPressure = totalConns >= 12 || activeConns >= 8 || mem.rssMb >= 380 || lagMetrics.avgLagMs >= 200;

    if (count >= 5) {
      if (hasInfraPressure) {
        this.consecutivePreSlaViolations.set(label, 0);
        await this.triggerProtectiveStop(
          `Pre-SLA Health Probe Failed on ${label.toUpperCase()} (${count} consecutive failures) with Correlated Importer Pressure: ${err?.message} (YSQL: ${totalConns}/13 total [${activeConns} active], RSS: ${mem.rssMb}MB, Lag: ${lagMetrics.avgLagMs}ms)`,
          { label, url, error: err?.message, totalConns, activeConns, rssMb: mem.rssMb, lagMs: lagMetrics.avgLagMs }
        );
      } else {
        this.logger.warn(
          `[EDGE_PROBE_ERROR_WARNING] Health probe on ${label} (${url}) failed (${count} consecutive): ${err?.message}, but importer infra is healthy (YSQL: ${totalConns}/13 total [${activeConns} active], RSS: ${mem.rssMb}MB). Observing without tripping PROTECTIVE_STOP.`
        );
      }
    }
  }
}
