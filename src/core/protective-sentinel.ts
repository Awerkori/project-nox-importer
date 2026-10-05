import http from 'http';
import https from 'https';
import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';
import { getYugabytePool } from '../db/yugabyte-direct.js';

export type IncidentClassification =
  | 'MANUAL_STOP'
  | 'TRANSIENT_EDGE_INCIDENT'
  | 'REAL_SYSTEM_PRESSURE'
  | 'YSQL_PRESSURE'
  | 'IMPORTER_PRESSURE';

export type SiteHealthState = 'GREEN' | 'YELLOW' | 'ORANGE' | 'RED';
type SiteProbeLabel = 'home' | 'reader' | 'health';

export interface AutoEmergencyPauseState {
  active: boolean;
  pausedAt: string | null;
  reason: string | null;
  siteP95: number | null;
  consecutiveCatastrophicCycles: number;
  nextRecheckAt: string | null;
  resumedAt: string | null;
  healthyCyclesCount: number;
}

export interface PressureSnapshot {
  timestamp: number;
  siteHealth: SiteHealthState;
  homeP50: number;
  homeP95: number;
  readerP50: number;
  readerP95: number;
  consecutive5xx: number;
  lastHttp5xx: number | null;
  ysqlTotal: number;
  ysqlActive: number;
  poolWait: number;
  rssMb: number;
  heapUsedMb: number;
  eventLoopLagMs: number;
  pressureScore: number; // 0 - 100
  pressureBreakdown: {
    sitePressure: number;
    dbPressure: number;
    memoryPressure: number;
    eventLoopPressure: number;
    storagePressure: number;
    sourcePressure: number;
    publicationPressure: number;
  };
  pressureReason: string;
  // A public HTML probe can occasionally be routed differently from a real
  // browser by an edge/WAF.  Keep that visible without turning an isolated
  // monitor-route mismatch into a global importer throttle.
  probeRouteMismatch: boolean;
  probeRouteMismatchCycles: number;
}

export interface ProtectiveStopInfo {
  active: boolean;
  reason?: string | null;
  classification?: IncidentClassification | null;
  details?: any;
  triggered_at?: string | null;
  resumed_at?: string | null;
  resumed_by?: string | null;
}

export interface SentinelThresholds {
  // Pre-SLA latency guard rails (WAN calibrated)
  homeTtfbPreSlaMs: number;    // 800ms warning (WAN SSR cold start / routing)
  readerTtfbPreSlaMs: number;  // 600ms warning
  mediaTtfbPreSlaMs: number;   // 300ms
  
  // Infrastructure tripwires
  ysqlConnTripwire: number;    // 12 connections (vs 13 limit)
  maxRssMb: number;            // 440MB (vs 512MB container limit)
  maxEventLoopLagMs: number;   // 350ms
  maxTelegramFloodWaitSec: number; // 60s
}

export const DEFAULT_SENTINEL_THRESHOLDS: SentinelThresholds = {
  homeTtfbPreSlaMs: 800,
  readerTtfbPreSlaMs: 600,
  mediaTtfbPreSlaMs: 300,
  ysqlConnTripwire: 12,
  maxRssMb: 440,
  maxEventLoopLagMs: 350,
  maxTelegramFloodWaitSec: 60,
};

// The sentinel must exercise the same public HTML route a reader sees. Some
// edge configurations legitimately route bot-like/no-Accept probes
// differently, which would turn a probe artifact into an importer throttle.
// This is intentionally a normal browser navigation header set, not a bypass
// header and not an authenticated request.
export const SITE_PROBE_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
};

export interface LatencySample {
  ttfbMs: number;
  timestamp: number;
}

export class ProtectiveSentinel {
  private logger = new Logger('ProtectiveSentinel');
  private cachedInfo: ProtectiveStopInfo = { active: false };
  private lastFetchMs = 0;
  private cacheTtlMs = 3000; // 3 second cache
  private isRunning = false;
  private stopSignal = false;
  
  // Rolling latency windows for p50/p95 with 75s expiration window (max 20 samples)
  private homeSamples: LatencySample[] = [];
  private readerSamples: LatencySample[] = [];
  private readonly LATENCY_SAMPLE_WINDOW_MS = 75_000;
  private consecutive5xxCount = 0;
  private last5xxTimestamp: number | null = null;
  private consecutiveProbeFailures = 0;
  private route4xxLabels = new Set<Exclude<SiteProbeLabel, 'health'>>();
  private lastProbeStatus: Partial<Record<SiteProbeLabel, number>> = {};
  private probeRouteMismatch = false;
  private probeRouteMismatchCycles = 0;
  // A single pg_stat_activity sample includes the monitor's own query and can
  // briefly spike while ordinary claims complete.  Mild DB pressure must be
  // sustained (or accompanied by a local pool waiter) before it changes
  // importer capacity.  Severe pressure remains immediate.
  private consecutiveMildDbPressureCycles = 0;

  // Cached dynamic chapter ID for Reader probe (refreshed every 5 min)
  private cachedReaderChapterId: string | null = null;
  private cachedReaderChapterAt = 0;

  // Latest computed pressure snapshot
  private latestSnapshot: PressureSnapshot = {
    timestamp: Date.now(),
    siteHealth: 'GREEN',
    homeP50: 0,
    homeP95: 0,
    readerP50: 0,
    readerP95: 0,
    consecutive5xx: 0,
    lastHttp5xx: null,
    ysqlTotal: 0,
    ysqlActive: 0,
    poolWait: 0,
    rssMb: 0,
    heapUsedMb: 0,
    eventLoopLagMs: 0,
    pressureScore: 0,
    pressureBreakdown: {
      sitePressure: 0,
      dbPressure: 0,
      memoryPressure: 0,
      eventLoopPressure: 0,
      storagePressure: 0,
      sourcePressure: 0,
      publicationPressure: 0,
    },
    pressureReason: 'System initialized and healthy',
    probeRouteMismatch: false,
    probeRouteMismatchCycles: 0,
  };

  private homeAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
  private readerAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
  private httpAgent = new http.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });

  private catastrophicCyclesCount = 0;
  private healthyCyclesCount = 0;
  private autoEmergencyPause: AutoEmergencyPauseState = {
    active: false,
    pausedAt: null,
    reason: null,
    siteP95: null,
    consecutiveCatastrophicCycles: 0,
    nextRecheckAt: null,
    resumedAt: null,
    healthyCyclesCount: 0,
  };
  private onAutoResume?: () => void;

  constructor(
    private supabase: SupabaseClient,
    private thresholds: SentinelThresholds = DEFAULT_SENTINEL_THRESHOLDS,
    private siteUrl?: string,
    private dbPool?: any
  ) {}

  /**
   * The engine finishes constructing its bounded direct pool after creating
   * the sentinel. Attach that shared pool before the watchdog starts so the
   * monitor never creates a second, unbudgeted Yugabyte pool of its own.
   */
  setDbPool(pool: any): void {
    this.dbPool = pool;
  }

  private getPool(): any {
    return this.dbPool !== undefined ? this.dbPool : getYugabytePool();
  }

  setOnAutoResume(fn: () => void): void {
    this.onAutoResume = fn;
  }

  isEmergencyPaused(): boolean {
    if (this.autoEmergencyPause.active) {
      const pausedMs = this.autoEmergencyPause.pausedAt ? Date.parse(this.autoEmergencyPause.pausedAt) : 0;
      if (pausedMs > 0 && Date.now() - pausedMs > 180_000) {
        this.autoEmergencyPause.active = false;
        this.autoEmergencyPause.resumedAt = new Date().toISOString();
        this.autoEmergencyPause.reason = 'Auto-expired after 3-minute safety limit';
        this.logger.info('✅ [AUTO_EMERGENCY_PAUSE EXPIRED] Auto-emergency pause reached 3-minute safety limit. Resuming pipeline.');
        void this.persistAutoEmergencyPause();
      }
    }
    return this.autoEmergencyPause.active;
  }

  getEmergencyPauseState(): AutoEmergencyPauseState {
    return { ...this.autoEmergencyPause };
  }

  /**
   * Checks whether a MANUAL staff protective stop is active.
   * STRICT INVARIANT: Automatic performance stops CANNOT make this return true.
   * If a legacy automatic stop exists in DB, it is auto-cleared on discovery.
   */
  async isProtectiveStopActive(): Promise<boolean> {
    const info = await this.getProtectiveStopInfo();
    if (!info.active) return false;

    const isManual =
      info.classification === 'MANUAL_STOP' ||
      info.reason?.toLowerCase().includes('manual') ||
      info.reason?.toLowerCase().includes('staff');

    if (isManual) {
      return true;
    }

    // Auto-clear legacy automatic performance stop
    this.logger.warn(
      `[ADAPTIVE_MIGRATION] Ignoring and clearing legacy automatic performance stop: "${info.reason}" (classification: ${info.classification})`
    );
    void this.resumeProtectiveStop('ADAPTIVE_MIGRATION');
    return false;
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
        const pool = this.getPool();
        if (pool && typeof pool.query === 'function') {
          const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_protective_stop'");
          if (res.rows.length > 0 && res.rows[0].value) {
            const raw = res.rows[0].value;
            const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
            this.cachedInfo = {
              active: Boolean(parsed.active),
              reason: parsed.reason || null,
              classification: parsed.classification || null,
              details: parsed.details || null,
              triggered_at: parsed.triggered_at || null,
              resumed_at: parsed.resumed_at || null,
              resumed_by: parsed.resumed_by || null,
            };
            this.lastFetchMs = now;
            return this.cachedInfo;
          }
        }
      } catch {}

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
            classification: parsed.classification || null,
            details: parsed.details || null,
            triggered_at: parsed.triggered_at || null,
            resumed_at: parsed.resumed_at || null,
            resumed_by: parsed.resumed_by || null,
          };
        } catch {
          this.cachedInfo = { active: false };
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
   * On startup, auto-clears any legacy automatic protective stop if active.
   */
  async clearLegacyProtectiveStopOnStartup(): Promise<void> {
    try {
      const info = await this.getProtectiveStopInfo(true);
      if (info.active) {
        const isManual =
          info.classification === 'MANUAL_STOP' ||
          info.reason?.toLowerCase().includes('manual') ||
          info.reason?.toLowerCase().includes('staff');
        if (!isManual) {
          this.logger.warn(
            `[ADAPTIVE_MIGRATION] Cleared legacy automatic protective stop on boot (was: "${info.reason}", classification: ${info.classification})`
          );
          await this.resumeProtectiveStop('ADAPTIVE_MIGRATION');
        }
      }
    } catch (err: any) {
      this.logger.warn('Failed checking legacy protective stop on boot', { error: err?.message });
    }
  }

  /**
   * On startup, hydrates auto emergency pause state from settings table.
   * If an active emergency pause is found:
   * - Restores in-memory state so claims remain gated immediately
   * - Validates timestamp and format
   * - Re-evaluates site health immediately
   * - If site is still catastrophic, keeps claims gated
   * - If site is already healthy, starts auto-resume recovery window
   */
  async hydrateAutoEmergencyPauseOnStartup(stateOverride?: AutoEmergencyPauseState): Promise<void> {
    if (stateOverride) {
      this.autoEmergencyPause = { ...stateOverride };
      if (this.autoEmergencyPause.active) {
        this.latestSnapshot.siteHealth = 'RED';
        this.latestSnapshot.pressureReason = `AUTO_EMERGENCY_PAUSE: ${this.autoEmergencyPause.reason}`;
        if (this.siteUrl) {
          await this.evaluatePreSlaGuardRails();
        }
      }
      return;
    }

    try {
      let raw: any = null;
      try {
        const pool = this.dbPool !== undefined ? this.dbPool : getYugabytePool();
        if (pool && typeof pool.query === 'function') {
          const res = await pool.query("SELECT value FROM settings WHERE key = 'importer_auto_emergency_pause'");
          if (res.rows.length > 0 && res.rows[0].value) {
            raw = res.rows[0].value;
          }
        }
      } catch {}

      if (!raw) {
        const { data, error } = await this.supabase
          .from('settings')
          .select('value')
          .eq('key', 'importer_auto_emergency_pause')
          .maybeSingle();
        if (!error && data?.value) {
          raw = data.value;
        }
      }

      if (raw) {
        const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
        if (parsed && typeof parsed === 'object' && parsed.active === true) {
          const parsedPausedAt = parsed.pausedAt && !isNaN(Date.parse(parsed.pausedAt))
            ? parsed.pausedAt
            : new Date().toISOString();

          // Do NOT restore emergency pauses older than 3 minutes (stale from prior incidents)
          const pauseAgeMs = Date.now() - Date.parse(parsedPausedAt);
          if (pauseAgeMs > 180_000) {
            this.logger.info(`[AUTO_EMERGENCY_PAUSE EXPIRED ON BOOT] Stale pause from ${parsedPausedAt} (${Math.round(pauseAgeMs / 1000)}s ago) ignored and cleared.`);
            this.autoEmergencyPause.active = false;
            void this.persistAutoEmergencyPause();
            return;
          }

          this.autoEmergencyPause = {
            active: true,
            pausedAt: parsedPausedAt,
            reason: parsed.reason || 'Restored active auto emergency pause from settings on boot',
            siteP95: typeof parsed.siteP95 === 'number' ? parsed.siteP95 : null,
            consecutiveCatastrophicCycles: typeof parsed.consecutiveCatastrophicCycles === 'number' ? parsed.consecutiveCatastrophicCycles : 3,
            nextRecheckAt: parsed.nextRecheckAt || new Date(Date.now() + 15_000).toISOString(),
            resumedAt: null,
            healthyCyclesCount: 0,
          };

          this.logger.warn(
            `🚨 [AUTO_EMERGENCY_PAUSE HYDRATED] Loaded active emergency pause on boot (pausedAt: ${this.autoEmergencyPause.pausedAt}, reason: ${this.autoEmergencyPause.reason}). Claims remain gated until site health is verified.`
          );

          this.latestSnapshot.siteHealth = 'RED';
          this.latestSnapshot.pressureReason = `AUTO_EMERGENCY_PAUSE: ${this.autoEmergencyPause.reason}`;

          // Re-evaluate site health immediately if siteUrl is configured
          if (this.siteUrl) {
            await this.evaluatePreSlaGuardRails();
          }
        }
      }
    } catch (err: any) {
      this.logger.warn('Failed to hydrate importer_auto_emergency_pause on startup', { error: err?.message });
    }
  }

  /**
   * Triggers a MANUAL staff protective stop.
   * AUTOMATIC PERFORMANCE STOPS ARE STRICTLY FORBIDDEN.
   * If called with classification != 'MANUAL_STOP', it is rejected and forwarded to adaptive pressure.
   */
  async triggerProtectiveStop(
    reason: string,
    details: any,
    classification: IncidentClassification = 'MANUAL_STOP'
  ): Promise<void> {
    if (classification !== 'MANUAL_STOP') {
      this.logger.warn(
        `🛡️ [AUTOMATIC_STOP_BLOCKED] Automatic stop rejected by Always-On design: "${reason}". Forwarding pressure to Adaptive Capacity Controller instead.`
      );
      this.updatePressureState(reason, classification, details);
      return;
    }

    const nowIso = new Date().toISOString();
    const payload: ProtectiveStopInfo = {
      active: true,
      reason,
      classification: 'MANUAL_STOP',
      details,
      triggered_at: nowIso,
    };

    this.cachedInfo = payload;
    this.lastFetchMs = Date.now();

    this.logger.error(
      `🚨 [MANUAL_STOP TRIGGERED] Staff requested emergency stop: ${reason}. Halting new claims immediately.`,
      { reason, details, triggered_at: nowIso }
    );

    try {
      const pool = this.getPool();
      if (pool && typeof pool.query === 'function') {
        await pool.query(
          "INSERT INTO settings (key, value) VALUES ('importer_protective_stop', $1) ON CONFLICT (key) DO UPDATE SET value = $1",
          [JSON.stringify(payload)]
        );
      }
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
   * Resumes normal operation after manual stop.
   */
  async resumeProtectiveStop(resumedBy = 'manual_staff'): Promise<void> {
    const nowIso = new Date().toISOString();
    const payload: ProtectiveStopInfo = {
      active: false,
      reason: null,
      classification: null,
      resumed_at: nowIso,
      resumed_by: resumedBy,
    };

    this.cachedInfo = payload;
    this.lastFetchMs = Date.now();

    this.logger.info(`[PROTECTIVE_STOP RESUMED] Importer resumed by ${resumedBy}.`, { resumed_at: nowIso });

    try {
      const pool = this.getPool();
      if (pool && typeof pool.query === 'function') {
        await pool.query(
          "INSERT INTO settings (key, value) VALUES ('importer_protective_stop', $1) ON CONFLICT (key) DO UPDATE SET value = $1",
          [JSON.stringify(payload)]
        );
      }
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
   * Returns the current computed pressure snapshot for AdaptiveAutotuner.
   */
  getPressureSnapshot(): PressureSnapshot {
    return this.latestSnapshot;
  }

  /**
   * Background sentinel monitoring loop.
   * Periodically measures site latency and system metrics to update PressureSnapshot.
   */
  startWatchdogLoop(): void {
    if (this.isRunning) return;
    this.isRunning = true;
    this.stopSignal = false;

    void (async () => {
      this.logger.info('Adaptive Pressure Monitor loop started', {
        siteUrl: this.siteUrl || '(not configured)',
      });

      // Clear legacy automatic stop on startup
      await this.clearLegacyProtectiveStopOnStartup();

      // Hydrate auto emergency pause state on startup
      await this.hydrateAutoEmergencyPauseOnStartup();

      // Grace period (10s)
      await new Promise((r) => setTimeout(r, 10_000));

      while (!this.stopSignal) {
        try {
          await this.evaluatePreSlaGuardRails();
        } catch (err: any) {
          this.logger.warn('Error during Adaptive Pressure Monitor cycle', { error: err?.message });
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
   * Resolves a valid published chapter ID dynamically to probe the reader.
   * Avoids querying on dead hardcoded chapters.
   */
  private async getValidReaderChapterId(): Promise<string | null> {
    const now = Date.now();
    if (this.cachedReaderChapterId && now - this.cachedReaderChapterAt < 5 * 60 * 1000) {
      return this.cachedReaderChapterId;
    }

    try {
      const pool = this.getPool();
      if (pool && typeof pool.query === 'function') {
        const res = await pool.query(`
          SELECT c.id
          FROM chapters c
          JOIN works w ON c.work_id = w.id
          WHERE c.published_at IS NOT NULL
            -- Probe an established public reader route. Freshly committed
            -- chapters can briefly race edge/data propagation and are not a
            -- representative health signal for an interactive reader.
            AND c.published_at < clock_timestamp() - INTERVAL '5 minutes'
            AND w.published IS TRUE
            AND EXISTS (
              SELECT 1 FROM pages p
              WHERE p.chapter_id = c.id
            )
          ORDER BY c.published_at DESC
          LIMIT 1;
        `);
        if (res.rows.length > 0 && res.rows[0].id) {
          this.cachedReaderChapterId = res.rows[0].id;
          this.cachedReaderChapterAt = now;
          return this.cachedReaderChapterId;
        }
      }
    } catch {}

    try {
      if (this.supabase) {
        const { data, error } = await this.supabase
          .from('chapters')
          .select('id')
          .not('published_at', 'is', null)
          .order('published_at', { ascending: false })
          .limit(1)
          .maybeSingle();
        if (!error && data?.id) {
          this.cachedReaderChapterId = data.id;
          this.cachedReaderChapterAt = now;
          return this.cachedReaderChapterId;
        }
      }
    } catch {}

    return this.cachedReaderChapterId;
  }

  /**
   * Evaluates all Pre-SLA guard rails and updates PressureSnapshot.
   * Does NOT trigger global stops.
   */
  async evaluatePreSlaGuardRails(): Promise<void> {
    const mem = diagnostics.getMemorySnapshot();
    const lagMetrics = (diagnostics as any).lagMonitor?.getMetrics?.() || { avgLagMs: 0 };

    let totalConns = 0;
    let activeConns = 0;
    let poolWait = 0;
    try {
      const pool = this.getPool();
      if (pool && typeof pool.query === 'function') {
        poolWait = Math.max(0, Number(pool.waitingCount || 0));
        const cRes = await pool.query(`
          SELECT count(*) as total,
                 count(*) FILTER (WHERE state = 'active') as active
          FROM pg_stat_activity
          WHERE datname = current_database()
        `);
        totalConns = parseInt(cRes.rows[0]?.total || '0', 10);
        activeConns = parseInt(cRes.rows[0]?.active || '0', 10);
      }
    } catch {
      try {
        const { data: connData, error: connErr } = await this.supabase.rpc('importer_active_connections_count');
        if (!connErr && typeof connData === 'number') {
          totalConns = connData;
          activeConns = connData;
        }
      } catch {}
    }

    // Probes site routes if siteUrl is configured
    if (this.siteUrl) {
      this.route4xxLabels.clear();
      this.probeRouteMismatch = false;
      await this.probeSiteLatency('home', `${this.siteUrl}/`);
      
      const chapterId = await this.getValidReaderChapterId();
      await new Promise((r) => setTimeout(r, 1000));
      if (chapterId) {
        await this.probeSiteLatency('reader', `${this.siteUrl}/ler/${chapterId}`);
      } else {
        await this.probeSiteLatency('reader', `${this.siteUrl}/api/health`);
      }

      // Do not let a Discloud/edge-specific 404 on an otherwise healthy
      // public route permanently pin the importer below its safe baseline.
      // An independent lightweight health success is sufficient evidence. A
      // uniform 404 across Home, Reader and health is also a monitor-routing
      // signature: reducing importer load cannot repair an edge route that
      // rejects every request from this one egress. Any 5xx, timeout, mixed
      // failure, or failed corroboration remains real capacity pressure.
      if (this.route4xxLabels.size > 0) {
        await this.probeSiteLatency('health', `${this.siteUrl}/api/health`);
        const healthStatus = this.lastProbeStatus.health || 0;
        const primaryStatuses = [...this.route4xxLabels].map((label) => this.lastProbeStatus[label] || 0);
        const uniform404 = healthStatus === 404 && primaryStatuses.length > 0 && primaryStatuses.every((status) => status === 404);
        if ((healthStatus >= 200 && healthStatus < 400) || uniform404) {
          this.consecutiveProbeFailures = 0;
          this.probeRouteMismatch = true;
          this.probeRouteMismatchCycles++;
          const confirmation = uniform404 ? 'all monitor routes returned 404' : `/api/health is ${healthStatus}`;
          this.logger.warn(
            `[Site Probe Mismatch] ${[...this.route4xxLabels].map((label) => label.toUpperCase()).join(', ')} returned 4xx and ${confirmation}; keeping site capacity under independent health confirmation (cycle ${this.probeRouteMismatchCycles})`
          );
        } else {
          this.probeRouteMismatchCycles = 0;
        }
      } else {
        this.probeRouteMismatchCycles = 0;
      }
    }

    // A 5xx is a real pressure signal while it is current, but the counter
    // must not survive forever when the probe path has since gone quiet (for
    // example after a transient edge/egress failure).  Keep the last event in
    // `lastHttp5xx` for diagnosis, while expiring only the decision-driving
    // consecutive count after the same bounded observation window used by the
    // latency samples.  A new 5xx in the current cycle refreshes the timestamp
    // and remains immediately effective.
    const nowAfterProbes = Date.now();
    if (
      this.consecutive5xxCount > 0 &&
      this.last5xxTimestamp !== null &&
      nowAfterProbes - this.last5xxTimestamp > this.LATENCY_SAMPLE_WINDOW_MS
    ) {
      this.logger.info(
        `[Site Probe Recovery] Expiring ${this.consecutive5xxCount} stale HTTP 5xx signal(s) after ${Math.round((nowAfterProbes - this.last5xxTimestamp) / 1000)}s without a new 5xx`
      );
      this.consecutive5xxCount = 0;
    }

    // Compute rolling percentiles with time eviction (75s window)
    const now = nowAfterProbes;
    this.homeSamples = this.homeSamples.filter((s) => now - s.timestamp <= this.LATENCY_SAMPLE_WINDOW_MS);
    this.readerSamples = this.readerSamples.filter((s) => now - s.timestamp <= this.LATENCY_SAMPLE_WINDOW_MS);

    const homeP50 = this.getPercentile(this.homeSamples, 0.50);
    const homeP95 = this.getPercentile(this.homeSamples, 0.95);
    const readerP50 = this.getPercentile(this.readerSamples, 0.50);
    const readerP95 = this.getPercentile(this.readerSamples, 0.95);

    const maxP95 = Math.max(homeP95, readerP95);

    // 1. CATASTROPHIC SITE DEGRADATION CHECK (Section 4)
    // Criteria: Home OR Reader P95 >= 10,000ms sustained for >= 3 consecutive cycles,
    // OR severe combo: Site P95 >= 8,000ms sustained + >= 3 consecutive 5xx errors.
    const isCatastrophicSignal =
      homeP95 >= 10_000 ||
      readerP95 >= 10_000 ||
      (maxP95 >= 8_000 && this.consecutive5xxCount >= 3);

    if (isCatastrophicSignal) {
      this.catastrophicCyclesCount++;
      if (this.catastrophicCyclesCount >= 3 && !this.autoEmergencyPause.active) {
        this.autoEmergencyPause = {
          active: true,
          pausedAt: new Date().toISOString(),
          reason: `Catastrophic site latency breach sustained for ${this.catastrophicCyclesCount} cycles (Home p95: ${homeP95}ms, Reader p95: ${readerP95}ms, 5xx: ${this.consecutive5xxCount})`,
          siteP95: maxP95,
          consecutiveCatastrophicCycles: this.catastrophicCyclesCount,
          nextRecheckAt: new Date(Date.now() + 15_000).toISOString(),
          resumedAt: null,
          healthyCyclesCount: 0,
        };
        this.logger.error(
          `🚨 [AUTO_EMERGENCY_PAUSE] Catastrophic user-facing site degradation sustained for 3 cycles (Home: ${homeP95}ms, Reader: ${readerP95}ms, 5xx: ${this.consecutive5xxCount}). Halting new chapter claims while preserving engine, watchdog and telemetry.`
        );
        void this.persistAutoEmergencyPause();
      }
    } else {
      this.catastrophicCyclesCount = 0;
    }

    // 2. AUTO-RESUME CHECK (Section 6)
    // When emergency pause is active, auto-resume if site returns to healthy (< 2000ms and 0 5xx) for sustained ~1 minute (4 cycles * 15s)
    if (this.autoEmergencyPause.active) {
      const isReaderHealthy = this.readerSamples.length === 0 || readerP95 < 1500;
      if (homeP95 < 2000 && isReaderHealthy && this.consecutive5xxCount === 0) {
        this.healthyCyclesCount++;
        if (this.healthyCyclesCount >= 4) {
          this.autoEmergencyPause.active = false;
          this.autoEmergencyPause.resumedAt = new Date().toISOString();
          this.autoEmergencyPause.reason = `Auto-resumed after site stabilization (Home: ${homeP95}ms, Reader: ${readerP95}ms sustained for 60s)`;
          this.healthyCyclesCount = 0;
          this.logger.info(
            `✅ [AUTO-RESUME] Site recovered to healthy state (Home: ${homeP95}ms, Reader: ${readerP95}ms). Auto-resuming claims.`
          );
          void this.persistAutoEmergencyPause();
          if (this.onAutoResume) {
            try { this.onAutoResume(); } catch {}
          }
        }
      } else {
        this.healthyCyclesCount = 0;
      }
    }

    // 3. SITE LATENCY TIERS (Section 19: GREEN, YELLOW, ORANGE, RED)
    let siteHealth: SiteHealthState = 'GREEN';
    let sitePressure = 0;
    let pressureReason = 'Site and infrastructure healthy';

    const hasReaderSamples = this.readerSamples.length > 0;
    const isReaderDegraded = hasReaderSamples ? readerP95 >= 2500 : false;
    const isReaderSevere = hasReaderSamples ? readerP95 >= 5000 : false;
    // Preserve an observed 5xx for diagnosis, but do not let one old failure
    // permanently throttle the importer when this egress has subsequently
    // produced a sustained, uniform 404 monitor-route signature. In that
    // state the monitor cannot observe the real route; a current 5xx (or a
    // sustained series of them) remains capacity pressure.
    const staleSingle5xxMaskedByConfirmedRouteMismatch =
      this.probeRouteMismatch &&
      this.probeRouteMismatchCycles >= 3 &&
      this.consecutive5xxCount === 1;
    const effectiveConsecutive5xx = staleSingle5xxMaskedByConfirmedRouteMismatch
      ? 0
      : this.consecutive5xxCount;
    const effectiveProbeFailures = staleSingle5xxMaskedByConfirmedRouteMismatch
      ? 0
      : this.consecutiveProbeFailures;

    if (this.autoEmergencyPause.active) {
      siteHealth = 'RED';
      sitePressure = 80;
      pressureReason = `AUTO_EMERGENCY_PAUSE: ${this.autoEmergencyPause.reason}`;
    } else if (effectiveConsecutive5xx >= 3 || homeP95 >= 10000 || (homeP95 >= 8000 && (!hasReaderSamples || isReaderSevere))) {
      siteHealth = 'RED';
      sitePressure = 60;
      pressureReason = effectiveConsecutive5xx >= 3
        ? `Sustained HTTP 5xx errors (${effectiveConsecutive5xx} consecutive)`
        : `Severe site latency breach (Home p95: ${homeP95}ms, Reader p95: ${readerP95}ms)`;
    } else if (effectiveConsecutive5xx >= 1 || effectiveProbeFailures >= 1 || (homeP95 >= 5000 && isReaderDegraded) || (homeP95 >= 8000 && !hasReaderSamples)) {
      siteHealth = 'ORANGE';
      sitePressure = 35;
      pressureReason = effectiveConsecutive5xx >= 1
        ? `HTTP 5xx error observed (${effectiveConsecutive5xx})`
        : effectiveProbeFailures >= 1
        ? `User-facing route/probe failure observed (${effectiveProbeFailures})`
        : `Confirmed site degradation (Home p95: ${homeP95}ms, Reader p95: ${readerP95}ms)`;
    } else if (effectiveConsecutive5xx === 0 && (homeP95 >= 1500 || readerP95 >= 1200)) {
      siteHealth = 'YELLOW';
      sitePressure = 15;
      pressureReason = `Mild site latency increase (Home p95: ${homeP95}ms, Reader p95: ${readerP95}ms)`;
    } else if (this.probeRouteMismatch) {
      const healthStatus = this.lastProbeStatus.health || 0;
      const primaryStatuses = [...this.route4xxLabels].map((label) => this.lastProbeStatus[label] || 0);
      const uniform404 = healthStatus === 404 && primaryStatuses.length > 0 && primaryStatuses.every((status) => status === 404);
      pressureReason = uniform404
        ? `Monitor route mismatch confirmed (uniform 404 from this egress)`
        : `Monitor route mismatch confirmed (${[...this.route4xxLabels].map((label) => label.toUpperCase()).join(', ')} 4xx; /api/health healthy)`;
    }

    // DB pressure score
    let dbPressure = 0;
    if (totalConns >= this.thresholds.ysqlConnTripwire || activeConns >= 6) {
      this.consecutiveMildDbPressureCycles = 0;
      dbPressure = 30;
      pressureReason = `Elevated YSQL load: ${totalConns}/13 total (${activeConns} active)`;
    } else if (totalConns >= 10 || activeConns >= 4) {
      this.consecutiveMildDbPressureCycles++;
      if (poolWait > 0 || this.consecutiveMildDbPressureCycles >= 3) {
        dbPressure = 15;
        pressureReason = poolWait > 0
          ? `YSQL pool wait: ${poolWait} waiter(s) with ${totalConns}/13 total (${activeConns} active)`
          : `Sustained YSQL activity for ${this.consecutiveMildDbPressureCycles} cycles: ${totalConns}/13 total (${activeConns} active)`;
      }
    } else {
      this.consecutiveMildDbPressureCycles = 0;
    }

    // Memory pressure score
    let memoryPressure = 0;
    if (mem.rssMb >= this.thresholds.maxRssMb) {
      memoryPressure = 35;
      pressureReason = `High memory pressure: ${mem.rssMb}MB >= limit ${this.thresholds.maxRssMb}MB`;
    } else if (mem.rssMb >= 380) {
      memoryPressure = 15;
    }

    // Event loop lag pressure score
    let eventLoopPressure = 0;
    if (lagMetrics.avgLagMs >= this.thresholds.maxEventLoopLagMs) {
      eventLoopPressure = 25;
      pressureReason = `High event loop lag: ${lagMetrics.avgLagMs}ms >= limit ${this.thresholds.maxEventLoopLagMs}ms`;
    } else if (lagMetrics.avgLagMs >= 150) {
      eventLoopPressure = 10;
    }

    const totalPressureScore = Math.min(100, sitePressure + dbPressure + memoryPressure + eventLoopPressure);

    this.latestSnapshot = {
      timestamp: Date.now(),
      siteHealth,
      homeP50,
      homeP95,
      readerP50,
      readerP95,
      consecutive5xx: this.consecutive5xxCount,
      lastHttp5xx: this.last5xxTimestamp,
      ysqlTotal: totalConns,
      ysqlActive: activeConns,
      poolWait,
      rssMb: mem.rssMb,
      heapUsedMb: mem.heapUsedMb,
      eventLoopLagMs: lagMetrics.avgLagMs,
      pressureScore: totalPressureScore,
      pressureBreakdown: {
        sitePressure,
        dbPressure,
        memoryPressure,
        eventLoopPressure,
        storagePressure: 0,
        sourcePressure: 0,
        publicationPressure: 0,
      },
      pressureReason,
      probeRouteMismatch: this.probeRouteMismatch,
      probeRouteMismatchCycles: this.probeRouteMismatchCycles,
    };
  }

  private getPercentile(samples: LatencySample[], p: number): number {
    const now = Date.now();
    const valid = samples.filter((s) => now - s.timestamp <= this.LATENCY_SAMPLE_WINDOW_MS);
    if (valid.length === 0) return 0;
    const sorted = valid.map((s) => s.ttfbMs).sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
    return sorted[idx];
  }

  /**
   * Probes site route latency using keep-alive connection.
   */
  private async probeSiteLatency(
    label: SiteProbeLabel,
    url: string,
    isRetry = false
  ): Promise<void> {
    return new Promise<void>((resolve) => {
      const t0 = performance.now();
      const isHttps = url.startsWith('https:');
      const mod = isHttps ? https : http;

      const req = mod.get(
        url,
        {
          agent: isHttps ? (label === 'reader' ? this.readerAgent : this.homeAgent) : this.httpAgent,
          headers: SITE_PROBE_HEADERS,
          timeout: 12000,
        },
        (res: any) => {
          let resolved = false;
          const finish = () => {
            if (resolved) return;
            resolved = true;
            try {
              res.resume();
            } catch {}
            const ttfbMs = Math.round(performance.now() - t0);
            this.recordProbeResult(label, ttfbMs, res.statusCode || 200);
            resolve();
          };

          res.once('data', () => { finish(); });
          res.on('end', () => { finish(); });
        }
      );

      req.on('error', (err: any) => {
        if (!isRetry && (err?.message?.includes('socket hang up') || err?.code === 'ECONNRESET')) {
          return this.probeSiteLatency(label, url, true).then(resolve);
        }
        this.recordProbeFailure(label, err);
        resolve();
      });

      req.on('timeout', () => {
        req.destroy();
        this.recordProbeFailure(label, new Error('Request timed out after 4000ms'));
        resolve();
      });
    });
  }

  public recordProbeResult(
    label: SiteProbeLabel,
    ttfbMs: number,
    statusCode: number = 200,
    timestamp: number = Date.now()
  ): void {
    this.lastProbeStatus[label] = statusCode;
    // A Reader 404 can occur when a chapter changes between the cached-ID
    // lookup and the probe. Refresh that target next cycle; it is neither a
    // healthy response nor a global site incident.
    if (label === 'reader' && statusCode === 404) {
      this.cachedReaderChapterId = null;
      this.route4xxLabels.add(label);
      this.logger.warn('[Site Probe] Reader returned 404 for probed chapter; invalidating cached probe target');
      return;
    }

    if (statusCode >= 500) {
      this.consecutive5xxCount++;
      this.last5xxTimestamp = timestamp;
      this.logger.warn(`[Site Probe 5xx] ${label.toUpperCase()} returned HTTP ${statusCode} (consecutive: ${this.consecutive5xxCount})`);
    } else if (statusCode >= 400) {
      // A Home/work route 4xx is user-facing routing/data failure. It must
      // not erase a preceding 5xx and falsely label the site recovered.
      this.consecutiveProbeFailures++;
      if (label !== 'health') this.route4xxLabels.add(label);
      this.logger.warn(`[Site Probe Route Error] ${label.toUpperCase()} returned HTTP ${statusCode} (consecutive: ${this.consecutiveProbeFailures})`);
      return;
    } else {
      // A lightweight health endpoint can corroborate a route mismatch, but
      // it must not erase a preceding user-facing 5xx. Only a successful
      // public page/Reader probe is evidence that the page path recovered.
      if (label !== 'health' && this.consecutive5xxCount > 0) {
        this.logger.info(`[Site Probe Recovered] ${label.toUpperCase()} returned HTTP ${statusCode} (5xx cleared)`);
      }
      if (label !== 'health') this.consecutive5xxCount = 0;
    }

    const sample: LatencySample = { ttfbMs, timestamp };
    const cutoff = timestamp - this.LATENCY_SAMPLE_WINDOW_MS;

    if (label === 'home') {
      this.homeSamples.push(sample);
      this.homeSamples = this.homeSamples.filter((s) => s.timestamp >= cutoff);
      if (this.homeSamples.length > 20) this.homeSamples.shift();
    } else if (label === 'reader') {
      this.readerSamples.push(sample);
      this.readerSamples = this.readerSamples.filter((s) => s.timestamp >= cutoff);
      if (this.readerSamples.length > 20) this.readerSamples.shift();
    }
    this.consecutiveProbeFailures = 0;
  }

  private recordProbeFailure(label: SiteProbeLabel, err: any): void {
    this.consecutiveProbeFailures++;
    this.logger.warn(`[Site Probe Error] ${label.toUpperCase()} probe error: ${err?.message} (consecutive: ${this.consecutiveProbeFailures})`);
  }

  private updatePressureState(reason: string, classification: IncidentClassification, details: any): void {
    this.latestSnapshot.pressureReason = reason;
  }

  private async persistAutoEmergencyPause(): Promise<void> {
    try {
      const pool = this.getPool();
      if (pool && typeof pool.query === 'function') {
        await pool.query(
          "INSERT INTO settings (key, value) VALUES ('importer_auto_emergency_pause', $1) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
          [JSON.stringify(this.autoEmergencyPause)]
        );
      }
    } catch (err: any) {
      this.logger.warn('Failed to persist importer_auto_emergency_pause', { error: err?.message });
    }
  }

  /**
   * Compatibility method for auto-heal watchdog
   */
  async evaluateAutoResume(): Promise<void> {
    // Under Always-On design, automatic stops are prevented.
    // If a legacy stop remains in the database, clear it immediately.
    await this.clearLegacyProtectiveStopOnStartup();
  }
}

export { ProtectiveSentinel as AdaptivePressureMonitor };
