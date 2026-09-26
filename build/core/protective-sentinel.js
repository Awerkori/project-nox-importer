import http from 'http';
import https from 'https';
import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';
import { getYugabytePool } from '../db/yugabyte-direct.js';
export const DEFAULT_SENTINEL_THRESHOLDS = {
    homeTtfbPreSlaMs: 800,
    readerTtfbPreSlaMs: 600,
    mediaTtfbPreSlaMs: 300,
    ysqlConnTripwire: 12,
    maxRssMb: 440,
    maxEventLoopLagMs: 350,
    maxTelegramFloodWaitSec: 60,
};
export class ProtectiveSentinel {
    supabase;
    thresholds;
    siteUrl;
    logger = new Logger('ProtectiveSentinel');
    cachedInfo = { active: false };
    lastFetchMs = 0;
    cacheTtlMs = 3000; // 3 second cache
    isRunning = false;
    stopSignal = false;
    // Rolling latency windows for p50/p95 (max 20 samples)
    homeSamples = [];
    readerSamples = [];
    consecutive5xxCount = 0;
    last5xxTimestamp = null;
    consecutiveProbeFailures = 0;
    // Cached dynamic chapter ID for Reader probe (refreshed every 5 min)
    cachedReaderChapterId = null;
    cachedReaderChapterAt = 0;
    // Latest computed pressure snapshot
    latestSnapshot = {
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
    };
    homeAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
    readerAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
    httpAgent = new http.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
    catastrophicCyclesCount = 0;
    healthyCyclesCount = 0;
    autoEmergencyPause = {
        active: false,
        pausedAt: null,
        reason: null,
        siteP95: null,
        consecutiveCatastrophicCycles: 0,
        nextRecheckAt: null,
        resumedAt: null,
        healthyCyclesCount: 0,
    };
    onAutoResume;
    constructor(supabase, thresholds = DEFAULT_SENTINEL_THRESHOLDS, siteUrl) {
        this.supabase = supabase;
        this.thresholds = thresholds;
        this.siteUrl = siteUrl;
    }
    setOnAutoResume(fn) {
        this.onAutoResume = fn;
    }
    isEmergencyPaused() {
        return this.autoEmergencyPause.active;
    }
    getEmergencyPauseState() {
        return { ...this.autoEmergencyPause };
    }
    /**
     * Checks whether a MANUAL staff protective stop is active.
     * STRICT INVARIANT: Automatic performance stops CANNOT make this return true.
     * If a legacy automatic stop exists in DB, it is auto-cleared on discovery.
     */
    async isProtectiveStopActive() {
        const info = await this.getProtectiveStopInfo();
        if (!info.active)
            return false;
        const isManual = info.classification === 'MANUAL_STOP' ||
            info.reason?.toLowerCase().includes('manual') ||
            info.reason?.toLowerCase().includes('staff');
        if (isManual) {
            return true;
        }
        // Auto-clear legacy automatic performance stop
        this.logger.warn(`[ADAPTIVE_MIGRATION] Ignoring and clearing legacy automatic performance stop: "${info.reason}" (classification: ${info.classification})`);
        void this.resumeProtectiveStop('ADAPTIVE_MIGRATION');
        return false;
    }
    /**
     * Retrieves full protective stop details from the settings table.
     */
    async getProtectiveStopInfo(forceFresh = false) {
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
            catch { }
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
                }
                catch {
                    this.cachedInfo = { active: false };
                }
            }
            else {
                this.cachedInfo = { active: false };
            }
            this.lastFetchMs = now;
        }
        catch (err) {
            this.logger.warn('Failed to fetch importer_protective_stop setting', { error: err?.message });
        }
        return this.cachedInfo;
    }
    /**
     * On startup, auto-clears any legacy automatic protective stop if active.
     */
    async clearLegacyProtectiveStopOnStartup() {
        try {
            const info = await this.getProtectiveStopInfo(true);
            if (info.active) {
                const isManual = info.classification === 'MANUAL_STOP' ||
                    info.reason?.toLowerCase().includes('manual') ||
                    info.reason?.toLowerCase().includes('staff');
                if (!isManual) {
                    this.logger.warn(`[ADAPTIVE_MIGRATION] Cleared legacy automatic protective stop on boot (was: "${info.reason}", classification: ${info.classification})`);
                    await this.resumeProtectiveStop('ADAPTIVE_MIGRATION');
                }
            }
        }
        catch (err) {
            this.logger.warn('Failed checking legacy protective stop on boot', { error: err?.message });
        }
    }
    /**
     * Triggers a MANUAL staff protective stop.
     * AUTOMATIC PERFORMANCE STOPS ARE STRICTLY FORBIDDEN.
     * If called with classification != 'MANUAL_STOP', it is rejected and forwarded to adaptive pressure.
     */
    async triggerProtectiveStop(reason, details, classification = 'MANUAL_STOP') {
        if (classification !== 'MANUAL_STOP') {
            this.logger.warn(`🛡️ [AUTOMATIC_STOP_BLOCKED] Automatic stop rejected by Always-On design: "${reason}". Forwarding pressure to Adaptive Capacity Controller instead.`);
            this.updatePressureState(reason, classification, details);
            return;
        }
        const nowIso = new Date().toISOString();
        const payload = {
            active: true,
            reason,
            classification: 'MANUAL_STOP',
            details,
            triggered_at: nowIso,
        };
        this.cachedInfo = payload;
        this.lastFetchMs = Date.now();
        this.logger.error(`🚨 [MANUAL_STOP TRIGGERED] Staff requested emergency stop: ${reason}. Halting new claims immediately.`, { reason, details, triggered_at: nowIso });
        try {
            const pool = getYugabytePool();
            await pool.query("INSERT INTO settings (key, value) VALUES ('importer_protective_stop', $1) ON CONFLICT (key) DO UPDATE SET value = $1", [JSON.stringify(payload)]);
        }
        catch {
            try {
                await this.supabase.from('settings').upsert({
                    key: 'importer_protective_stop',
                    value: JSON.stringify(payload),
                });
            }
            catch (dbErr) {
                this.logger.error('Failed to persist importer_protective_stop to database', { error: dbErr?.message });
            }
        }
    }
    /**
     * Resumes normal operation after manual stop.
     */
    async resumeProtectiveStop(resumedBy = 'manual_staff') {
        const nowIso = new Date().toISOString();
        const payload = {
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
            const pool = getYugabytePool();
            await pool.query("INSERT INTO settings (key, value) VALUES ('importer_protective_stop', $1) ON CONFLICT (key) DO UPDATE SET value = $1", [JSON.stringify(payload)]);
        }
        catch {
            try {
                await this.supabase.from('settings').upsert({
                    key: 'importer_protective_stop',
                    value: JSON.stringify(payload),
                });
            }
            catch (dbErr) {
                this.logger.error('Failed to clear importer_protective_stop in database', { error: dbErr?.message });
            }
        }
    }
    /**
     * Returns the current computed pressure snapshot for AdaptiveAutotuner.
     */
    getPressureSnapshot() {
        return this.latestSnapshot;
    }
    /**
     * Background sentinel monitoring loop.
     * Periodically measures site latency and system metrics to update PressureSnapshot.
     */
    startWatchdogLoop() {
        if (this.isRunning)
            return;
        this.isRunning = true;
        this.stopSignal = false;
        void (async () => {
            this.logger.info('Adaptive Pressure Monitor loop started', {
                siteUrl: this.siteUrl || '(not configured)',
            });
            // Clear legacy automatic stop on startup
            await this.clearLegacyProtectiveStopOnStartup();
            // Grace period (10s)
            await new Promise((r) => setTimeout(r, 10_000));
            while (!this.stopSignal) {
                try {
                    await this.evaluatePreSlaGuardRails();
                }
                catch (err) {
                    this.logger.warn('Error during Adaptive Pressure Monitor cycle', { error: err?.message });
                }
                await new Promise((r) => setTimeout(r, 15_000));
            }
            this.isRunning = false;
        })();
    }
    stop() {
        this.stopSignal = true;
    }
    /**
     * Resolves a valid published chapter ID dynamically to probe the reader.
     * Avoids querying on dead hardcoded chapters.
     */
    async getValidReaderChapterId() {
        const now = Date.now();
        if (this.cachedReaderChapterId && now - this.cachedReaderChapterAt < 5 * 60 * 1000) {
            return this.cachedReaderChapterId;
        }
        try {
            const pool = getYugabytePool();
            const res = await pool.query("SELECT id FROM chapters WHERE published_at IS NOT NULL ORDER BY published_at DESC LIMIT 1");
            if (res.rows.length > 0 && res.rows[0].id) {
                this.cachedReaderChapterId = res.rows[0].id;
                this.cachedReaderChapterAt = now;
                return this.cachedReaderChapterId;
            }
        }
        catch { }
        return this.cachedReaderChapterId;
    }
    /**
     * Evaluates all Pre-SLA guard rails and updates PressureSnapshot.
     * Does NOT trigger global stops.
     */
    async evaluatePreSlaGuardRails() {
        const mem = diagnostics.getMemorySnapshot();
        const lagMetrics = diagnostics.lagMonitor?.getMetrics?.() || { avgLagMs: 0 };
        let totalConns = 0;
        let activeConns = 0;
        try {
            const pool = getYugabytePool();
            const cRes = await pool.query(`
        SELECT count(*) as total,
               count(*) FILTER (WHERE state = 'active') as active
        FROM pg_stat_activity
        WHERE datname = current_database()
      `);
            totalConns = parseInt(cRes.rows[0]?.total || '0', 10);
            activeConns = parseInt(cRes.rows[0]?.active || '0', 10);
        }
        catch {
            try {
                const { data: connData, error: connErr } = await this.supabase.rpc('importer_active_connections_count');
                if (!connErr && typeof connData === 'number') {
                    totalConns = connData;
                    activeConns = connData;
                }
            }
            catch { }
        }
        // Probes site routes if siteUrl is configured
        if (this.siteUrl) {
            await this.probeSiteLatency('home', `${this.siteUrl}/`);
            const chapterId = await this.getValidReaderChapterId();
            if (chapterId) {
                await new Promise((r) => setTimeout(r, 1000));
                await this.probeSiteLatency('reader', `${this.siteUrl}/ler/${chapterId}`);
            }
        }
        // Compute rolling percentiles
        const homeP50 = this.getPercentile(this.homeSamples, 0.50);
        const homeP95 = this.getPercentile(this.homeSamples, 0.95);
        const readerP50 = this.getPercentile(this.readerSamples, 0.50);
        const readerP95 = this.getPercentile(this.readerSamples, 0.95);
        const maxP95 = Math.max(homeP95, readerP95);
        // 1. CATASTROPHIC SITE DEGRADATION CHECK (Section 4)
        // Criteria: Home OR Reader P95 >= 10,000ms sustained for >= 3 consecutive cycles,
        // OR severe combo: Site P95 >= 8,000ms sustained + >= 3 consecutive 5xx errors.
        const isCatastrophicSignal = homeP95 >= 10_000 ||
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
                this.logger.error(`🚨 [AUTO_EMERGENCY_PAUSE] Catastrophic user-facing site degradation sustained for 3 cycles (Home: ${homeP95}ms, Reader: ${readerP95}ms, 5xx: ${this.consecutive5xxCount}). Halting new chapter claims while preserving engine, watchdog and telemetry.`);
                void this.persistAutoEmergencyPause();
            }
        }
        else {
            this.catastrophicCyclesCount = 0;
        }
        // 2. AUTO-RESUME CHECK (Section 6)
        // When emergency pause is active, auto-resume if site returns to healthy (< 1500ms and 0 5xx) for sustained ~2 minutes (8 cycles * 15s)
        if (this.autoEmergencyPause.active) {
            if (homeP95 < 1500 && readerP95 < 1200 && this.consecutive5xxCount === 0) {
                this.healthyCyclesCount++;
                if (this.healthyCyclesCount >= 8) {
                    this.autoEmergencyPause.active = false;
                    this.autoEmergencyPause.resumedAt = new Date().toISOString();
                    this.autoEmergencyPause.reason = `Auto-resumed after site stabilization (Home: ${homeP95}ms, Reader: ${readerP95}ms sustained for 2m)`;
                    this.healthyCyclesCount = 0;
                    this.logger.info(`✅ [AUTO-RESUME] Site recovered to healthy state (Home: ${homeP95}ms, Reader: ${readerP95}ms). Auto-resuming claims at capacity 1.`);
                    void this.persistAutoEmergencyPause();
                    if (this.onAutoResume) {
                        try {
                            this.onAutoResume();
                        }
                        catch { }
                    }
                }
            }
            else {
                this.healthyCyclesCount = 0;
            }
        }
        // 3. SITE LATENCY TIERS (Section 19: GREEN, YELLOW, ORANGE, RED)
        let siteHealth = 'GREEN';
        let sitePressure = 0;
        let pressureReason = 'Site and infrastructure healthy';
        if (this.autoEmergencyPause.active) {
            siteHealth = 'RED';
            sitePressure = 80;
            pressureReason = `AUTO_EMERGENCY_PAUSE: ${this.autoEmergencyPause.reason}`;
        }
        else if (this.consecutive5xxCount >= 3 || homeP95 >= 3500 || readerP95 >= 3000) {
            siteHealth = 'RED';
            sitePressure = 60;
            pressureReason = this.consecutive5xxCount >= 3
                ? `Sustained HTTP 5xx errors (${this.consecutive5xxCount} consecutive)`
                : `Severe site latency breach (Home p95: ${homeP95}ms, Reader p95: ${readerP95}ms)`;
        }
        else if (this.consecutive5xxCount >= 1 || homeP95 >= 1500 || readerP95 >= 1200) {
            siteHealth = 'ORANGE';
            sitePressure = 35;
            pressureReason = `Confirmed site degradation (Home p95: ${homeP95}ms, Reader p95: ${readerP95}ms)`;
        }
        else if (homeP95 >= 800 || readerP95 >= 700) {
            siteHealth = 'YELLOW';
            sitePressure = 15;
            pressureReason = `Mild site latency increase (Home p95: ${homeP95}ms, Reader p95: ${readerP95}ms)`;
        }
        // DB pressure score
        let dbPressure = 0;
        if (totalConns >= this.thresholds.ysqlConnTripwire || activeConns >= 6) {
            dbPressure = 30;
            pressureReason = `Elevated YSQL load: ${totalConns}/13 total (${activeConns} active)`;
        }
        else if (totalConns >= 10 || activeConns >= 4) {
            dbPressure = 15;
        }
        // Memory pressure score
        let memoryPressure = 0;
        if (mem.rssMb >= this.thresholds.maxRssMb) {
            memoryPressure = 35;
            pressureReason = `High memory pressure: ${mem.rssMb}MB >= limit ${this.thresholds.maxRssMb}MB`;
        }
        else if (mem.rssMb >= 380) {
            memoryPressure = 15;
        }
        // Event loop lag pressure score
        let eventLoopPressure = 0;
        if (lagMetrics.avgLagMs >= this.thresholds.maxEventLoopLagMs) {
            eventLoopPressure = 25;
            pressureReason = `High event loop lag: ${lagMetrics.avgLagMs}ms >= limit ${this.thresholds.maxEventLoopLagMs}ms`;
        }
        else if (lagMetrics.avgLagMs >= 150) {
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
            poolWait: 0,
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
        };
    }
    getPercentile(samples, p) {
        if (samples.length === 0)
            return 0;
        const sorted = [...samples].sort((a, b) => a - b);
        const idx = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
        return sorted[idx];
    }
    /**
     * Probes site route latency using keep-alive connection.
     */
    async probeSiteLatency(label, url, isRetry = false) {
        return new Promise((resolve) => {
            const t0 = performance.now();
            const isHttps = url.startsWith('https:');
            const mod = isHttps ? https : http;
            const req = mod.get(url, {
                agent: isHttps ? (label === 'reader' ? this.readerAgent : this.homeAgent) : this.httpAgent,
                headers: {
                    'User-Agent': 'Project-Nox-AdaptiveMonitor/2.0',
                },
                timeout: 12000,
            }, (res) => {
                let resolved = false;
                const finish = () => {
                    if (resolved)
                        return;
                    resolved = true;
                    try {
                        res.resume();
                    }
                    catch { }
                    const ttfbMs = Math.round(performance.now() - t0);
                    this.recordProbeResult(label, ttfbMs, res.statusCode || 200);
                    resolve();
                };
                res.once('data', () => { finish(); });
                res.on('end', () => { finish(); });
            });
            req.on('error', (err) => {
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
    recordProbeResult(label, ttfbMs, statusCode = 200) {
        if (statusCode >= 500) {
            this.consecutive5xxCount++;
            this.last5xxTimestamp = Date.now();
            this.logger.warn(`[Site Probe 5xx] ${label.toUpperCase()} returned HTTP ${statusCode} (consecutive: ${this.consecutive5xxCount})`);
        }
        else {
            if (this.consecutive5xxCount > 0) {
                this.logger.info(`[Site Probe Recovered] ${label.toUpperCase()} returned HTTP ${statusCode} (5xx cleared)`);
            }
            this.consecutive5xxCount = 0;
        }
        if (label === 'home') {
            this.homeSamples.push(ttfbMs);
            if (this.homeSamples.length > 20)
                this.homeSamples.shift();
        }
        else {
            this.readerSamples.push(ttfbMs);
            if (this.readerSamples.length > 20)
                this.readerSamples.shift();
        }
        this.consecutiveProbeFailures = 0;
    }
    recordProbeFailure(label, err) {
        this.consecutiveProbeFailures++;
        this.logger.warn(`[Site Probe Error] ${label.toUpperCase()} probe error: ${err?.message} (consecutive: ${this.consecutiveProbeFailures})`);
    }
    updatePressureState(reason, classification, details) {
        this.latestSnapshot.pressureReason = reason;
    }
    async persistAutoEmergencyPause() {
        try {
            const pool = getYugabytePool();
            await pool.query("INSERT INTO settings (key, value) VALUES ('importer_auto_emergency_pause', $1) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", [JSON.stringify(this.autoEmergencyPause)]);
        }
        catch (err) {
            this.logger.warn('Failed to persist importer_auto_emergency_pause', { error: err?.message });
        }
    }
    /**
     * Compatibility method for auto-heal watchdog
     */
    async evaluateAutoResume() {
        // Under Always-On design, automatic stops are prevented.
        // If a legacy stop remains in the database, clear it immediately.
        await this.clearLegacyProtectiveStopOnStartup();
    }
}
export { ProtectiveSentinel as AdaptivePressureMonitor };
