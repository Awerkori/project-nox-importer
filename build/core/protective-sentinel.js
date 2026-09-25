import http from 'http';
import https from 'https';
import { Logger } from './logger.js';
import { diagnostics } from './diagnostics.js';
import { getYugabytePool } from '../db/yugabyte-direct.js';
export const DEFAULT_SENTINEL_THRESHOLDS = {
    homeTtfbPreSlaMs: 250,
    readerTtfbPreSlaMs: 210,
    mediaTtfbPreSlaMs: 120,
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
    // Isolated counters to strictly separate transient edge jitter from sustained outages
    consecutive5xxErrors = new Map();
    consecutiveLatencyViolations = new Map();
    consecutiveProbeErrors = new Map();
    homeAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
    readerAgent = new https.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
    httpAgent = new http.Agent({ keepAlive: true, maxSockets: 5, keepAliveMsecs: 60000 });
    constructor(supabase, thresholds = DEFAULT_SENTINEL_THRESHOLDS, siteUrl) {
        this.supabase = supabase;
        this.thresholds = thresholds;
        this.siteUrl = siteUrl;
    }
    /**
     * Checks whether the protective stop is currently active.
     * Reads from database 'settings' table with a 3s TTL cache.
     */
    async isProtectiveStopActive() {
        const info = await this.getProtectiveStopInfo();
        return info.active;
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
            catch {
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
                        classification: parsed.classification || null,
                        details: parsed.details || null,
                        triggered_at: parsed.triggered_at || null,
                        resumed_at: parsed.resumed_at || null,
                        resumed_by: parsed.resumed_by || null,
                    };
                }
                catch {
                    this.cachedInfo = { active: data.value === 'true' || data.value === 'ACTIVE' };
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
     * Triggers a persistent PROTECTIVE STOP with incident classification.
     * Halts all new job claims, allows in-flight jobs to safely drain,
     * keeps publication barrier alive.
     */
    async triggerProtectiveStop(reason, details, classification = 'REAL_SYSTEM_PRESSURE') {
        const nowIso = new Date().toISOString();
        const payload = {
            active: true,
            reason,
            classification,
            details,
            triggered_at: nowIso,
        };
        this.cachedInfo = payload;
        this.lastFetchMs = Date.now();
        this.consecutive5xxErrors.clear();
        this.consecutiveLatencyViolations.clear();
        this.consecutiveProbeErrors.clear();
        this.logger.error(`🚨 [PROTECTIVE_STOP TRIGGERED] [${classification}] ${reason}. Halting new claims immediately. In-flight jobs will safely drain.`, { reason, classification, details, triggered_at: nowIso });
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
     * Resumes normal operation.
     */
    async resumeProtectiveStop(resumedBy = 'manual_staff') {
        const nowIso = new Date().toISOString();
        const payload = {
            active: false,
            resumed_at: nowIso,
            resumed_by: resumedBy,
        };
        this.cachedInfo = payload;
        this.lastFetchMs = Date.now();
        this.consecutive5xxErrors.clear();
        this.consecutiveLatencyViolations.clear();
        this.consecutiveProbeErrors.clear();
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
     * Background sentinel watchdog loop.
     * Probes metrics every 15s. If stopped, triggers rapid auto-heal checks.
     */
    startWatchdogLoop() {
        if (this.isRunning)
            return;
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
                    }
                    else {
                        await this.evaluateAutoResume();
                    }
                }
                catch (err) {
                    this.logger.warn('Error during Sentinel pre-SLA evaluation', { error: err?.message });
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
     * Evaluates whether a currently stopped importer can safely auto-resume.
     * Distinguishes transient edge incidents from sustained pressure.
     * Checks 2 consecutive healthy samples with 5s debounce for rapid recovery (15-30s).
     * NEVER auto-resumes manual staff stops or active ongoing degradation.
     */
    async evaluateAutoResume() {
        try {
            const stopInfo = await this.getProtectiveStopInfo(true);
            if (!stopInfo.active)
                return;
            // Staff manual stops require staff manual resumption
            if (stopInfo.reason?.toLowerCase().includes('manual') ||
                stopInfo.reason?.toLowerCase().includes('staff') ||
                stopInfo.classification === 'MANUAL_STOP') {
                return;
            }
            // Check current infrastructure
            if (global.gc) {
                try {
                    global.gc();
                }
                catch { }
            }
            const mem = diagnostics.getMemorySnapshot();
            // Only block auto-resume on RAM if RSS exceeds the actual tripwire (440MB) or heap is severely bloated (>280MB)
            if (mem.rssMb >= this.thresholds.maxRssMb || mem.heapUsedMb >= 280) {
                this.logger.warn(`[Auto-Resume] RAM too high for auto-resume: ${mem.rssMb}MB rss (limit: ${this.thresholds.maxRssMb}MB), ${mem.heapUsedMb}MB heap`);
                return;
            }
            let activeConns = 0;
            let totalConns = 0;
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
            catch { }
            // Must have calm DB (<12 total, <4 active). Baseline idle pools (Hyperdrive + Importer) hold ~9-10 idle connections.
            if (totalConns >= 12 || activeConns >= 4) {
                this.logger.warn(`[Auto-Resume] YSQL not calm yet: total=${totalConns}/13, active=${activeConns}`);
                return;
            }
            // Check if stop was triggered by transient local pressure (lag/RAM) that has now resolved
            const stoppedAgeSec = stopInfo.triggered_at
                ? Math.floor((Date.now() - new Date(stopInfo.triggered_at).getTime()) / 1000)
                : 0;
            if (stoppedAgeSec >= 15 * 60 && stopInfo.classification === 'IMPORTER_PRESSURE') {
                const lagMetrics = diagnostics.lagMonitor?.getMetrics?.() || { avgLagMs: 0 };
                if (lagMetrics.avgLagMs < 150 && mem.rssMb < this.thresholds.maxRssMb) {
                    this.logger.info(`🛡️ [AUTO-HEAL / AUTO-RESUME] Stale protective stop (>15m) with resolved local pressure (lag=${lagMetrics.avgLagMs}ms, RSS=${mem.rssMb}MB, YSQL=${totalConns}/13). Auto-resuming claims immediately!`);
                    await this.resumeProtectiveStop('auto_healing_sentinel_recovery');
                    return;
                }
            }
            // Quick latency probes
            if (this.siteUrl) {
                // WAN-adjusted TTFB ceilings for remote container probes:
                // Home SSR document is ~180KB (remote container over WAN can take up to 2000ms when cold)
                // Reader is ~40KB (up to 750ms WAN TTFB is healthy)
                const homeMaxTtfb = 2000;
                const readerMaxTtfb = 750;
                // Sample 1
                const homeProbe1 = await this.measureRoute(`${this.siteUrl}/`, homeMaxTtfb, 'home');
                const readerProbe1 = await this.measureRoute(`${this.siteUrl}/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c`, readerMaxTtfb, 'reader');
                const isSample1Healthy = Boolean((readerProbe1 && readerProbe1.statusCode >= 200 && readerProbe1.statusCode < 400 && readerProbe1.ttfbMs <= readerMaxTtfb) &&
                    (!homeProbe1 || (homeProbe1.statusCode >= 200 && homeProbe1.statusCode < 400 && homeProbe1.ttfbMs <= homeMaxTtfb)));
                if (!isSample1Healthy) {
                    this.logger.warn(`[Auto-Resume] Sample 1 unhealthy: Home=${homeProbe1?.ttfbMs}ms [${homeProbe1?.statusCode}], Reader=${readerProbe1?.ttfbMs}ms [${readerProbe1?.statusCode}]`);
                    return;
                }
                // Wait 5s debounce between samples for fast, reliable verification
                await new Promise((r) => setTimeout(r, 5000));
                // Sample 2
                const homeProbe2 = await this.measureRoute(`${this.siteUrl}/`, homeMaxTtfb, 'home');
                const readerProbe2 = await this.measureRoute(`${this.siteUrl}/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c`, readerMaxTtfb, 'reader');
                const isSample2Healthy = Boolean((readerProbe2 && readerProbe2.statusCode >= 200 && readerProbe2.statusCode < 400 && readerProbe2.ttfbMs <= readerMaxTtfb) &&
                    (!homeProbe2 || (homeProbe2.statusCode >= 200 && homeProbe2.statusCode < 400 && homeProbe2.ttfbMs <= homeMaxTtfb)));
                if (isSample2Healthy) {
                    this.logger.info(`🛡️ [AUTO-HEAL / AUTO-RESUME] Transient edge oscillation resolved. 2 consecutive healthy samples verified (Home: ${homeProbe2?.ttfbMs}ms [${homeProbe2?.statusCode}], Reader: ${readerProbe2.ttfbMs}ms [${readerProbe2.statusCode}], YSQL: ${totalConns}/13 total [${activeConns} active]). Auto-resuming claims immediately!`);
                    await this.resumeProtectiveStop('auto_healing_sentinel_recovery');
                }
                else {
                    this.logger.warn(`[Auto-Resume] Sample 2 unhealthy after 5s debounce: Home=${homeProbe2?.ttfbMs}ms [${homeProbe2?.statusCode}], Reader=${readerProbe2?.ttfbMs}ms [${readerProbe2?.statusCode}]`);
                }
            }
        }
        catch (err) {
            this.logger.warn('Failed during auto-resume evaluation', { error: err?.message });
        }
    }
    async measureRoute(url, thresholdMs, label = 'home') {
        return new Promise((resolve) => {
            const t0 = performance.now();
            const isHttps = url.startsWith('https:');
            const mod = isHttps ? https : http;
            const req = mod.get(url, {
                agent: isHttps ? (label === 'reader' ? this.readerAgent : this.homeAgent) : this.httpAgent,
                headers: {
                    'User-Agent': 'Project-Nox-Sentinel/1.0 (Auto-Resume Probe)',
                },
                timeout: 4000,
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
                    resolve({
                        ttfbMs: Math.round(performance.now() - t0),
                        statusCode: res.statusCode || 500,
                    });
                };
                res.once('data', finish);
                res.on('end', finish);
            });
            req.on('error', () => resolve(null));
            req.on('timeout', () => { req.destroy(); resolve(null); });
        });
    }
    /**
     * Evaluates all Pre-SLA guard rails.
     */
    async evaluatePreSlaGuardRails() {
        // 1. RAM Check (rssMb > 440MB of 512MB)
        const mem = diagnostics.getMemorySnapshot();
        if (mem.rssMb >= this.thresholds.maxRssMb) {
            await this.triggerProtectiveStop(`Pre-SLA RAM Tripwire Exceeded: ${mem.rssMb}MB >= ${this.thresholds.maxRssMb}MB (512MB limit)`, { rssMb: mem.rssMb, heapUsedMb: mem.heapUsedMb }, 'IMPORTER_PRESSURE');
            return;
        }
        // 2. Event Loop Lag Check (> 350ms)
        const lagMetrics = diagnostics.lagMonitor?.getMetrics?.() || { avgLagMs: 0 };
        if (lagMetrics.avgLagMs >= this.thresholds.maxEventLoopLagMs) {
            await this.triggerProtectiveStop(`Pre-SLA Event Loop Lag Tripwire Exceeded: ${lagMetrics.avgLagMs}ms >= ${this.thresholds.maxEventLoopLagMs}ms`, { avgLagMs: lagMetrics.avgLagMs }, 'IMPORTER_PRESSURE');
            return;
        }
        // 3. YSQL Connection Tripwire (>= 12 of 13)
        try {
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
                const { data: connData, error: connErr } = await this.supabase.rpc('importer_active_connections_count');
                if (!connErr && typeof connData === 'number') {
                    totalConns = connData;
                    activeConns = connData;
                }
            }
            if (totalConns >= this.thresholds.ysqlConnTripwire || activeConns >= 8) {
                await this.triggerProtectiveStop(`Pre-SLA YSQL Connection Tripwire Exceeded: ${totalConns} total connections (${activeConns} active) >= ${this.thresholds.ysqlConnTripwire} (limit 13)`, { totalConnections: totalConns, activeConnections: activeConns, tripwire: this.thresholds.ysqlConnTripwire }, 'YSQL_PRESSURE');
                return;
            }
        }
        catch { }
        // 4. Site Latency Probes (Home > 250ms pre-SLA, Reader > 210ms pre-SLA)
        if (this.siteUrl) {
            await this.probeSiteLatency('home', `${this.siteUrl}/`, this.thresholds.homeTtfbPreSlaMs, 350);
            await new Promise((r) => setTimeout(r, 2000));
            await this.probeSiteLatency('reader', `${this.siteUrl}/ler/46b7538b-fcb8-40ec-b3ee-cdadd2edb04c`, this.thresholds.readerTtfbPreSlaMs, 250);
        }
    }
    /**
     * Probes site route latency using keep-alive connection.
     */
    async probeSiteLatency(label, url, thresholdMs, slaTargetMs, isRetry = false) {
        return new Promise((resolve) => {
            const t0 = performance.now();
            const isHttps = url.startsWith('https:');
            const mod = isHttps ? https : http;
            const req = mod.get(url, {
                agent: isHttps ? (label === 'reader' ? this.readerAgent : this.homeAgent) : this.httpAgent,
                headers: {
                    'User-Agent': 'Project-Nox-Sentinel/1.0 (Pre-SLA Monitor)',
                },
                timeout: 5000,
            }, (res) => {
                let resolved = false;
                const finish = async () => {
                    if (resolved)
                        return;
                    resolved = true;
                    try {
                        res.resume();
                    }
                    catch { }
                    const ttfbMs = Math.round(performance.now() - t0);
                    await this.handleProbeResult(label, url, ttfbMs, thresholdMs, slaTargetMs, res.statusCode || 200);
                    resolve();
                };
                res.once('data', () => { void finish(); });
                res.on('end', () => { void finish(); });
            });
            req.on('error', async (err) => {
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
    /**
     * Evaluates probe responses, strictly distinguishing:
     * A) TRANSIENT EDGE INCIDENTS:
     *    - 1 isolated 5xx
     *    - normal WAN latency jitter
     *    - healthy DB and importer
     *    => DO NOT STOP, log warning and observe.
     *
     * B) REAL SYSTEM PRESSURE:
     *    - >= 3 consecutive 5xx errors (sustained edge/Worker failure)
     *    - >= 2 consecutive 5xx errors WITH confirmed infra pressure
     *    - Sustained severe latency (>= 3000ms) for 3+ consecutive probes
     *    => Trip PROTECTIVE_STOP with appropriate classification.
     */
    async handleProbeResult(label, url, ttfbMs, thresholdMs, slaTargetMs, statusCode) {
        const is5xx = statusCode >= 500;
        // Check infrastructure metrics
        const mem = diagnostics.getMemorySnapshot();
        const lagMetrics = diagnostics.lagMonitor?.getMetrics?.() || { avgLagMs: 0 };
        let activeConns = 0;
        let totalConns = 0;
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
        catch { }
        const hasYsqlPressure = totalConns >= this.thresholds.ysqlConnTripwire || activeConns >= 8;
        const hasImporterPressure = mem.rssMb >= 380 || lagMetrics.avgLagMs >= this.thresholds.maxEventLoopLagMs;
        const hasInfraPressure = hasYsqlPressure || hasImporterPressure;
        // 1. Handle HTTP 5xx Status
        if (is5xx) {
            this.consecutiveLatencyViolations.set(label, 0);
            const count5xx = (this.consecutive5xxErrors.get(label) || 0) + 1;
            this.consecutive5xxErrors.set(label, count5xx);
            // Case A: Isolated 5xx (count = 1) without infra pressure
            if (count5xx === 1 && !hasInfraPressure) {
                this.logger.warn(`[EDGE_TRANSIENT_WARNING] Isolated HTTP ${statusCode} on ${label.toUpperCase()} (${url}) TTFB: ${ttfbMs}ms. Infrastructure is healthy (YSQL: ${totalConns}/13 [${activeConns} active], RSS: ${mem.rssMb}MB, Lag: ${lagMetrics.avgLagMs}ms). NOT tripping PROTECTIVE_STOP. Observing next probe.`);
                return;
            }
            // Case B: Real sustained pressure: >=3 consecutive 5xx errors, OR >=2 with infra pressure
            const shouldTrip = count5xx >= 3 || (count5xx >= 2 && hasInfraPressure);
            if (shouldTrip) {
                this.consecutive5xxErrors.set(label, 0);
                const classification = hasYsqlPressure
                    ? 'YSQL_PRESSURE'
                    : hasImporterPressure
                        ? 'IMPORTER_PRESSURE'
                        : 'REAL_SYSTEM_PRESSURE';
                await this.triggerProtectiveStop(`Sustained HTTP ${statusCode} on ${label.toUpperCase()} (${count5xx} consecutive errors): observed ${ttfbMs}ms (YSQL: ${totalConns}/13 total [${activeConns} active], RSS: ${mem.rssMb}MB, Lag: ${lagMetrics.avgLagMs}ms)`, { label, url, ttfbMs, status: statusCode, count5xx, totalConns, activeConns, rssMb: mem.rssMb, lagMs: lagMetrics.avgLagMs }, classification);
                return;
            }
            else {
                this.logger.warn(`[HTTP_5XX_WARNING] Route ${label.toUpperCase()} returned HTTP ${statusCode} (${count5xx}/3 consecutive). YSQL: ${totalConns}/13 [${activeConns} active]. Observing.`);
                return;
            }
        }
        // 200 OK: Reset 5xx counter
        this.consecutive5xxErrors.set(label, 0);
        // 2. Handle Latency
        const isLatencyViolating = ttfbMs > thresholdMs;
        if (isLatencyViolating) {
            const countLatency = (this.consecutiveLatencyViolations.get(label) || 0) + 1;
            this.consecutiveLatencyViolations.set(label, countLatency);
            // Trip if latency breach is correlated with confirmed infra pressure
            const isSlaBreached = ttfbMs >= slaTargetMs;
            const shouldTrip = (isSlaBreached && countLatency >= 2 && hasInfraPressure) ||
                (countLatency >= 3 && hasInfraPressure) ||
                (ttfbMs >= 3000 && countLatency >= 3); // Extreme hung responses even without DB pressure
            if (shouldTrip) {
                this.consecutiveLatencyViolations.set(label, 0);
                const classification = hasYsqlPressure
                    ? 'YSQL_PRESSURE'
                    : hasImporterPressure
                        ? 'IMPORTER_PRESSURE'
                        : 'REAL_SYSTEM_PRESSURE';
                await this.triggerProtectiveStop(`Pre-SLA Latency Guard Rail Breached on ${label.toUpperCase()}: observed ${ttfbMs}ms (HTTP ${statusCode}) > ${isSlaBreached ? `SLA target ${slaTargetMs}ms` : `threshold ${thresholdMs}ms`} (YSQL: ${totalConns}/13 total [${activeConns} active], RSS: ${mem.rssMb}MB, Lag: ${lagMetrics.avgLagMs}ms)`, { label, url, ttfbMs, thresholdMs, slaTargetMs, status: statusCode, totalConns, activeConns, rssMb: mem.rssMb, lagMs: lagMetrics.avgLagMs }, classification);
            }
            else {
                this.logger.warn(`[Pre-SLA Latency Warning] Route ${label} (${url}) TTFB: ${ttfbMs}ms (HTTP ${statusCode}) > threshold ${thresholdMs}ms (SLA: ${slaTargetMs}ms). Consecutive sample: ${countLatency}/3. Infra healthy: ${!hasInfraPressure}.`);
            }
        }
        else {
            this.consecutiveLatencyViolations.set(label, 0);
        }
    }
    async handleProbeError(label, url, err) {
        const count = (this.consecutiveProbeErrors.get(label) || 0) + 1;
        this.consecutiveProbeErrors.set(label, count);
        // Check for correlated importer pressure
        const mem = diagnostics.getMemorySnapshot();
        const lagMetrics = diagnostics.lagMonitor?.getMetrics?.() || { avgLagMs: 0 };
        let activeConns = 0;
        let totalConns = 0;
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
        catch { }
        const hasInfraPressure = totalConns >= 12 || activeConns >= 8 || mem.rssMb >= 380 || lagMetrics.avgLagMs >= 200;
        if (count >= 3 && hasInfraPressure) {
            this.consecutiveProbeErrors.set(label, 0);
            await this.triggerProtectiveStop(`Pre-SLA Health Probe Failed on ${label.toUpperCase()} (${count} consecutive failures) with Correlated Importer Pressure: ${err?.message} (YSQL: ${totalConns}/13 total [${activeConns} active], RSS: ${mem.rssMb}MB, Lag: ${lagMetrics.avgLagMs}ms)`, { label, url, error: err?.message, totalConns, activeConns, rssMb: mem.rssMb, lagMs: lagMetrics.avgLagMs }, totalConns >= 12 || activeConns >= 8 ? 'YSQL_PRESSURE' : 'IMPORTER_PRESSURE');
        }
        else if (count >= 5) {
            this.consecutiveProbeErrors.set(label, 0);
            await this.triggerProtectiveStop(`Pre-SLA Health Probe Failed on ${label.toUpperCase()} (${count} consecutive timeouts/failures): ${err?.message}`, { label, url, error: err?.message, totalConns, activeConns, rssMb: mem.rssMb, lagMs: lagMetrics.avgLagMs }, 'REAL_SYSTEM_PRESSURE');
        }
        else {
            this.logger.warn(`[EDGE_PROBE_ERROR_WARNING] Health probe on ${label} (${url}) failed (${count}/5): ${err?.message}. Importer infra: YSQL ${totalConns}/13, RSS ${mem.rssMb}MB. Observing without tripping PROTECTIVE_STOP.`);
        }
    }
}
