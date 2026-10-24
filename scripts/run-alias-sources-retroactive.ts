import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { createYugabyteMetadataPool } from '../src/db/yugabyte-direct.js';
import {
  AliasOnlyEnrichmentService,
  AliasOnlyRepository,
  MangaBallAliasSource,
  MangaDexAliasSource,
  MangaFireAliasSource,
  type AliasSourceId,
  type NoxWorkAliasTarget,
  type WorkAliasEnrichmentResult,
} from '../src/alias-sources/index.js';

interface CliOptions {
  campaignId: string;
  limit: number | null;
  concurrency: number;
  batchSize: number;
  rollbackCampaignId: string | null;
  statusFile: string | null;
  mdIntervalMs: number;
  mfIntervalMs: number;
  mbIntervalMs: number;
}

function parseCliArgs(argv: string[]): CliOptions {
  const getVal = (flag: string): string | undefined => {
    const idx = argv.indexOf(flag);
    if (idx >= 0 && idx + 1 < argv.length) return argv[idx + 1];
    return undefined;
  };

  const limitRaw = getVal('--limit');
  return {
    campaignId: getVal('--campaign') || 'alias_4sources_v1',
    limit: limitRaw ? Math.max(1, parseInt(limitRaw, 10)) : null,
    concurrency: Math.max(1, parseInt(getVal('--concurrency') || '12', 10)),
    batchSize: Math.max(1, parseInt(getVal('--batch-size') || '15', 10)),
    rollbackCampaignId: getVal('--rollback') || null,
    statusFile: getVal('--status-file') || null,
    mdIntervalMs: Math.max(180, parseInt(getVal('--md-interval') || '210', 10)),
    mfIntervalMs: Math.max(80, parseInt(getVal('--mf-interval') || '120', 10)),
    mbIntervalMs: Math.max(80, parseInt(getVal('--mb-interval') || '120', 10)),
  };
}

interface SourceRunCounters {
  consulted: number;
  confirmed: number;
  ambiguous: number;
  notFound: number;
  errors: number;
  aliasesAdded: number;
  ptBrAdded: number;
}

function emptySourceCounters(): Record<AliasSourceId, SourceRunCounters> {
  const mk = (): SourceRunCounters => ({
    consulted: 0,
    confirmed: 0,
    ambiguous: 0,
    notFound: 0,
    errors: 0,
    aliasesAdded: 0,
    ptBrAdded: 0,
  });
  return {
    mangadex: mk(),
    mangafire: mk(),
    mangaball: mk(),
  };
}

async function main(): Promise<void> {
  const opts = parseCliArgs(process.argv.slice(2));
  process.env.METADATA_DB_POOL_MAX = process.env.METADATA_DB_POOL_MAX || '2';

  const pool = createYugabyteMetadataPool();
  const repo = new AliasOnlyRepository(pool);

  try {
    if (opts.rollbackCampaignId) {
      console.log(`[Rollback] Reverting campaign "${opts.rollbackCampaignId}"...`);
      const res = await repo.rollbackCampaign(opts.rollbackCampaignId);
      console.log(`[Rollback] Done. Restored works: ${res.restoredWorks}`);
      return;
    }

    const service = new AliasOnlyEnrichmentService({
      maxQueriesPerSource: 2,
      mangaFireMaxDetailProbes: 2,
      adapters: {
        mangadex: new MangaDexAliasSource({
          maxConcurrent: 5,
          minIntervalMs: opts.mdIntervalMs,
          cacheMaxEntries: 16_000,
        }),
        mangafire: new MangaFireAliasSource({
          maxConcurrent: 6,
          minIntervalMs: opts.mfIntervalMs,
          cacheMaxEntries: 16_000,
        }),
        mangaball: new MangaBallAliasSource({
          maxConcurrent: 6,
          minIntervalMs: opts.mbIntervalMs,
          cacheMaxEntries: 16_000,
        }),
      },
    });

    const loadRes = await repo.loadPendingPublishedWorksForCampaign(opts.campaignId);
    const queue = opts.limit ? loadRes.pending.slice(0, opts.limit) : loadRes.pending;

    const bucketDistribution: Record<number, number> = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
    for (const item of queue) {
      bucketDistribution[item.priorityBucket] = (bucketDistribution[item.priorityBucket] || 0) + 1;
    }

    console.log(
      JSON.stringify({
        event: 'CAMPAIGN_START',
        campaignId: opts.campaignId,
        mode: opts.limit ? `PILOT_BENCHMARK(${opts.limit})` : 'FULL_RETROACTIVE',
        totalPublishedWorks: loadRes.totalPublished,
        alreadyCompletedBeforeRun: loadRes.alreadyCompleted,
        queuedThisRun: queue.length,
        bucketDistribution,
        concurrency: opts.concurrency,
        batchSize: opts.batchSize,
        intervalsMs: {
          mangadex: opts.mdIntervalMs,
          mangafire: opts.mfIntervalMs,
          mangaball: opts.mbIntervalMs,
        },
        startedAt: new Date().toISOString(),
      }),
    );

    if (queue.length === 0) {
      console.log('[Campaign] Nothing pending for this campaign. Exiting cleanly.');
      return;
    }

    const startedMs = performance.now();
    let processedThisRun = 0;
    let persistedThisRun = 0;
    let worksUpdatedThisRun = 0;
    let aliasesPersistedThisRun = 0;
    let ptBrPersistedThisRun = 0;
    let ambiguousWorksThisRun = 0;
    let sumWorkDurationMs = 0;
    let dbFlushCount = 0;
    let dbFlushLatencySumMs = 0;
    const perSourceCounters = emptySourceCounters();

    const pendingFlush: Array<{
      target: NoxWorkAliasTarget;
      priorityBucket: number;
      result: WorkAliasEnrichmentResult;
    }> = [];
    let flushChain = Promise.resolve();
    let lastProgressReportPersisted = 0;
    let lastProgressReportMs = startedMs;
    let stopRequested = false;

    const onSignal = (sig: string) => {
      if (!stopRequested) {
        stopRequested = true;
        console.warn(`[Campaign] Received ${sig}; draining in-flight tasks and flushing checkpoints...`);
      }
    };
    process.on('SIGINT', () => onSignal('SIGINT'));
    process.on('SIGTERM', () => onSignal('SIGTERM'));

    const buildProgressPayload = async (eventName: string) => {
      const elapsedSec = Math.max(0.001, (performance.now() - startedMs) / 1000);
      const worksPerMin = Number(((persistedThisRun / elapsedSec) * 60).toFixed(1));
      const remainingInRun = Math.max(0, queue.length - persistedThisRun);
      const etaMinutes =
        worksPerMin > 0 ? Number((remainingInRun / worksPerMin).toFixed(1)) : null;
      const fullCatalogRemaining = Math.max(
        0,
        loadRes.totalPublished - (loadRes.alreadyCompleted + persistedThisRun),
      );
      const fullCatalogEtaMinutes =
        worksPerMin > 0 ? Number((fullCatalogRemaining / worksPerMin).toFixed(1)) : null;

      const mem = process.memoryUsage();
      const health = service.getHealthSnapshots();
      const sourceTelemetry: Record<string, unknown> = {};
      for (const s of ['mangadex', 'mangafire', 'mangaball'] as const) {
        const h = health[s];
        const c = perSourceCounters[s];
        sourceTelemetry[s] = {
          ...c,
          matchRatePct: c.consulted > 0 ? Number(((c.confirmed / c.consulted) * 100).toFixed(1)) : 0,
          errorRatePct: c.consulted > 0 ? Number(((c.errors / c.consulted) * 100).toFixed(1)) : 0,
          httpRequests: h.requestsTotal,
          cacheHits: h.cacheHits,
          effectiveRps: Number((h.requestsTotal / elapsedSec).toFixed(2)),
          avgHttpLatencyMs: h.avgLatencyMs,
          rateLimited429: h.rateLimitedTotal,
          circuitOpen: h.circuitOpen,
          circuitReason: h.circuitReason ?? null,
        };
      }

      let importerHeartbeatAgeSec: number | null = null;
      let importerHeartbeatActive: boolean | null = null;
      try {
        const hb = await pool.query<{ value: unknown }>(
          `SELECT value FROM public.settings WHERE key = 'importer_heartbeat'`,
        );
        const raw = hb.rows[0]?.value;
        const parsed = typeof raw === 'string' ? JSON.parse(raw) : raw;
        const tsStr = parsed?.timestamp || parsed?.updatedAt || parsed?.observedAt || parsed?.ts;
        if (tsStr) {
          const age = Math.round((Date.now() - Date.parse(String(tsStr))) / 1000);
          if (Number.isFinite(age)) {
            importerHeartbeatAgeSec = age;
            importerHeartbeatActive = age <= 180;
          }
        }
      } catch {
        // non-fatal diagnostic
      }

      return {
        event: eventName,
        timestamp: new Date().toISOString(),
        campaignId: opts.campaignId,
        completedTotalCatalog: loadRes.alreadyCompleted + persistedThisRun,
        totalPublishedWorks: loadRes.totalPublished,
        persistedThisRun,
        queuedThisRun: queue.length,
        remainingInRun,
        fullCatalogRemaining,
        worksUpdatedThisRun,
        aliasesPersistedThisRun,
        ptBrPersistedThisRun,
        ambiguousWorksThisRun,
        throughputWorksPerMin: worksPerMin,
        avgWallMsPerWork: Number(((elapsedSec * 1000) / Math.max(1, persistedThisRun)).toFixed(1)),
        avgWorkerDurationMs: Number((sumWorkDurationMs / Math.max(1, processedThisRun)).toFixed(1)),
        elapsedMinutes: Number((elapsedSec / 60).toFixed(2)),
        etaRunMinutes: etaMinutes,
        etaFullCatalogMinutes: fullCatalogEtaMinutes,
        memory: {
          rssMB: Math.round(mem.rss / 1048576),
          heapUsedMB: Math.round(mem.heapUsed / 1048576),
        },
        ysqlPool: {
          total: pool.totalCount,
          idle: pool.idleCount,
          waiting: pool.waitingCount,
          flushes: dbFlushCount,
          avgFlushMs: dbFlushCount > 0 ? Math.round(dbFlushLatencySumMs / dbFlushCount) : 0,
        },
        chapterImporterHeartbeatAgeSec: importerHeartbeatAgeSec,
        chapterImporterHealthy: importerHeartbeatActive,
        sources: sourceTelemetry,
      };
    };

    const scheduleFlush = (forceAll = false): Promise<void> => {
      if (pendingFlush.length === 0) return flushChain;
      if (!forceAll && pendingFlush.length < opts.batchSize) return flushChain;

      const batch = pendingFlush.splice(0, forceAll ? pendingFlush.length : opts.batchSize);
      flushChain = flushChain.then(async () => {
        const t0 = performance.now();
        const dbRes = await repo.persistBatchResults(opts.campaignId, batch);
        const flushMs = performance.now() - t0;

        dbFlushCount++;
        dbFlushLatencySumMs += flushMs;
        persistedThisRun += batch.length;
        worksUpdatedThisRun += dbRes.worksUpdated;
        aliasesPersistedThisRun += dbRes.aliasesPersisted;
        ptBrPersistedThisRun += dbRes.ptBrPersisted;

        const nowMs = performance.now();
        const hit500 = persistedThisRun - lastProgressReportPersisted >= 500;
        const hit10Min = nowMs - lastProgressReportMs >= 10 * 60_000;

        if (opts.statusFile) {
          const snap = await buildProgressPayload('STATUS_SNAPSHOT');
          fs.mkdirSync(path.dirname(opts.statusFile), { recursive: true });
          fs.writeFileSync(opts.statusFile, JSON.stringify(snap, null, 2));
        }

        if (hit500 || hit10Min) {
          lastProgressReportPersisted = persistedThisRun;
          lastProgressReportMs = nowMs;
          const progress = await buildProgressPayload('CAMPAIGN_PROGRESS');
          console.log(JSON.stringify(progress));
        }
      });
      return flushChain;
    };

    let cursor = 0;
    const workerLoop = async (): Promise<void> => {
      while (!stopRequested) {
        const idx = cursor++;
        if (idx >= queue.length) break;
        const item = queue[idx];

        const result = await service.enrichWork(item);
        processedThisRun++;
        sumWorkDurationMs += result.durationMs;
        if (result.ambiguousSources.length > 0) ambiguousWorksThisRun++;

        for (const s of ['mangadex', 'mangafire', 'mangaball'] as const) {
          const rec = result.perSource[s];
          if (!rec || (rec.evidence as any)?.skipped) continue;
          const c = perSourceCounters[s];
          c.consulted++;
          if (rec.status === 'CONFIRMED_MATCH') c.confirmed++;
          else if (rec.status === 'AMBIGUOUS_MATCH') c.ambiguous++;
          else if (rec.status === 'NOT_FOUND') c.notFound++;
          else if (rec.status === 'SOURCE_ERROR') c.errors++;
          c.aliasesAdded += rec.addedAliases.length;
          c.ptBrAdded += rec.ptBrAliasesAdded.length;
        }

        pendingFlush.push({
          target: item,
          priorityBucket: item.priorityBucket,
          result,
        });

        if (pendingFlush.length >= opts.batchSize) {
          await scheduleFlush(false);
        }
      }
    };

    const workers = Array.from({ length: Math.min(opts.concurrency, queue.length) }, () =>
      workerLoop(),
    );
    await Promise.all(workers);

    while (pendingFlush.length > 0) {
      await scheduleFlush(true);
    }
    await flushChain;

    const finalSummary = await buildProgressPayload('CAMPAIGN_COMPLETE');
    if (opts.statusFile) {
      fs.mkdirSync(path.dirname(opts.statusFile), { recursive: true });
      fs.writeFileSync(opts.statusFile, JSON.stringify(finalSummary, null, 2));
    }
    console.log(JSON.stringify(finalSummary, null, 2));
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error('[Campaign] Fatal error:', err);
  process.exit(1);
});
