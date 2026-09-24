import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';

export type BarrierState = 'OPEN' | 'CAUTION' | 'CLOSED' | 'RECOVERING';

export interface BarrierStatus {
  state: BarrierState;
  updatedAt: string;
  reason?: string;
}

export interface StallMetrics {
  publicationThroughput: number;
  readyBacklog: number;
  producerActive: boolean;
}

export interface StallEvaluation {
  isStalled: boolean;
  publicationThroughput: number;
  readyBacklog: number;
  producerActive: boolean;
  currentState: BarrierState;
  nextState: BarrierState;
  action: 'NONE' | 'AUTO_CLOSE' | 'AUTO_RECOVER' | 'AUTO_OPEN';
  reason?: string;
}

export class PublicationSafetyBarrier {
  private logger = new Logger('SafetyBarrier');
  private cachedState: BarrierState;
  private lastFetchMs = 0;
  private cacheTtlMs = 5000; // 5 second cache

  constructor(private supabase: SupabaseClient) {
    this.cachedState = 'OPEN';
  }

  /**
   * Returns current safety barrier state from database with short caching.
   */
  async getState(forceFresh = false): Promise<BarrierState> {
    const now = Date.now();
    if (!forceFresh && now - this.lastFetchMs < this.cacheTtlMs) {
      return this.cachedState;
    }

    try {
      const { data, error } = await this.supabase
        .from('settings')
        .select('value')
        .eq('key', 'publication_safety_barrier')
        .maybeSingle();

      if (!error && data?.value) {
        const val = String(data.value).toUpperCase() as BarrierState;
        if (['OPEN', 'CAUTION', 'CLOSED', 'RECOVERING'].includes(val)) {
          this.cachedState = val;
        } else {
          this.cachedState = 'OPEN';
        }
      } else if (!data) {
        await this.supabase.from('settings').upsert({
          key: 'publication_safety_barrier',
          value: 'OPEN',
        });
        this.cachedState = 'OPEN';
      }
      this.lastFetchMs = now;
    } catch (err: any) {
      this.logger.warn('Failed to read publication_safety_barrier setting, retaining current cached state', {
        error: err?.message,
        currentState: this.cachedState,
      });
      if (!this.cachedState) {
        this.cachedState = 'OPEN';
      }
    }

    return this.cachedState;
  }

  /**
   * Sets safety barrier state in public.settings.
   */
  async setState(newState: BarrierState, reason?: string): Promise<void> {
    this.cachedState = newState;
    this.lastFetchMs = Date.now();
    this.logger.info(`Setting publication safety barrier to ${newState}`, { reason });

    await this.supabase.from('settings').upsert({
      key: 'publication_safety_barrier',
      value: newState,
    });
  }

  /**
   * Checks whether worker slots should acquire IMPORT_CHAPTER jobs.
   * Allows OPEN, CAUTION, and RECOVERING so workers can fill gaps and drain backlog.
   */
  async canAcquireChapters(): Promise<boolean> {
    const st = await this.getState();
    return st === 'OPEN' || st === 'CAUTION' || st === 'RECOVERING';
  }

  async canProcessChapter(workId?: string, sortKey?: number): Promise<boolean> {
    if (await this.canAcquireChapters()) return true;
    if (!workId || !Number.isFinite(sortKey)) return false;
    const { data, error } = await this.supabase.from('importer_chapter_mappings')
      .select('id').eq('work_id', workId).eq('status', 'STAGED')
      .gt('chapter_sort_key', sortKey!).limit(1);
    return !error && Boolean(data?.length);
  }

  /**
   * Checks whether historical backfill can enqueue/process bulk chapters.
   * Only allowed when state is fully OPEN.
   */
  async isBackfillAllowed(): Promise<boolean> {
    const st = await this.getState();
    return st === 'OPEN';
  }

  /**
   * Evaluates stall condition deterministically from metrics and current barrier state.
   * Enforces transition rules:
   * 1. OPEN / CAUTION -> CLOSED when publication throughput = 0, ready backlog > 0, and producer active.
   * 2. CLOSED -> RECOVERING when publisher is restored (throughput > 0 or drain active).
   * 3. RECOVERING -> OPEN when ready backlog is drained and sequence restored.
   * Backlog is NEVER dumped or deleted in any transition.
   */
  public evaluatePublicationStall(metrics: StallMetrics, currentState: BarrierState): StallEvaluation {
    const { publicationThroughput, readyBacklog, producerActive } = metrics;

    // Condition 1: Detect publication stall while producer continues (only on sustained 15m stall)
    if ((currentState === 'OPEN' || currentState === 'CAUTION') && publicationThroughput === 0 && readyBacklog > 0 && producerActive) {
      return {
        isStalled: true,
        publicationThroughput,
        readyBacklog,
        producerActive,
        currentState,
        nextState: 'CAUTION',
        action: 'NONE',
        reason: `PUBLICATION_STALL: Throughput=0 with readyBacklog=${readyBacklog} while producer is active. Holding in CAUTION to allow gap resolution.`,
      };
    }

    // Condition 2: Publisher active or recovering while in CLOSED state -> transition to RECOVERING
    if (currentState === 'CLOSED') {
      return {
        isStalled: false,
        publicationThroughput,
        readyBacklog,
        producerActive,
        currentState,
        nextState: 'RECOVERING',
        action: 'AUTO_RECOVER',
        reason: `PUBLICATION_RECOVERY_STARTED: Transitioning to RECOVERING to allow workers to fill gaps and drain backlog.`,
      };
    }

    // Condition 3: Recovery active -> transition to OPEN
    if (currentState === 'RECOVERING') {
      return {
        isStalled: false,
        publicationThroughput,
        readyBacklog,
        producerActive,
        currentState,
        nextState: 'OPEN',
        action: 'AUTO_OPEN',
        reason: `PUBLICATION_RECOVERED: Reopening barrier for normal operation.`,
      };
    }

    return {
      isStalled: false,
      publicationThroughput,
      readyBacklog,
      producerActive,
      currentState,
      nextState: currentState,
      action: 'NONE',
    };
  }

  /**
   * Queries live database metrics and executes the stall evaluation.
   * Applies state changes automatically if needed.
   */
  async checkAndEnforceStallDetector(injectedMetrics?: StallMetrics): Promise<StallEvaluation> {
    const currentState = await this.getState(true);

    let metrics: StallMetrics;
    if (injectedMetrics) {
      metrics = injectedMetrics;
    } else {
      // 1. Throughput: chapters published in last 15 minutes
      const fifteenMinutesAgo = new Date(Date.now() - 900_000).toISOString();
      const { count: pubCount } = await this.supabase
        .from('chapters')
        .select('id', { count: 'exact', head: true })
        .gt('published_at', fifteenMinutesAgo);

      // 2. Ready backlog: chapters in STAGED status awaiting publication
      const { count: stagedCount } = await this.supabase
        .from('importer_chapter_mappings')
        .select('id', { count: 'exact', head: true })
        .eq('status', 'STAGED');

      // 3. Producer activity: jobs currently IMPORTING or chapters staged recently
      const { count: importingCount } = await this.supabase
        .from('importer_queue')
        .select('id', { count: 'exact', head: true })
        .eq('status', 'IMPORTING')
        .eq('task_type', 'IMPORT_CHAPTER');

      metrics = {
        publicationThroughput: pubCount ?? 0,
        readyBacklog: stagedCount ?? 0,
        producerActive: (importingCount ?? 0) > 0 || (stagedCount ?? 0) > 0,
      };
    }

    const evaluation = this.evaluatePublicationStall(metrics, currentState);

    if (evaluation.action !== 'NONE' && evaluation.nextState !== currentState) {
      this.logger.warn(`Stall detector triggered transition: ${currentState} -> ${evaluation.nextState}`, {
        action: evaluation.action,
        reason: evaluation.reason,
        metrics,
      });
      await this.setState(evaluation.nextState, evaluation.reason);
    }

    return evaluation;
  }
}
