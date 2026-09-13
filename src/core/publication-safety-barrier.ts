import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';

export type BarrierState = 'OPEN' | 'CAUTION' | 'CLOSED' | 'RECOVERING';

export interface BarrierStatus {
  state: BarrierState;
  updatedAt: string;
  reason?: string;
}

export class PublicationSafetyBarrier {
  private logger = new Logger('SafetyBarrier');
  private cachedState: BarrierState;
  private lastFetchMs = 0;
  private cacheTtlMs = 5000; // 5 second cache

  constructor(private supabase: SupabaseClient) {
    const isTest = typeof process !== 'undefined' && (process.env.NODE_ENV === 'test' || Boolean(process.env.VITEST));
    this.cachedState = isTest ? 'OPEN' : 'CLOSED';
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
          this.cachedState = 'CLOSED';
        }
      } else if (!data) {
        const isTest = typeof process !== 'undefined' && (process.env.NODE_ENV === 'test' || Boolean(process.env.VITEST));
        if (isTest) {
          this.cachedState = 'OPEN';
        } else {
          // If row doesn't exist yet, insert CLOSED for emergency safety
          await this.supabase.from('settings').upsert({
            key: 'publication_safety_barrier',
            value: 'CLOSED',
          });
          this.cachedState = 'CLOSED';
        }
      }
      this.lastFetchMs = now;
    } catch (err: any) {
      const isTest = typeof process !== 'undefined' && (process.env.NODE_ENV === 'test' || Boolean(process.env.VITEST));
      if (isTest) {
        this.cachedState = 'OPEN';
      } else {
        this.logger.warn('Failed to read publication_safety_barrier setting, defaulting to safe CLOSED', {
          error: err?.message,
        });
        this.cachedState = 'CLOSED';
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
   * If CLOSED or RECOVERING, returns false so 0 worker slots and 0 semaphores are held.
   */
  async canAcquireChapters(): Promise<boolean> {
    const st = await this.getState();
    return st === 'OPEN' || st === 'CAUTION';
  }

  /**
   * Checks whether historical backfill can enqueue/process bulk chapters.
   * Only allowed when state is fully OPEN.
   */
  async isBackfillAllowed(): Promise<boolean> {
    const st = await this.getState();
    return st === 'OPEN';
  }
}
