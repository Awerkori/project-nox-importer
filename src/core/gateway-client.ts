import { Logger } from './logger.js';
import { AsyncSemaphore } from './concurrency.js';

export interface GatewayJob {
  id: string;
  task_type: string;
  source: string;
  priority: number;
  payload: Record<string, any>;
  dedupe_key: string;
  status: string;
  attempts: number;
  max_attempts: number;
  locked_by: string | null;
  locked_at: string | null;
  lease_expires_at: string | null;
  next_run_at: string;
  last_error: string | null;
  chapter_sort_key: number | null;
}

export interface PublishBatchParams {
  jobId?: string;
  work: {
    id?: string;
    slug: string;
    title: string;
    synopsis?: string;
    description?: string;
    author?: string;
    artist?: string;
    kind?: string;
    status?: string;
    ageRating?: number;
    aliases?: string[];
  };
  workMapping?: {
    source: string;
    sourceWorkId: string;
    sourceSlug: string;
    sourceTitle: string;
    metadata?: any;
    confidenceScore?: number;
    isPrimary?: boolean;
  };
  chapter: {
    number: number;
    title?: string;
    chapterSortKey?: number;
    sourceChapterId: string;
    source: string;
  };
  pages: Array<{
    position: number;
    mediaId?: string;
    providerKey: string;
    botReference?: string;
    storageShardId?: string;
    mime: string;
    width: number;
    height: number;
    bytes: number;
    sha256: string;
  }>;
  isPageProvider?: boolean;
}

export class ImporterGatewayClient {
  private logger = new Logger('GatewayClient');
  private baseUrl: string;
  private bridgeToken: string;
  // Bounded concurrency limiter: at most 4 simultaneous HTTPS calls from Importer to Gateway
  private gatewayLimiter = new AsyncSemaphore(4);

  constructor(
    mangaUrl: string,
    bridgeToken: string
  ) {
    if (!bridgeToken) {
      throw new Error('ImporterGatewayClient requires valid NOX_STORAGE_BRIDGE_TOKEN');
    }
    this.bridgeToken = bridgeToken;
    this.baseUrl = `${mangaUrl.replace(/\/$/, '')}/api/internal/importer`;
  }

  private async request<T = any>(endpoint: string, options: {
    method?: 'GET' | 'POST';
    body?: any;
    timeoutMs?: number;
  } = {}): Promise<T> {
    return await this.gatewayLimiter.runExclusive(async () => {
      const url = `${this.baseUrl}${endpoint.startsWith('/') ? endpoint : `/${endpoint}`}`;
      const method = options.method || 'GET';
      const timeoutMs = options.timeoutMs || 20000;

      const headers: Record<string, string> = {
        Authorization: `Bearer ${this.bridgeToken}`,
        Accept: 'application/json',
        'User-Agent': 'ProjectNox-Importer-GatewayClient/1.0',
      };
      if (options.body) {
        headers['Content-Type'] = 'application/json';
      }

      const res = await fetch(url, {
        method,
        headers,
        body: options.body ? JSON.stringify(options.body) : undefined,
        signal: AbortSignal.timeout(timeoutMs),
      });

      if (res.status === 401 || res.status === 403) {
        throw new Error(`Gateway authentication failed: HTTP ${res.status}`);
      }

      const json = await res.json().catch(() => null);

      if (!res.ok || json?.success === false) {
        const errMsg = json?.error || `Gateway request to ${endpoint} failed with HTTP ${res.status}`;
        throw new Error(errMsg);
      }

      return json as T;
    });
  }

  /* 1. Job Acquisition */
  async acquireJobs(options: {
    workerId: string;
    leaseDurationMinutes?: number;
    source?: string;
    taskType?: string;
    batchSize?: number;
  }): Promise<GatewayJob[]> {
    const res = await this.request<{ success: boolean; jobs: GatewayJob[] }>('/acquire-jobs', {
      method: 'POST',
      body: options,
      timeoutMs: 15000,
    });
    return res.jobs || [];
  }

  /* 2. Heartbeat Batch */
  async heartbeat(workerId: string, jobs: Array<{
    jobId: string;
    progressCurrent?: number;
    progressTotal?: number;
    progressStage?: string;
    leaseDurationMinutes?: number;
  }>): Promise<Array<{ jobId: string; status: string; cancelRequested: boolean; renewed: boolean }>> {
    const res = await this.request<{ success: boolean; updates: any[] }>('/heartbeat', {
      method: 'POST',
      body: { workerId, jobs },
      timeoutMs: 15000,
    });
    return res.updates || [];
  }

  /* 3. Fail / Retry Batch */
  async failBatch(workerId: string, jobs: Array<{
    jobId: string;
    status?: 'RETRY' | 'FAILED' | 'PAUSED_BY_STAFF' | 'BLOCKED_BY_UPSTREAM';
    error?: string;
    retryDelaySeconds?: number;
    retryReason?: string;
  }>): Promise<number> {
    const res = await this.request<{ success: boolean; updatedCount: number }>('/fail-batch', {
      method: 'POST',
      body: { workerId, jobs },
      timeoutMs: 15000,
    });
    return res.updatedCount || 0;
  }

  /* 4. Atomic Publication Batch */
  async publishBatch(payload: PublishBatchParams): Promise<{
    success: boolean;
    workId: string;
    chapterId: string;
    pageCount: number;
    publishedAt: string;
  }> {
    return await this.request('/publish-batch', {
      method: 'POST',
      body: payload,
      timeoutMs: 30000,
    });
  }

  /* 5. Enqueue Jobs */
  async enqueueJobs(jobs: Array<{
    taskType: string;
    source: string;
    dedupeKey: string;
    payload?: Record<string, any>;
    priority?: number;
    chapterSortKey?: number | null;
  }>): Promise<number> {
    const res = await this.request<{ success: boolean; enqueuedCount: number }>('/enqueue-jobs', {
      method: 'POST',
      body: { jobs },
      timeoutMs: 15000,
    });
    return res.enqueuedCount || 0;
  }

  /* 6. Recover Stalled Leases */
  async recoverStalled(): Promise<number> {
    const res = await this.request<{ success: boolean; recoveredCount: number }>('/recover-stalled', {
      method: 'POST',
      timeoutMs: 15000,
    });
    return res.recoveredCount || 0;
  }

  /* 7. Sources */
  async getSources(enabledOnly = false): Promise<any[]> {
    const res = await this.request<{ success: boolean; sources: any[] }>(`/sources?enabled=${enabledOnly}`, {
      method: 'GET',
      timeoutMs: 15000,
    });
    return res.sources || [];
  }

  async updateSource(sourceIdOrName: string, update: {
    lastSyncAt?: string;
    enabled?: boolean;
    config?: Record<string, any>;
  }): Promise<boolean> {
    const res = await this.request<{ success: boolean; updated: boolean }>('/sources', {
      method: 'POST',
      body: { id: sourceIdOrName, ...update },
      timeoutMs: 15000,
    });
    return Boolean(res.updated);
  }

  /* 8. Checkpoints */
  async getCheckpoint(source: string): Promise<any | null> {
    const res = await this.request<{ success: boolean; checkpoint: any }>(`/checkpoints?source=${encodeURIComponent(source)}`, {
      method: 'GET',
      timeoutMs: 10000,
    });
    return res.checkpoint || null;
  }

  async saveCheckpoint(source: string, cursorValue: string | null, metadata: Record<string, any> = {}): Promise<void> {
    await this.request('/checkpoints', {
      method: 'POST',
      body: { source, cursorValue, metadata },
      timeoutMs: 10000,
    });
  }

  /* 9. Work Deduplication / Matching */
  async resolveWork(candidate: {
    source: string;
    sourceWorkId: string;
    sourceSlug?: string;
    title: string;
    slug?: string;
    aliases?: string[];
  }): Promise<{
    matched: boolean;
    matchMethod?: string;
    workId?: string;
    mappingId?: string | null;
    work?: any;
  }> {
    return await this.request('/resolve-work', {
      method: 'POST',
      body: candidate,
      timeoutMs: 15000,
    });
  }

  /* 10. Safety Barrier */
  async getSafetyBarrier(): Promise<string> {
    const res = await this.request<{ success: boolean; state: string }>('/safety-barrier', {
      method: 'GET',
      timeoutMs: 10000,
    });
    return res.state || 'CLOSED';
  }

  async setSafetyBarrier(state: string): Promise<void> {
    await this.request('/safety-barrier', {
      method: 'POST',
      body: { state },
      timeoutMs: 10000,
    });
  }

  /* 11. Reconcile */
  async getReconcileWork(workId: string): Promise<any> {
    return await this.request(`/reconcile?workId=${encodeURIComponent(workId)}`, {
      method: 'GET',
      timeoutMs: 15000,
    });
  }

  async saveWorkHealth(healthData: {
    workId: string;
    status?: string;
    totalKnownChapters?: number;
    totalImportedChapters?: number;
    missingStart?: boolean;
    firstChapterNumber?: number | null;
    latestChapterNumber?: number | null;
    gaps?: any;
    unresolvedGaps?: any;
    providersSummary?: any;
  }): Promise<void> {
    await this.request('/reconcile', {
      method: 'POST',
      body: healthData,
      timeoutMs: 15000,
    });
  }

  /* 12. Stats */
  async getStats(): Promise<any> {
    const res = await this.request<{ success: boolean; stats: any }>('/stats', {
      method: 'GET',
      timeoutMs: 15000,
    });
    return res.stats;
  }

  /* 13. Raw Parameterized SQL Gateway */
  async sql<T = any>(query: string, params: any[] = []): Promise<{ rows: T[]; rowCount: number }> {
    const res = await this.request<{ success: boolean; rows: T[]; rowCount: number }>('/sql', {
      method: 'POST',
      body: { query, params },
      timeoutMs: 30000,
    });
    return { rows: res.rows || [], rowCount: res.rowCount ?? (res.rows ? res.rows.length : 0) };
  }

  /* 14. Batch Transaction SQL Gateway */
  async batchSql(queries: Array<{ text: string; params?: any[] }>): Promise<Array<{ rows: any[]; rowCount: number }>> {
    const res = await this.request<{ success: boolean; results: any[] }>('/sql', {
      method: 'POST',
      body: { queries: queries.map(q => ({ query: q.text, params: q.params })) },
      timeoutMs: 45000,
    });
    return res.results || [];
  }
}

