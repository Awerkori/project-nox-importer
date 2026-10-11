/**
 * Durable vocabulary for Importer Lite.  These are deliberately smaller than
 * the legacy queue vocabulary: detail belongs in checkpoints and reasons, not
 * in ever more states.
 */
export const LITE_PRIORITIES = ['STAFF', 'P0', 'P1', 'P2'] as const;
export type LitePriority = (typeof LITE_PRIORITIES)[number];
export const LITE_PRIORITY_RANK: Record<LitePriority, number> = {
  STAFF: 0,
  P0: 1,
  P1: 2,
  P2: 3,
};

export const LITE_TASK_STATES = ['READY', 'RUNNING', 'RETRY_WAIT', 'BLOCKED', 'DONE'] as const;
export type LiteTaskState = (typeof LITE_TASK_STATES)[number];
export type LiteTaskType = 'DISCOVER' | 'CATALOG' | 'IMPORT_CHAPTER' | 'PUBLISH';

export interface LiteTask {
  id: string;
  taskType: LiteTaskType;
  workId: string | null;
  workKey: string;
  chapterKey: string | null;
  priority: LitePriority;
  basePriority: LitePriority;
  state: LiteTaskState;
  idempotencyKey: string;
  nextRunAt: Date;
  attempts: number;
  maxAttempts: number;
  leaseOwner: string | null;
  leaseUntil: Date | null;
  reason: string | null;
  payload: Record<string, unknown>;
  checkpoint: Record<string, unknown>;
}

export type SourceHealth = 'HEALTHY' | 'DEGRADED' | 'RATE_LIMITED' | 'BROKEN' | 'DISABLED';

export interface ChapterSourceRoute {
  source: string;
  sourceChapterId: string;
  canonicalChapterKey: string;
  workId: string;
  language?: string | null;
  edition?: string | null;
  sourceRank: number;
  health: SourceHealth;
  cooldownUntil?: Date | null;
  confirmed: boolean;
  hasChapter: boolean;
}

export interface SourceSelection {
  route: ChapterSourceRoute | null;
  reason?: 'NO_CONFIRMED_ROUTE' | 'ALL_ROUTES_COOLDOWN' | 'ALL_ROUTES_UNHEALTHY';
}
