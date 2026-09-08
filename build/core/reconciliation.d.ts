import type { SupabaseClient } from '@supabase/supabase-js';
import { ImporterQueue } from './queue.js';
import { SourceRegistry } from '../sources/registry.js';
export interface ReconciliationStats {
    worksScanned: number;
    worksWithConfirmedGaps: number;
    confirmedGapsDiscovered: number;
    newChaptersDiscovered: number;
    jobsEnqueued: number;
    stagedSkipped: number;
    duplicatesAvoided: number;
}
export declare class ExistingWorksReconciler {
    private supabase;
    private queue;
    private registry;
    private logger;
    private lastReconciliationAt;
    constructor(supabase: SupabaseClient, queue: ImporterQueue, registry: SourceRegistry);
    /**
     * Reconcilia obras existentes de forma paginada e com baixo custo de rede.
     * Utiliza deduplicação estritamente canônica (workId:sortKey) e protege capítulos STAGED.
     *
     * @param batchSize Quantidade de obras a reconciliar por rodada (default: 20)
     */
    reconcileExistingWorks(batchSize?: number): Promise<ReconciliationStats>;
}
