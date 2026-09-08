import type { SupabaseClient } from '@supabase/supabase-js';
export interface SourceCheckpoint {
    id: string;
    source: string;
    cursor_value: string | null;
    last_checked_at: string;
    metadata: Record<string, any>;
}
export declare class CheckpointManager {
    private supabase;
    private logger;
    constructor(supabase: SupabaseClient);
    getCheckpoint(source: string): Promise<SourceCheckpoint | null>;
    saveCheckpoint(source: string, cursorValue: string | null, metadata?: Record<string, any>): Promise<void>;
    isCatalogCompleted(source: string): Promise<boolean>;
    markCatalogCompleted(source: string, lastCursor: string | null, additionalMetadata?: Record<string, any>): Promise<void>;
}
