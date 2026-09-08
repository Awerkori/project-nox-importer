import { Logger } from './logger.js';
export class CheckpointManager {
    supabase;
    logger = new Logger('Checkpoint');
    constructor(supabase) {
        this.supabase = supabase;
    }
    async getCheckpoint(source) {
        const { data, error } = await this.supabase
            .from('importer_checkpoints')
            .select('*')
            .eq('source', source)
            .maybeSingle();
        if (error) {
            this.logger.error('Failed to fetch checkpoint', { source, error: error.message });
            throw error;
        }
        return data;
    }
    async saveCheckpoint(source, cursorValue, metadata = {}) {
        const { error } = await this.supabase
            .from('importer_checkpoints')
            .upsert({
            source,
            cursor_value: cursorValue,
            last_checked_at: new Date().toISOString(),
            metadata,
            updated_at: new Date().toISOString(),
        }, { onConflict: 'source' });
        if (error) {
            this.logger.error('Failed to save checkpoint', { source, error: error.message });
            throw error;
        }
        this.logger.debug('Checkpoint updated', { source, cursorValue });
    }
    async isCatalogCompleted(source) {
        const cp = await this.getCheckpoint(source);
        return Boolean(cp?.metadata?.catalog_completed);
    }
    async markCatalogCompleted(source, lastCursor, additionalMetadata = {}) {
        const existing = await this.getCheckpoint(source);
        const metadata = {
            ...(existing?.metadata || {}),
            ...additionalMetadata,
            catalog_completed: true,
            catalog_completed_at: new Date().toISOString(),
        };
        await this.saveCheckpoint(source, lastCursor, metadata);
        this.logger.info(`Source ${source} completed catalog bootstrap. Transitioned to maintenance mode.`, {
            source,
            completedAt: metadata.catalog_completed_at,
        });
    }
}
