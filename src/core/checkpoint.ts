import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';

export interface SourceCheckpoint {
  id: string;
  source: string;
  cursor_value: string | null;
  last_checked_at: string;
  metadata: Record<string, any>;
}

export class CheckpointManager {
  private logger = new Logger('Checkpoint');

  constructor(private supabase: SupabaseClient) {}

  async getCheckpoint(source: string): Promise<SourceCheckpoint | null> {
    const { data, error } = await this.supabase
      .from('importer_checkpoints')
      .select('*')
      .eq('source', source)
      .maybeSingle();

    if (error) {
      this.logger.error('Failed to fetch checkpoint', { source, error: error.message });
      throw error;
    }
    return data as SourceCheckpoint | null;
  }

  async saveCheckpoint(
    source: string,
    cursorValue: string | null,
    metadata: Record<string, any> = {}
  ): Promise<void> {
    const { error } = await this.supabase
      .from('importer_checkpoints')
      .upsert(
        {
          source,
          cursor_value: cursorValue,
          last_checked_at: new Date().toISOString(),
          metadata,
          updated_at: new Date().toISOString(),
        },
        { onConflict: 'source' }
      );

    if (error) {
      this.logger.error('Failed to save checkpoint', { source, error: error.message });
      throw error;
    }
    this.logger.debug('Checkpoint updated', { source, cursorValue });
  }

  async isCatalogCompleted(source: string): Promise<boolean> {
    const cp = await this.getCheckpoint(source);
    return Boolean(cp?.metadata?.catalog_completed);
  }

  async markCatalogCompleted(
    source: string,
    lastCursor: string | null,
    additionalMetadata: Record<string, any> = {}
  ): Promise<void> {
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
