/**
 * StorageProvider Interface
 *
 * Provides decoupling between the crawler/importer and specific media storage services
 * (Telegram Bot API, Cloudflare R2, Supabase Storage, S3, etc.).
 */
export interface StorageProvider {
    /**
     * Upload binary data and return a unique provider_key (e.g. Telegram file_id)
     *
     * @param bytes Validated image payload
     * @param mime Image MIME type ('image/webp' | 'image/png' | 'image/jpeg')
     * @param id Unique media ID (UUID)
     * @returns provider_key string (e.g. Telegram file_id)
     */
    upload(bytes: Uint8Array, mime: string, id: string, chapterId?: string): Promise<string>;
    /**
     * Check if storage service is operational and accessible
     */
    healthCheck(): Promise<boolean>;
    /**
     * Return canonical provider identifier for public.media ('telegram', 'supabase', etc.)
     */
    getProviderKey(): string;
    getLastBotReference?(id?: string): string;
    getLastShardId?(id?: string): string | null;
    getLastChannelId?(id?: string): string | null;
}
