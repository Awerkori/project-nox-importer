import type { SupabaseClient } from '@supabase/supabase-js';
import { StorageProvider } from './provider.js';
export interface ImageInfo {
    mime: 'image/png' | 'image/jpeg' | 'image/webp' | 'image/gif' | 'image/avif';
    width: number;
    height: number;
}
/**
 * Inspect image bytes directly in binary with bounds and integrity checks.
 * Exact equivalent of Project Nox Manga inspectImage.
 */
export declare function inspectImage(a: Uint8Array): ImageInfo;
export declare function calculateSha256(bytes: Uint8Array): string;
export interface StoredMediaResult {
    mediaId: string;
    reused: boolean;
    width: number;
    height: number;
    bytes: number;
    mime: string;
}
/**
 * Process a single image:
 * 1. Validate binary structure & dimensions.
 * 2. Calculate SHA-256.
 * 3. Check public.media for existing hash (deduplication).
 * 4. If not found, upload via StorageProvider and insert into public.media.
 */
export declare function processAndStoreMedia(supabase: SupabaseClient, storage: StorageProvider, bytes: Uint8Array, userId: string, purpose?: string, chapterId?: string): Promise<StoredMediaResult>;
