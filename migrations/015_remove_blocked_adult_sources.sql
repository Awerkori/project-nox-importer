-- ==============================================================================
-- Migration: 015_remove_blocked_adult_sources.sql
-- Description: Completely remove Cloudflare-blocked adult sources
--              (acervohentai, blackoutcomics, inkapk, tiamanhwa)
--              from importer_sources and importer_queue, matching nexus_toons removal.
-- ==============================================================================

-- 1. Cancel/delete any pending or parked jobs for these 4 sources
DELETE FROM public.importer_queue
WHERE source IN ('acervohentai', 'blackoutcomics', 'inkapk', 'tiamanhwa');

-- 2. Remove the sources from importer_sources
DELETE FROM public.importer_sources
WHERE id IN ('acervohentai', 'blackoutcomics', 'inkapk', 'tiamanhwa');
