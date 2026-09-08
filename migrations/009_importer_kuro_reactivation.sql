-- ==============================================================================
-- PROJECT NOX IMPORTER - MIGRATION 009: Kuro Reactivation
-- ==============================================================================

update public.importer_sources
set status = 'ACTIVE', enabled = true, cooldown_until = null, updated_at = now()
where id = 'kuro';
