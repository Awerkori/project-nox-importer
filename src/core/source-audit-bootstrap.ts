import type pg from 'pg';
import { Logger } from './logger.js';

/**
 * One-time, idempotent production reconciliation for the 2026-10 source
 * audit.  Registry membership alone is intentionally not enough to schedule
 * a provider: Lite also reads importer_sources.  Keeping this narrow list
 * here prevents a validated provider from being present only in code while
 * avoiding a broad "enable every registered adapter" action on each boot.
 */
export const SOURCE_AUDIT_RELEASE = '2026-10-17';

type Queryable = Pick<pg.Pool, 'query'>;

const logger = new Logger('SourceAuditBootstrap');

export async function reconcileAuditedSources(pool: Queryable): Promise<void> {
  // New or previously unregistered providers.  Catalog discovery stays off:
  // they serve only verified mappings/onboarding until a separate discovery
  // capacity decision is made.
  const inserted = await pool.query(
    `INSERT INTO public.importer_sources (
       id, name, base_url, enabled, status, rate_limit_per_second,
       sync_interval_minutes, chapter_ingestion_enabled,
       catalog_discovery_enabled, config
     ) VALUES
       ('brscans', 'BR Scans', 'https://brscans.com', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', $1::text)),
       ('mangastop', 'MangaStop', 'https://mangastop.net', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', $1::text)),
       ('hentaiteca', 'HentaiTeca', 'https://hentaiteca.online', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', $1::text)),
       ('imperiodabritannia', 'Sagrado Império da Britannia', 'https://api.imperiodabritannia.net/api', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', $1::text))
     ON CONFLICT (id) DO NOTHING`,
    [SOURCE_AUDIT_RELEASE],
  );

  // Kuro was disabled while the old _kn/X-Client-Token contract was stale.
  // The current authenticated _vx/X-Request-Ctx flow passed a full live E2E.
  // The marker makes this activation one-shot; a future runtime cooldown is
  // never erased by an ordinary restart.
  const kuro = await pool.query(
    `UPDATE public.importer_sources
       SET name = 'Kuro Mangas',
           base_url = 'https://kuromangas.com',
           enabled = true,
           status = 'ACTIVE',
           chapter_ingestion_enabled = true,
           catalog_discovery_enabled = false,
           cooldown_until = NULL,
           blocked_reason = NULL,
           blocked_details = '{}'::jsonb,
           config = COALESCE(config, '{}'::jsonb) || jsonb_build_object('sourceAuditRecovery', $1::text),
           updated_at = now()
     WHERE id = 'kuro'
       AND COALESCE(config, '{}'::jsonb)->>'sourceAuditRecovery' IS DISTINCT FROM $1::text`,
    [SOURCE_AUDIT_RELEASE],
  );

  // Preserve all historical rows and provenance, but make the retirement
  // explicit in the scheduler state as well as removing the adapters from the
  // registry.  The marker similarly avoids rewriting a future manual review.
  const retired = await pool.query(
    `UPDATE public.importer_sources
       SET enabled = false,
           status = 'DISABLED',
           chapter_ingestion_enabled = false,
           catalog_discovery_enabled = false,
           cooldown_until = NULL,
           blocked_reason = 'Retired after source audit: no sustainable public Reader route',
           blocked_details = jsonb_build_object('sourceAuditRetired', $1::text),
           config = COALESCE(config, '{}'::jsonb) || jsonb_build_object('sourceAuditRetired', $1::text),
           updated_at = now()
     WHERE id IN ('megahentai', 'osakascan')
       AND COALESCE(config, '{}'::jsonb)->>'sourceAuditRetired' IS DISTINCT FROM $1::text`,
    [SOURCE_AUDIT_RELEASE],
  );

  logger.info('Reconciled audited source admission state', {
    inserted: inserted.rowCount ?? 0,
    kuroRecovered: kuro.rowCount ?? 0,
    retired: retired.rowCount ?? 0,
  });
}
