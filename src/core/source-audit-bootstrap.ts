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
export const KURO_RETIREMENT_RELEASE = '2026-10-18';
export const BLOCKED_SOURCE_RETIREMENT_RELEASE = '2026-10-18';

type Queryable = Pick<pg.Pool, 'query'>;

const logger = new Logger('SourceAuditBootstrap');

/**
 * These adapters passed the same catalog → work → chapter → Reader/image
 * contract as new providers, but their old importer_sources rows still carry
 * the broad sustainability cooldown used before this audit.  Keep discovery
 * off: this reconciliation only makes their verified mappings runnable.
 */
const RECOVERED_SOURCES = [
  ['manhastro', 'Manhastro', 'https://manhastro.net'],
  ['nexus', 'Nexus Mangas', 'https://www.nexusmangas.com'],
  ['nocturnesummer', 'Nocturne Summer', 'https://nocfsb.com'],
  ['tankouhentai', 'Tankou Hentai', 'https://tankouhentai.com'],
] as const;

/** Permanently retired after a full production admission-gate audit. */
export const BLOCKED_SOURCE_RETIREMENTS = [
  ['acervohentai', 'AUDIT_2026_10_JS_CHALLENGE'],
  ['amuy', 'AUDIT_2026_10_JS_CHALLENGE'],
  ['arthurscan', 'AUDIT_2026_10_JS_CHALLENGE'],
  ['geasscomics', 'AUDIT_2026_10_JS_CHALLENGE'],
  ['inkapk', 'AUDIT_2026_10_JS_CHALLENGE'],
  ['mangaonline', 'AUDIT_2026_10_JS_CHALLENGE'],
  ['mangastop', 'AUDIT_2026_10_JS_CHALLENGE'],
  ['pointzerotoons', 'AUDIT_2026_10_JS_CHALLENGE'],
  ['vegitoons', 'AUDIT_2026_10_API_BLOCK'],
  ['yaoifanclub', 'AUDIT_2026_10_JS_CHALLENGE'],
  ['yuriverso', 'AUDIT_2026_10_JS_CHALLENGE'],
] as const;

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
       ('hentaiteca', 'HentaiTeca', 'https://hentaiteca.online', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', $1::text)),
       ('imperiodabritannia', 'Sagrado Império da Britannia', 'https://api.imperiodabritannia.net/api', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', $1::text))
     ON CONFLICT (id) DO NOTHING`,
    [SOURCE_AUDIT_RELEASE],
  );

  const recovered = await pool.query(
    `WITH recovered(id, name, base_url) AS (
       VALUES ${RECOVERED_SOURCES.map((_, index) => `($${index * 3 + 2}::text, $${index * 3 + 3}::text, $${index * 3 + 4}::text)`).join(',\n              ')}
     )
     UPDATE public.importer_sources AS source
        SET name = recovered.name,
            base_url = recovered.base_url,
            enabled = true,
            status = 'ACTIVE',
            chapter_ingestion_enabled = true,
            catalog_discovery_enabled = false,
            cooldown_until = NULL,
            blocked_reason = NULL,
            blocked_details = '{}'::jsonb,
            config = COALESCE(source.config, '{}'::jsonb) || jsonb_build_object('sourceAuditActivation', $1::text),
            updated_at = now()
       FROM recovered
      WHERE source.id = recovered.id
        AND COALESCE(source.config, '{}'::jsonb)->>'sourceAuditActivation' IS DISTINCT FROM $1::text`,
    [SOURCE_AUDIT_RELEASE, ...RECOVERED_SOURCES.flat()],
  );

  // Kuro is permanently retired from the importer.  This guard runs on every
  // boot until the retirement marker is present, so an old row cannot be
  // re-enabled by a stale configuration or a future scheduler change.
  const kuro = await pool.query(
    `UPDATE public.importer_sources
       SET enabled = false,
           status = 'DISABLED',
           chapter_ingestion_enabled = false,
           catalog_discovery_enabled = false,
           cooldown_until = NULL,
           blocked_reason = 'Retired: recurring Cloudflare authentication challenge has no sustainable importer route',
           blocked_details = jsonb_build_object(
             'sourceRetirement', $1::text,
             'reason', 'KURO_EXTERNAL_CLOUDFLARE_LOGIN_CHALLENGE'
           ),
           config = COALESCE(config, '{}'::jsonb) || jsonb_build_object(
             'sourceRetirement', $1::text,
             'sourceRetirementReason', 'KURO_EXTERNAL_CLOUDFLARE_LOGIN_CHALLENGE'
           ),
           updated_at = now()
     WHERE id = 'kuro'
       AND COALESCE(config, '{}'::jsonb)->>'sourceRetirement' IS DISTINCT FROM $1::text`,
    [KURO_RETIREMENT_RELEASE],
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

  // These providers failed their actual catalog/API gate from the production
  // datacenter (ten JS challenges and one API block). Keep the durable marker
  // on every boot so stale rows, bootstrap scripts, and restarts cannot make
  // a retired provider schedulable again. The archival runner removes only
  // operational records later; public works, chapters, pages and media stay.
  const blocked = await pool.query(
    `WITH retired(id, reason) AS (
       VALUES ${BLOCKED_SOURCE_RETIREMENTS.map((_, index) => `($${index * 2 + 2}::text, $${index * 2 + 3}::text)`).join(',\n              ')}
     )
     UPDATE public.importer_sources AS source
        SET enabled = false,
            status = 'DISABLED',
            chapter_ingestion_enabled = false,
            catalog_discovery_enabled = false,
            cooldown_until = NULL,
            blocked_reason = 'Retired after production source audit: ' || retired.reason,
            blocked_details = jsonb_build_object(
              'sourceRetirement', $1::text,
              'reason', retired.reason,
              'audit', 'production-admission-gate'
            ),
            config = COALESCE(source.config, '{}'::jsonb) || jsonb_build_object(
              'sourceRetirement', $1::text,
              'sourceRetirementReason', retired.reason
            ),
            updated_at = now()
       FROM retired
      WHERE source.id = retired.id
        AND (
          COALESCE(source.config, '{}'::jsonb)->>'sourceRetirement' IS DISTINCT FROM $1::text
          OR source.enabled IS DISTINCT FROM false
          OR source.status IS DISTINCT FROM 'DISABLED'
          OR source.chapter_ingestion_enabled IS DISTINCT FROM false
          OR source.catalog_discovery_enabled IS DISTINCT FROM false
        )`,
    [BLOCKED_SOURCE_RETIREMENT_RELEASE, ...BLOCKED_SOURCE_RETIREMENTS.flat()],
  );

  logger.info('Reconciled audited source admission state', {
    inserted: inserted.rowCount ?? 0,
    recovered: recovered.rowCount ?? 0,
    kuroRetired: kuro.rowCount ?? 0,
    retired: retired.rowCount ?? 0,
    blockedRetired: blocked.rowCount ?? 0,
  });
}
