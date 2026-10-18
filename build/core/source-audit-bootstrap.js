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
const logger = new Logger('SourceAuditBootstrap');
/**
 * These adapters passed the same catalog → work → chapter → Reader/image
 * contract as new providers, but their old importer_sources rows still carry
 * the broad sustainability cooldown used before this audit.  Keep discovery
 * off: this reconciliation only makes their verified mappings runnable.
 */
const RECOVERED_SOURCES = [
    ['acervohentai', 'Acervo Hentai', 'https://acervohentai.com'],
    ['amuy', 'AMUY Scan', 'https://www.apenasmaisumyaoi.com'],
    ['arthurscan', 'Arthur Scan', 'https://arthurscan.xyz'],
    ['geasscomics', 'Geass Comics', 'https://geasscomics.xyz'],
    ['inkapk', 'Inkapk', 'https://inkapk.net'],
    ['mangaonline', 'Manga Online', 'https://mangaonline.love'],
    ['manhastro', 'Manhastro', 'https://manhastro.net'],
    ['nexus', 'Nexus Mangas', 'https://www.nexusmangas.com'],
    ['nocturnesummer', 'Nocturne Summer', 'https://nocfsb.com'],
    ['pointzerotoons', 'Kitsune Yako', 'https://kitsuneyako.com'],
    ['tankouhentai', 'Tankou Hentai', 'https://tankouhentai.com'],
    ['yaoifanclub', 'Yaoi Fan Club', 'https://yaoifanclub.com'],
    ['yuriverso', 'Yuri on Air', 'https://yurionair.top'],
];
export async function reconcileAuditedSources(pool) {
    // New or previously unregistered providers.  Catalog discovery stays off:
    // they serve only verified mappings/onboarding until a separate discovery
    // capacity decision is made.
    const inserted = await pool.query(`INSERT INTO public.importer_sources (
       id, name, base_url, enabled, status, rate_limit_per_second,
       sync_interval_minutes, chapter_ingestion_enabled,
       catalog_discovery_enabled, config
     ) VALUES
       ('brscans', 'BR Scans', 'https://brscans.com', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', $1::text)),
       ('mangastop', 'MangaStop', 'https://mangastop.net', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', $1::text)),
       ('hentaiteca', 'HentaiTeca', 'https://hentaiteca.online', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', $1::text)),
       ('imperiodabritannia', 'Sagrado Império da Britannia', 'https://api.imperiodabritannia.net/api', true, 'ACTIVE', 1.00, 30, true, false, jsonb_build_object('sourceAuditRelease', $1::text))
     ON CONFLICT (id) DO NOTHING`, [SOURCE_AUDIT_RELEASE]);
    const recovered = await pool.query(`WITH recovered(id, name, base_url) AS (
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
        AND COALESCE(source.config, '{}'::jsonb)->>'sourceAuditActivation' IS DISTINCT FROM $1::text`, [SOURCE_AUDIT_RELEASE, ...RECOVERED_SOURCES.flat()]);
    // Kuro is permanently retired from the importer.  This guard runs on every
    // boot until the retirement marker is present, so an old row cannot be
    // re-enabled by a stale configuration or a future scheduler change.
    const kuro = await pool.query(`UPDATE public.importer_sources
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
       AND COALESCE(config, '{}'::jsonb)->>'sourceRetirement' IS DISTINCT FROM $1::text`, [KURO_RETIREMENT_RELEASE]);
    // Preserve all historical rows and provenance, but make the retirement
    // explicit in the scheduler state as well as removing the adapters from the
    // registry.  The marker similarly avoids rewriting a future manual review.
    const retired = await pool.query(`UPDATE public.importer_sources
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
       AND COALESCE(config, '{}'::jsonb)->>'sourceAuditRetired' IS DISTINCT FROM $1::text`, [SOURCE_AUDIT_RELEASE]);
    logger.info('Reconciled audited source admission state', {
        inserted: inserted.rowCount ?? 0,
        recovered: recovered.rowCount ?? 0,
        kuroRetired: kuro.rowCount ?? 0,
        retired: retired.rowCount ?? 0,
    });
}
