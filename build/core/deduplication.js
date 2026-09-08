import { Logger } from './logger.js';
export class DeduplicationEngine {
    supabase;
    logger = new Logger('Deduplication');
    constructor(supabase) {
        this.supabase = supabase;
    }
    /**
     * Resolve or register a work conservatively.
     * Never blindly overwrite or perform destructive merges on fuzzy matches.
     */
    async resolveWork(candidate) {
        const { source, sourceWorkId, title, slug } = candidate;
        // 1. Check existing mapping for this exact source + source_work_id
        const { data: existingMapping, error: mapErr } = await this.supabase
            .from('importer_work_mappings')
            .select('*')
            .eq('source', source)
            .eq('source_work_id', sourceWorkId)
            .maybeSingle();
        if (mapErr) {
            this.logger.error('Error querying work mappings', { error: mapErr.message, source, sourceWorkId });
            throw mapErr;
        }
        if (existingMapping) {
            if (existingMapping.sync_status === 'AMBIGUOUS') {
                return {
                    workId: existingMapping.work_id,
                    mappingId: existingMapping.id,
                    status: 'AMBIGUOUS',
                    slug: existingMapping.source_slug,
                    reason: 'Marked as AMBIGUOUS in mapping table pending editorial review',
                };
            }
            if (existingMapping.work_id) {
                // Update last_synced_at
                await this.supabase
                    .from('importer_work_mappings')
                    .update({ last_synced_at: new Date().toISOString() })
                    .eq('id', existingMapping.id);
                return {
                    workId: existingMapping.work_id,
                    mappingId: existingMapping.id,
                    status: 'EXISTING_MAPPING',
                    slug: existingMapping.source_slug,
                };
            }
        }
        // 2. Check for slug collision or exact title collision in public.works
        const cleanSlug = this.sanitizeSlug(slug || title);
        const { data: worksBySlug, error: slugErr } = await this.supabase
            .from('works')
            .select('id, title, slug, author, kind')
            .eq('slug', cleanSlug);
        if (slugErr)
            throw slugErr;
        const { data: worksByTitle, error: titleErr } = await this.supabase
            .from('works')
            .select('id, title, slug, author, kind')
            .ilike('title', title.trim());
        if (titleErr)
            throw titleErr;
        const matchedWorks = [...(worksBySlug || []), ...(worksByTitle || [])].filter((w, i, arr) => arr.findIndex((x) => x.id === w.id) === i);
        // If matches exist from an unmapped or conflicting source, flag as AMBIGUOUS
        if (matchedWorks.length > 0) {
            // Check if any matched work is already claimed by another source work
            const matchedWorkIds = matchedWorks.map((w) => w.id);
            const { data: claims } = await this.supabase
                .from('importer_work_mappings')
                .select('work_id, source, source_work_id')
                .in('work_id', matchedWorkIds);
            const isClaimedByDifferentId = (claims || []).some((c) => c.source !== source || c.source_work_id !== sourceWorkId);
            if (isClaimedByDifferentId || matchedWorks.length > 1) {
                this.logger.warn('Ambiguous work candidate detected - flagging for editorial review', {
                    source,
                    sourceWorkId,
                    title,
                    matchedCount: matchedWorks.length,
                });
                // Insert or update mapping with AMBIGUOUS
                const { data: insertedMapping } = await this.supabase
                    .from('importer_work_mappings')
                    .upsert({
                    source,
                    source_work_id: sourceWorkId,
                    work_id: null,
                    source_slug: cleanSlug,
                    source_title: title,
                    sync_status: 'AMBIGUOUS',
                    metadata: {
                        ambiguity_reason: 'Conflict with existing title or slug claimed by another source work',
                        candidates: matchedWorks,
                        raw: candidate.rawMetadata,
                    },
                    last_synced_at: new Date().toISOString(),
                }, { onConflict: 'source,source_work_id' })
                    .select()
                    .single();
                return {
                    workId: null,
                    mappingId: insertedMapping?.id ?? '',
                    status: 'AMBIGUOUS',
                    slug: cleanSlug,
                    reason: 'Conflict with existing work. Safe disambiguation required.',
                };
            }
            // Exact single match that is not claimed by another source
            const matched = matchedWorks[0];
            const { data: insertedMapping } = await this.supabase
                .from('importer_work_mappings')
                .upsert({
                source,
                source_work_id: sourceWorkId,
                work_id: matched.id,
                source_slug: cleanSlug,
                source_title: title,
                sync_status: 'SYNCED',
                metadata: candidate.rawMetadata || {},
                last_synced_at: new Date().toISOString(),
            }, { onConflict: 'source,source_work_id' })
                .select()
                .single();
            return {
                workId: matched.id,
                mappingId: insertedMapping?.id ?? '',
                status: 'EXISTING_MAPPING',
                slug: matched.slug,
            };
        }
        // 3. No match exists anywhere -> create brand new work safely
        let uniqueSlug = cleanSlug;
        let suffix = 1;
        while (true) {
            const { data: check } = await this.supabase
                .from('works')
                .select('id')
                .eq('slug', uniqueSlug)
                .maybeSingle();
            if (!check)
                break;
            uniqueSlug = `${cleanSlug}-${++suffix}`;
        }
        const newWorkId = crypto.randomUUID();
        const { error: insertWorkErr } = await this.supabase.from('works').insert({
            id: newWorkId,
            slug: uniqueSlug,
            title: title.slice(0, 200),
            aliases: candidate.aliases || [],
            synopsis: candidate.synopsis?.slice(0, 5000) || '',
            description: candidate.synopsis?.slice(0, 10000) || '',
            author: candidate.author?.slice(0, 100) || '',
            artist: candidate.artist?.slice(0, 100) || '',
            kind: candidate.kind || 'MANGA',
            status: candidate.status || 'ONGOING',
            year: candidate.year && candidate.year >= 1900 && candidate.year <= 2200 ? candidate.year : null,
            age_rating: candidate.ageRating ?? 12,
            published: false, // Default to unpublished draft until chapters are imported & verified
            featured: false,
            cover_id: candidate.coverId || null,
        });
        if (insertWorkErr) {
            this.logger.error('Failed to create new work', { error: insertWorkErr.message });
            throw insertWorkErr;
        }
        const { data: insertedMapping, error: mapInsertErr } = await this.supabase
            .from('importer_work_mappings')
            .upsert({
            source,
            source_work_id: sourceWorkId,
            work_id: newWorkId,
            source_slug: uniqueSlug,
            source_title: title,
            sync_status: 'SYNCED',
            metadata: candidate.rawMetadata || {},
            last_synced_at: new Date().toISOString(),
        }, { onConflict: 'source,source_work_id' })
            .select()
            .single();
        if (mapInsertErr)
            throw mapInsertErr;
        this.logger.info('Created new work & mapping', {
            workId: newWorkId,
            slug: uniqueSlug,
            title,
            source,
        });
        return {
            workId: newWorkId,
            mappingId: insertedMapping.id,
            status: 'NEW_WORK',
            slug: uniqueSlug,
        };
    }
    sanitizeSlug(raw) {
        const slug = raw
            .toLowerCase()
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .replace(/[^a-z0-9]+/g, '-')
            .replace(/^-+|-+$/g, '');
        return slug || 'obra';
    }
}
