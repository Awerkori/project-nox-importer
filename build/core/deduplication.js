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
                // Apply metadata precedence on re-sync
                await this.applyMetadataPrecedence(existingMapping.work_id, candidate, source);
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
        // If matches exist, evaluate whether this is a clean single canonical work match
        if (matchedWorks.length > 0) {
            // Check if any matched work is already claimed by the SAME source with a different ID (duplicate/collision within source)
            const matchedWorkIds = matchedWorks.map((w) => w.id);
            const { data: claims } = await this.supabase
                .from('importer_work_mappings')
                .select('work_id, source, source_work_id')
                .in('work_id', matchedWorkIds);
            const isClaimedBySameSourceDifferentId = (claims || []).some((c) => c.source === source && c.source_work_id !== sourceWorkId);
            if (isClaimedBySameSourceDifferentId || matchedWorks.length > 1) {
                this.logger.warn('Ambiguous work candidate detected - flagging for review', {
                    source,
                    sourceWorkId,
                    title,
                    matchedCount: matchedWorks.length,
                    isClaimedBySameSourceDifferentId,
                });
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
                        ambiguity_reason: isClaimedBySameSourceDifferentId
                            ? 'Work already claimed by another ID from the same source'
                            : 'Conflict with multiple existing works',
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
                    reason: isClaimedBySameSourceDifferentId
                        ? 'Work already claimed by another ID from the same source.'
                        : 'Conflict with multiple existing works. Disambiguation required.',
                };
            }
            // Exact single match: Link this source to the canonical work_id
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
            // Apply per-field metadata precedence (ADMIN > KURO > OTHER)
            await this.applyMetadataPrecedence(matched.id, candidate, source);
            return {
                workId: matched.id,
                mappingId: insertedMapping?.id ?? '',
                status: 'EXISTING_MAPPING',
                slug: matched.slug,
            };
        }
        // 3. No match exists anywhere -> create brand new canonical work safely
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
        const now = new Date().toISOString();
        const initialProv = {};
        if (title)
            initialProv.title = { source, updated_at: now };
        if (candidate.synopsis) {
            initialProv.synopsis = { source, updated_at: now };
            initialProv.description = { source, updated_at: now };
        }
        if (candidate.author)
            initialProv.author = { source, updated_at: now };
        if (candidate.artist)
            initialProv.artist = { source, updated_at: now };
        if (candidate.kind)
            initialProv.kind = { source, updated_at: now };
        if (candidate.status)
            initialProv.status = { source, updated_at: now };
        if (candidate.year)
            initialProv.year = { source, updated_at: now };
        if (candidate.ageRating !== undefined)
            initialProv.age_rating = { source, updated_at: now };
        if (candidate.coverId)
            initialProv.cover = { source, updated_at: now };
        if (candidate.aliases && candidate.aliases.length > 0)
            initialProv.aliases = { source, updated_at: now };
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
            published: false,
            featured: false,
            cover_id: candidate.coverId || null,
            metadata_provenance: initialProv,
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
            last_synced_at: now,
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
    /**
     * Applies field-level metadata precedence:
     * Priority: MANUAL (Admin/Editor) > KURO > OTHER SOURCES
     * Rules:
     * 1. Manual edit provenance is strictly immutable.
     * 2. Kuro upgrades non-manual fields if candidate has valid data.
     * 3. Other sources only fill empty/null fields.
     * 4. Never overwrite valid data with empty/null.
     */
    async applyMetadataPrecedence(workId, candidate, source) {
        const { data: work, error } = await this.supabase
            .from('works')
            .select('id, title, aliases, synopsis, description, author, artist, kind, status, year, age_rating, cover_id, metadata_provenance')
            .eq('id', workId)
            .maybeSingle();
        if (error || !work) {
            this.logger.warn('Could not load work for metadata precedence', { workId, error: error?.message });
            return;
        }
        const prov = {
            ...(work.metadata_provenance || {}),
        };
        const updates = {};
        const now = new Date().toISOString();
        const canUpdateField = (fieldName, candidateValue) => {
            // 1. Never replace valid data with null/undefined/empty
            if (candidateValue === null || candidateValue === undefined || candidateValue === '')
                return false;
            if (Array.isArray(candidateValue) && candidateValue.length === 0)
                return false;
            // 2. Manual edit is strictly immutable
            if (prov[fieldName]?.source === 'manual')
                return false;
            // 3. If field is empty in DB, any source can fill it
            const currentVal = work[fieldName];
            const isCurrentEmpty = currentVal === null || currentVal === undefined || currentVal === '' || (Array.isArray(currentVal) && currentVal.length === 0);
            if (isCurrentEmpty)
                return true;
            // 4. Kuro can upgrade any non-manual field
            if (source === 'kuro')
                return true;
            // Other sources cannot overwrite populated fields
            return false;
        };
        // Title
        if (candidate.title && canUpdateField('title', candidate.title.trim())) {
            updates.title = candidate.title.trim().slice(0, 200);
            prov.title = { source, updated_at: now };
        }
        // Aliases: merge non-destructively
        if (candidate.aliases && Array.isArray(candidate.aliases) && candidate.aliases.length > 0) {
            if (prov.aliases?.source !== 'manual') {
                const existingAliases = Array.isArray(work.aliases) ? work.aliases : [];
                const mergedAliases = Array.from(new Set([...existingAliases, ...candidate.aliases.map((a) => a.trim()).filter(Boolean)])).slice(0, 50);
                if (mergedAliases.length > existingAliases.length || (source === 'kuro' && existingAliases.length === 0)) {
                    updates.aliases = mergedAliases;
                    prov.aliases = { source, updated_at: now };
                }
            }
        }
        // Synopsis & Description
        if (candidate.synopsis && candidate.synopsis.trim().length > 10 && canUpdateField('synopsis', candidate.synopsis.trim())) {
            updates.synopsis = candidate.synopsis.trim().slice(0, 5000);
            prov.synopsis = { source, updated_at: now };
            if (canUpdateField('description', candidate.synopsis.trim())) {
                updates.description = candidate.synopsis.trim().slice(0, 10000);
                prov.description = { source, updated_at: now };
            }
        }
        // Author
        if (candidate.author && canUpdateField('author', candidate.author.trim())) {
            updates.author = candidate.author.trim().slice(0, 100);
            prov.author = { source, updated_at: now };
        }
        // Artist
        if (candidate.artist && canUpdateField('artist', candidate.artist.trim())) {
            updates.artist = candidate.artist.trim().slice(0, 100);
            prov.artist = { source, updated_at: now };
        }
        // Kind
        if (candidate.kind && canUpdateField('kind', candidate.kind)) {
            updates.kind = candidate.kind;
            prov.kind = { source, updated_at: now };
        }
        // Status
        if (candidate.status && canUpdateField('status', candidate.status)) {
            updates.status = candidate.status;
            prov.status = { source, updated_at: now };
        }
        // Year
        if (candidate.year && candidate.year >= 1900 && candidate.year <= 2200 && canUpdateField('year', candidate.year)) {
            updates.year = candidate.year;
            prov.year = { source, updated_at: now };
        }
        // Age Rating
        if (candidate.ageRating !== undefined && candidate.ageRating !== null && canUpdateField('age_rating', candidate.ageRating)) {
            updates.age_rating = candidate.ageRating;
            prov.age_rating = { source, updated_at: now };
        }
        // Cover
        if (candidate.coverId && canUpdateField('cover', candidate.coverId)) {
            updates.cover_id = candidate.coverId;
            prov.cover = { source, updated_at: now };
        }
        // Commit updates if any field changed
        if (Object.keys(updates).length > 0) {
            const { error: updateErr } = await this.supabase
                .from('works')
                .update({
                ...updates,
                metadata_provenance: prov,
                updated_at: now,
            })
                .eq('id', workId);
            if (updateErr) {
                this.logger.error('Failed to update work metadata with precedence', { workId, source, error: updateErr.message });
            }
            else {
                this.logger.info('Updated work metadata with precedence', {
                    workId,
                    source,
                    updatedFields: Object.keys(updates),
                });
            }
        }
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
