
    // 2. Check for slug collision or exact title collision in public.works
    const cleanSlug = this.sanitizeSlug(slug || title);
    const { data: worksBySlug, error: slugErr } = await this.supabase
      .from('works')
      .select('id, title, slug, author, kind')
      .eq('slug', cleanSlug);

    if (slugErr) throw slugErr;

    const { data: worksByTitle, error: titleErr } = await this.supabase
      .from('works')
      .select('id, title, slug, author, kind')
      .ilike('title', title.trim());

    if (titleErr) throw titleErr;

    const matchedWorks = [...(worksBySlug || []), ...(worksByTitle || [])].filter(
      (w, i, arr) => arr.findIndex((x) => x.id === w.id) === i
    );

    // If matches exist, evaluate whether this is a clean single canonical work match
    if (matchedWorks.length > 0) {
      // Check if any matched work is already claimed by the SAME source with a different ID (duplicate/collision within source)
      const matchedWorkIds = matchedWorks.map((w) => w.id);
      const { data: claims } = await this.supabase
        .from('importer_work_mappings')
        .select('work_id, source, source_work_id')
        .in('work_id', matchedWorkIds);

      const isClaimedBySameSourceDifferentId = (claims || []).some(
        (c) => c.source === source && c.source_work_id !== sourceWorkId
      );

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
