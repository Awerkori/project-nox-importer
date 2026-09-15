import fs from 'fs';
let content = fs.readFileSync('/home/awerkori/.Projects/project-nox-importer/src/core/deduplication.ts', 'utf8');

const regex = /try\s*\{\s*const tagRes = await this\.supabase\.from\('tags'\)\.select\('id, name, slug'\);[\s\S]*?\} catch \{\s*\/\/ Safe non-blocking\s*\}/m;

const newInner = `try {
      const tagRes = await this.supabase.from('tags').select('id, name, slug');
      const allTags = tagRes?.data || [];

      const tagLookup = new Map<string, string>();
      for (const t of allTags) {
        if (t.name) tagLookup.set(t.name.trim().toLowerCase(), t.id);
        if (t.slug) tagLookup.set(t.slug.trim().toLowerCase(), t.id);
      }

      const targetTagIds = new Set<string>();

      if (isAdult) {
        const adultTag = tagLookup.get('adulto') || tagLookup.get('18') || tagLookup.get('+18');
        if (adultTag) targetTagIds.add(adultTag);

        const effectiveKind = (kind || candidate.kind || '').toUpperCase();
        const hasManhwaGenre = (candidate.genres || []).some((g) => /manhwa|pornhwa/i.test(g));
        if (effectiveKind === 'MANHWA' || hasManhwaGenre) {
          const pornhwaTag = tagLookup.get('pornhwa');
          if (pornhwaTag) targetTagIds.add(pornhwaTag);
        }
      }
      
      const desiredTags = new Set<string>();
      
      if (source) {
         const def = this.getProviderDefaultTags(source);
         for (const d of def) desiredTags.add(d);
      }

      if (Array.isArray(candidate.genres)) {
        for (const genre of candidate.genres) {
           if (!genre || this.isGarbageTag(genre)) continue;
           desiredTags.add(this.normalizeTagName(genre));
        }
      }
      
      // Auto-create missing tags safely
      for (const tName of desiredTags) {
         const lower = tName.toLowerCase();
         const tSlug = this.sanitizeSlug(lower);
         
         let tagId = tagLookup.get(lower) || tagLookup.get(tSlug);
         if (!tagId) {
            // Attempt to create it safely (idempotent due to unique constraint on slug/name)
            const { data: newTag, error: createErr } = await this.supabase.from('tags').upsert({
               name: tName,
               slug: tSlug,
               kind: 'TAG'
            }, { onConflict: 'slug' }).select('id').maybeSingle();
            
            if (newTag?.id) {
               tagId = newTag.id;
               // Add to lookup for same run
               tagLookup.set(lower, tagId);
               tagLookup.set(tSlug, tagId);
            }
         }
         
         if (tagId) {
            targetTagIds.add(tagId);
         }
      }

      if (targetTagIds.size > 0) {
        const rows = Array.from(targetTagIds).map((tagId) => ({
          work_id: workId,
          tag_id: tagId,
          system_generated: true,
        }));
        await this.supabase.from('work_tags').upsert(rows, { onConflict: 'work_id,tag_id' });
      }
    } catch (err: any) {
      this.logger?.warn?.('Safe non-blocking error in syncWorkTags', { error: err.message });
    }`;

content = content.replace(regex, newInner);

fs.writeFileSync('/home/awerkori/.Projects/project-nox-importer/src/core/deduplication.ts', content);
console.log('Patch 2 applied');
