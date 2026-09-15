import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';
import { decodeHtmlEntities } from '../sources/common/html-utils.js';

export interface DeduplicationResult {
  workId: string | null;
  mappingId: string;
  status: 'EXISTING_MAPPING' | 'NEW_WORK' | 'AMBIGUOUS' | 'FAILED';
  slug: string;
  reason?: string;
}

export const ADULT_SOURCES = new Set([
  'hanamiheaven',
  'hipercool',
  'instahentai',
  'megahentai',
]);

export interface CandidateWork {
  source: string;
  sourceWorkId: string;
  title: string;
  slug: string;
  synopsis?: string;
  author?: string;
  artist?: string;
  kind?: 'MANGA' | 'MANHWA' | 'MANHUA' | 'WEBTOON' | 'PORNHWA' | 'UNKNOWN';
  status?: 'ONGOING' | 'COMPLETED' | 'HIATUS' | 'CANCELLED' | 'UNKNOWN';
  year?: number;
  ageRating?: number;
  contentRating?: 'GENERAL' | 'ADULT_18';
  coverId?: string | null;
  aliases?: string[];
  genres?: string[];
  rawMetadata?: Record<string, any>;
}


export function computeCanonicalChapterKey(chapterNumber: number | string, chapterTitle?: string): {
  normalizedNumber: number;
  sortKey: number;
  isSpecial: boolean;
  specialCategory?: 'prologue' | 'extra' | 'special' | 'side';
} {
  const num = typeof chapterNumber === 'number' ? chapterNumber : parseFloat(String(chapterNumber));
  const normalizedNumber = isNaN(num) || num < 0 ? 0 : Number(num.toFixed(4));
  const titleLower = (chapterTitle || '').toLowerCase();

  const hasSpecialKeywords = /especial|special|extra|omake|side|spin-off/i.test(titleLower);
  const hasPrologueKeywords = /pr[oó]logo|prologue/i.test(titleLower);

  const isPrologue = hasPrologueKeywords || (normalizedNumber === 0 && !hasSpecialKeywords);
  const isSpecial = hasSpecialKeywords || isPrologue;

  let specialCategory: 'prologue' | 'extra' | 'special' | 'side' | undefined;
  if (isPrologue) specialCategory = 'prologue';
  else if (/extra/i.test(titleLower)) specialCategory = 'extra';
  else if (/side/i.test(titleLower)) specialCategory = 'side';
  else if (hasSpecialKeywords) specialCategory = 'special';

  let sortKey = normalizedNumber;
  if (hasSpecialKeywords && normalizedNumber === 0) {
    sortKey = 0.0001;
  }

  return {
    normalizedNumber,
    sortKey: Number(sortKey.toFixed(4)),
    isSpecial,
    specialCategory,
  };
}

export class DeduplicationEngine {
  private logger = new Logger('Deduplication');

  constructor(private supabase: SupabaseClient) {}

  /**
   * Resolve or register a work conservatively.
   * Never blindly overwrite or perform destructive merges on fuzzy matches.
   */
  async resolveWork(candidate: CandidateWork): Promise<DeduplicationResult> {
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
    // 2. Candidate Matching & Canonical Resolution
    const incomingTitles = [title, ...(candidate.aliases || [])]
      .map(t => t.trim())
      .filter(t => t.length > 0);
      
    const cleanSlug = this.sanitizeSlug(slug || title);
    const incomingSlugs = Array.from(new Set(incomingTitles.map(t => this.sanitizeSlug(t))));
    
    if (incomingSlugs.length === 0) {
       incomingSlugs.push(cleanSlug);
       incomingTitles.push(title);
    }

    // Find candidates via slug OR aliases
    const [bySlugRes, byAliasRes] = await Promise.all([
      this.supabase.from('works').select('id, title, slug, author, aliases, synopsis, kind').in('slug', incomingSlugs),
      this.supabase.from('works').select('id, title, slug, author, aliases, synopsis, kind').overlaps('aliases', incomingTitles)
    ]);

    if (bySlugRes.error) throw bySlugRes.error;
    if (byAliasRes.error) throw byAliasRes.error;

    const candidateMap = new Map<string, any>();
    for (const w of [...(bySlugRes.data || []), ...(byAliasRes.data || [])]) {
      candidateMap.set(w.id, w);
    }
    const matchedWorks = Array.from(candidateMap.values());

    if (matchedWorks.length > 0) {
      // Check if any matched work is already claimed by the SAME source with a different ID
      const matchedWorkIds = matchedWorks.map((w) => w.id);
      const { data: claims } = await this.supabase
        .from('importer_work_mappings')
        .select('work_id, source, source_work_id')
        .in('work_id', matchedWorkIds);

      const claimedBySameSource = new Set(
        (claims || [])
          .filter((c) => c.source === source && c.source_work_id !== sourceWorkId && c.work_id)
          .map((c) => c.work_id)
      );

      let bestMatch: any = null;
      let highestScore = -1;

      for (const w of matchedWorks) {
        if (claimedBySameSource.has(w.id)) continue;

        let score = 0;
        const existingTitles = [w.title, ...(w.aliases || [])].filter(Boolean).map(t => this.sanitizeSlug(t.trim()));
        
        // Signal 1: Title/Alias intersection
        const intersection = incomingSlugs.filter(s => existingTitles.includes(s));
        if (intersection.length > 0) {
          score += 50;
          if (intersection.includes(cleanSlug) || intersection.includes(this.sanitizeSlug(w.title))) {
            score += 20; // Primary title match bonus
          }
        }

        // Signal 2: Author match
        if (candidate.author && w.author) {
          const inAuthor = this.sanitizeSlug(candidate.author);
          const exAuthor = this.sanitizeSlug(w.author);
          if (inAuthor && exAuthor && (inAuthor.includes(exAuthor) || exAuthor.includes(inAuthor))) {
            score += 30;
          }
        }

        // Signal 3: Synopsis basic similarity
        if (candidate.synopsis && w.synopsis) {
          const s1 = candidate.synopsis.toLowerCase();
          const s2 = w.synopsis.toLowerCase();
          if (s1.length > 50 && s2.length > 50) {
            const w1 = s1.split(/\s+/).slice(0, 20);
            const w2 = s2.split(/\s+/).slice(0, 20);
            const common = w1.filter(word => w2.includes(word) && word.length > 3);
            if (common.length >= 4) {
              score += 15;
            }
          }
        }

        if (candidate.kind && w.kind && candidate.kind === w.kind) score += 5;

        w._score = score;
        if (score > highestScore) {
          highestScore = score;
          bestMatch = w;
        }
      }

      const validMatches = matchedWorks.filter(w => !claimedBySameSource.has(w.id));
      let isAmbiguous = validMatches.length === 0;
      
      // High confidence threshold: >= 60 points
      if (validMatches.length > 0 && highestScore >= 60) {
        // Ensure no other match is too close (difference < 20 points)
        const closeMatches = validMatches.filter(w => w.id !== bestMatch.id && w._score >= highestScore - 20);
        if (closeMatches.length > 0) {
          isAmbiguous = true;
        }
      } else {
        isAmbiguous = true;
      }

      if (isAmbiguous || !bestMatch) {
        this.logger.warn('Ambiguous work candidate detected - flagging for review', {
          source,
          sourceWorkId,
          title,
          matchedCount: matchedWorks.length,
          bestScore: highestScore,
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
              ambiguity_reason: validMatches.length === 0
                ? 'Work already claimed by another ID from the same source'
                : 'Conflict with multiple existing works or low confidence score',
              candidates: matchedWorks.map(m => ({ id: m.id, title: m.title, score: m._score })),
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
          reason: 'Conflict or low confidence. Disambiguation required.',
        };
      }

      // Exact single match: Link this source to the canonical work_id
      const matched = bestMatch;
      
      // Merge missing incoming aliases into canonical work
      const existingAliasSlugs = new Set((matched.aliases || []).map((a: string) => this.sanitizeSlug(a)));
      const newAliases = [...(matched.aliases || [])];
      let addedAliases = false;
      for (const t of incomingTitles) {
         if (!existingAliasSlugs.has(this.sanitizeSlug(t)) && t.toLowerCase() !== matched.title.toLowerCase()) {
            newAliases.push(t);
            addedAliases = true;
         }
      }
      if (addedAliases) {
         await this.supabase.from('works').update({ aliases: newAliases }).eq('id', matched.id);
      }
      
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
      if (!check) break;
      uniqueSlug = `${cleanSlug}-${++suffix}`;
    }

    const now = new Date().toISOString();
    const isAdultSource = ADULT_SOURCES.has(source) || candidate.contentRating === 'ADULT_18';
    const contentRating = isAdultSource ? 'ADULT_18' : (candidate.contentRating || 'GENERAL');
    const ageRating = isAdultSource ? Math.max(18, candidate.ageRating ?? 18) : (candidate.ageRating ?? 12);

    const initialProv: Record<string, { source: string; updated_at: string }> = {};
    if (title) initialProv.title = { source, updated_at: now };
    if (candidate.synopsis) {
      initialProv.synopsis = { source, updated_at: now };
      initialProv.description = { source, updated_at: now };
    }
    if (candidate.author) initialProv.author = { source, updated_at: now };
    if (candidate.artist) initialProv.artist = { source, updated_at: now };
    if (candidate.kind) initialProv.kind = { source, updated_at: now };
    if (candidate.status) initialProv.status = { source, updated_at: now };
    if (candidate.year) initialProv.year = { source, updated_at: now };
    initialProv.age_rating = { source, updated_at: now };
    initialProv.content_rating = { source, updated_at: now };
    if (isAdultSource) {
      initialProv.adult_source = { source, updated_at: now };
    }
    if (candidate.coverId) initialProv.cover = { source, updated_at: now };
    if (candidate.aliases && candidate.aliases.length > 0) initialProv.aliases = { source, updated_at: now };

    const newWorkId = crypto.randomUUID();
    const { error: insertWorkErr } = await this.supabase.from('works').insert({
      id: newWorkId,
      slug: uniqueSlug,
      title: decodeHtmlEntities(title).slice(0, 200),
      aliases: (candidate.aliases || []).map((a) => decodeHtmlEntities(a)),
      synopsis: decodeHtmlEntities(candidate.synopsis || '').slice(0, 5000),
      description: decodeHtmlEntities(candidate.synopsis || '').slice(0, 10000),
      author: candidate.author?.slice(0, 100) || '',
      artist: candidate.artist?.slice(0, 100) || '',
      kind: candidate.kind || 'UNKNOWN',
      status: candidate.status || 'UNKNOWN',
      year: candidate.year && candidate.year >= 1900 && candidate.year <= 2200 ? candidate.year : null,
      age_rating: ageRating,
      content_rating: contentRating,
      published: false,
      featured: false,
      cover_id: candidate.coverId || null,
      metadata_provenance: initialProv,
    });

    if (insertWorkErr) {
      this.logger.error('Failed to create new work', { error: insertWorkErr.message });
      throw insertWorkErr;
    }

    const mappingMetadata: Record<string, any> = { ...(candidate.rawMetadata || {}) };
    if (isAdultSource) {
      mappingMetadata.adult_source = true;
      mappingMetadata.adult_source_id = source;
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
        metadata: mappingMetadata,
        last_synced_at: now,
      }, { onConflict: 'source,source_work_id' })
      .select()
      .single();

    if (mapInsertErr) throw mapInsertErr;

    // Attach canonical adult tags and upstream genres safely
    await this.syncWorkTags(newWorkId, candidate, isAdultSource, candidate.kind);

    this.logger.info('Created new work & mapping', {
      workId: newWorkId,
      slug: uniqueSlug,
      title,
      source,
      contentRating,
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
  async applyMetadataPrecedence(workId: string, candidate: CandidateWork, source: string): Promise<void> {
    const { data: work, error } = await this.supabase
      .from('works')
      .select('id, title, aliases, synopsis, description, author, artist, kind, status, year, age_rating, cover_id, metadata_provenance, content_rating')
      .eq('id', workId)
      .maybeSingle();

    if (error || !work) {
      this.logger.warn('Could not load work for metadata precedence', { workId, error: error?.message });
      return;
    }

    const prov: Record<string, { source: string; updated_at: string; actor_id?: string }> = {
      ...(work.metadata_provenance || {}),
    };

    const updates: Record<string, any> = {};
    const now = new Date().toISOString();

    const isAdultCandidate = ADULT_SOURCES.has(source) || candidate.contentRating === 'ADULT_18';
    const isCurrentlyAdult = work.content_rating === 'ADULT_18';

    const canUpdateField = (fieldName: string, candidateValue: any): boolean => {
      // 1. Never replace valid data with null/undefined/empty
      if (candidateValue === null || candidateValue === undefined || candidateValue === '') return false;
      if (Array.isArray(candidateValue) && candidateValue.length === 0) return false;

      // 2. Manual edit is strictly immutable
      if (prov[fieldName]?.source === 'manual') return false;

      // 3. Monotonic adult protection: once ADULT_18, never downgrade content_rating or age_rating
      if (fieldName === 'content_rating' && isCurrentlyAdult && candidateValue !== 'ADULT_18') return false;
      if (fieldName === 'age_rating' && isCurrentlyAdult && candidateValue < 18) return false;

      // 4. If field is empty in DB, any source can fill it
      const currentVal = (work as any)[fieldName];
      const isCurrentEmpty = currentVal === null || currentVal === undefined || currentVal === '' || (Array.isArray(currentVal) && currentVal.length === 0);
      if (isCurrentEmpty) return true;

      // 4.5 UNKNOWN should never overwrite a known value
      if ((fieldName === 'kind' || fieldName === 'status') && candidateValue === 'UNKNOWN' && !isCurrentEmpty && currentVal !== 'UNKNOWN') return false;

      // Other sources cannot overwrite populated fields
      return false;
    };

    // Adult rating promotion & monotonicity
    if (isAdultCandidate) {
      if (!isCurrentlyAdult) {
        updates.content_rating = 'ADULT_18';
        prov.content_rating = { source, updated_at: now };
      }
      if ((work.age_rating || 0) < 18) {
        updates.age_rating = 18;
        prov.age_rating = { source, updated_at: now };
      }
      prov.adult_source = { source, updated_at: now };
    }

    // Title
    if (candidate.title && canUpdateField('title', candidate.title.trim())) {
      updates.title = candidate.title.trim().slice(0, 200);
      prov.title = { source, updated_at: now };
    }

    // Aliases: merge non-destructively
    if (candidate.aliases && Array.isArray(candidate.aliases) && candidate.aliases.length > 0) {
      if (prov.aliases?.source !== 'manual') {
        const existingAliases = Array.isArray(work.aliases) ? work.aliases : [];
        const mergedAliases = Array.from(
          new Set([...existingAliases, ...candidate.aliases.map((a) => a.trim()).filter(Boolean)])
        ).slice(0, 50);

        if (mergedAliases.length > existingAliases.length || (source === 'kuro' && existingAliases.length === 0)) {
          updates.aliases = mergedAliases;
          prov.aliases = { source, updated_at: now };
        }
      }
    }

    // Synopsis & Description
    if (candidate.synopsis && candidate.synopsis.trim().length > 10 && canUpdateField('synopsis', candidate.synopsis.trim())) {
      const decodedSyn = decodeHtmlEntities(candidate.synopsis.trim());
      updates.synopsis = decodedSyn.slice(0, 5000);
      prov.synopsis = { source, updated_at: now };
      if (canUpdateField('description', candidate.synopsis.trim())) {
        updates.description = decodedSyn.slice(0, 10000);
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
    const kindVal = candidate.kind || 'UNKNOWN';
    if (canUpdateField('kind', kindVal)) {
      updates.kind = kindVal;
      prov.kind = { source, updated_at: now };
    }

    // Status
    const statusVal = candidate.status || 'UNKNOWN';
    if (canUpdateField('status', statusVal)) {
      updates.status = statusVal;
      prov.status = { source, updated_at: now };
    }

    // Year
    if (candidate.year && candidate.year >= 1900 && candidate.year <= 2200 && canUpdateField('year', candidate.year)) {
      updates.year = candidate.year;
      prov.year = { source, updated_at: now };
    }

    // Age Rating
    if (candidate.ageRating !== undefined && candidate.ageRating !== null && canUpdateField('age_rating', candidate.ageRating)) {
      const targetAge = (isCurrentlyAdult || isAdultCandidate) ? Math.max(18, candidate.ageRating) : candidate.ageRating;
      updates.age_rating = targetAge;
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
      } else {
        this.logger.info('Updated work metadata with precedence', {
          workId,
          source,
          updatedFields: Object.keys(updates),
        });
      }
    }

    // Always sync canonical tags and upstream genres safely
    await this.syncWorkTags(workId, candidate, isCurrentlyAdult || isAdultCandidate, updates.kind || work.kind, source);
  }

  /**
   * Synchronize canonical adult tags and upstream genres to public.work_tags
   */
  /**
   * Normalize and resolve a tag name to its canonical form
   */
  private normalizeTagName(raw: string): string {
    const canonicalAliases: Record<string, string> = {
      'bl': 'Yaoi',
      'boys love': 'Yaoi',
      'boy\'s love': 'Yaoi',
      'boys-love': 'Yaoi',
      'shounen ai': 'Yaoi',
      'shounen-ai': 'Yaoi',
      'shonen ai': 'Yaoi',
      
      'gl': 'Yuri',
      'girls love': 'Yuri',
      'girl\'s love': 'Yuri',
      'girls-love': 'Yuri',
      'shoujo ai': 'Yuri',
      'shoujo-ai': 'Yuri',
      'shojo ai': 'Yuri',
      
      'adult': 'Adulto',
      'adults only': 'Adulto',
      '18+': 'Adulto',
      '+18': 'Adulto',
      'mature': 'Adulto',
      
      'pornhwa': 'Pornhwa',
      'porn hwa': 'Pornhwa',
      
      'manhua': 'Manhua',
      'manga': 'Manga',
      'manhwa': 'Manhwa',
      'webtoon': 'Webtoon',
      'doujinshi': 'Doujinshi'
    };

    let cleaned = raw.trim();
    const lower = cleaned.toLowerCase();
    
    if (canonicalAliases[lower]) {
      return canonicalAliases[lower];
    }
    
    // Default capitalization (first letter upper)
    if (cleaned.length > 0) {
      cleaned = cleaned.charAt(0).toUpperCase() + cleaned.slice(1).toLowerCase();
    }
    return cleaned;
  }

  private isGarbageTag(raw: string): boolean {
    const garbage = [
      'leia no nosso site', 'atualizacao', 'atualização', 'projeto da scan',
      'completo', 'em andamento', 'em lancamento', 'em lançamento', 'cancelado', 'hiato', 'lancamento'
    ];
    const lower = raw.toLowerCase();
    return garbage.some(g => lower.includes(g));
  }

  private getProviderDefaultTags(source: string): string[] {
    const special: Record<string, string[]> = {
      'yaoifanclub': ['Yaoi'],
      'cafecomyaoi': ['Yaoi'],
      'fleurblanche': ['Yaoi'],
      'amuy': ['Yaoi'],
      'apenasumafa': ['Yaoi'],
      'pinkrosa': ['Yaoi'],
      'covenscan': ['Yaoi'],
      'borutoexplorer': ['Yaoi'],
      'megahentai': ['Hentai', 'Adulto'],
      'universohentai': ['Hentai', 'Adulto'],
      'hentaifusion': ['Hentai', 'Adulto'],
      'hentaihome': ['Hentai', 'Adulto'],
      'hentaiseason': ['Hentai', 'Adulto'],
      'hentaitokyo': ['Hentai', 'Adulto'],
      'tankouhentai': ['Hentai', 'Adulto'],
      'mundohentai': ['Hentai', 'Adulto'],
      'nhentaibr': ['Hentai', 'Adulto'],
      'instahentai': ['Hentai', 'Adulto'],
      'hotcabaretscan': ['Hentai', 'Adulto'],
      'acervohentai': ['Hentai', 'Adulto'],
      'nocturnesummer': ['Pornhwa', 'Adulto'],
      'tiamanhwa': ['Pornhwa', 'Adulto'],
      'inkapk': ['Pornhwa', 'Adulto'],
      'littletyrant': ['Pornhwa', 'Adulto'],
      'yuriverso': ['Yuri']
    };
    return special[source] || [];
  }

  async syncWorkTags(
    workId: string,
    candidate: CandidateWork,
    isAdult: boolean,
    kind?: string,
    source?: string
  ): Promise<void> {
    try {
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
               if(tagId) tagLookup.set(lower, tagId);
               if(tagId) tagLookup.set(tSlug, tagId);
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
    }
  }

  private sanitizeSlug(raw: string): string {
    const slug = raw
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '');
    return slug || 'obra';
  }
}

