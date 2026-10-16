import type { SupabaseClient } from '@supabase/supabase-js';
import { Logger } from './logger.js';
import { decodeHtmlEntities } from '../sources/common/html-utils.js';
import { evaluateMetadataMatchEvidence, matchWorkCandidate, getCanonicalSynonyms } from './matching.js';
import { CANONICAL_ADULT_TAG_NAME, CANONICAL_ADULT_TAG_SLUG, canonicalAdultTagName } from './tag-normalization.js';
import { mergeAlternativeTitles } from './alternative-titles.js';

export interface DeduplicationResult {
  workId: string | null;
  mappingId: string;
  status: 'EXISTING_MAPPING' | 'NEW_WORK' | 'AMBIGUOUS' | 'FAILED';
  slug: string;
  reason?: string;
  metadataDelta?: { aliasesAdded: number; tagsAdded: number };
}

interface MetadataMergeDelta {
  aliasesAdded: number;
  tagsAdded: number;
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
  /**
   * Cover registration can cost a source request and a storage upload. It is
   * intentionally lazy so an ambiguous work or healthy canonical cover does
   * not pay for media that will be discarded.
   */
  loadCoverId?: () => Promise<string | null>;
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

export interface EditorialValidationResult {
  valid: boolean;
  reason?: string;
}

export function validateEditorialTitle(rawTitle: string): EditorialValidationResult {
  if (!rawTitle || typeof rawTitle !== 'string') {
    return { valid: false, reason: 'Title is empty or not a string' };
  }

  const clean = decodeHtmlEntities(rawTitle).trim();
  if (clean.length < 2) {
    return { valid: false, reason: `Title is too short (${clean.length} chars)` };
  }
  if (clean.length > 250) {
    return { valid: false, reason: `Title exceeds maximum length (${clean.length} chars)` };
  }

  const lower = clean.toLowerCase();

  // Authentication & session prompts
  if (/^(entrar(\s+na\s+conta)?|login|log\s*in|sign\s*in|fazer\s+login|cadastre-se|minha\s+conta|criar\s+conta|autentica[çc][ãa]o)$/i.test(lower)) {
    return { valid: false, reason: 'Title is an authentication or login prompt' };
  }

  // Cloudflare and bot challenge indicators
  if (/cloudflare|just a moment|attention required|checking your browser|ray id|turnstile|verify you are human|bot detection|ddos-guard/i.test(lower)) {
    return { valid: false, reason: 'Title contains Cloudflare or bot protection text' };
  }

  // HTTP status codes or generic server errors
  if (/^(40[0-9]|50[0-9])(\s+.*)?$/i.test(lower) || /^(erro\s*(40[0-9]|50[0-9])?|not found|forbidden|access denied|bad gateway|internal server error|page not found|p[aá]gina n[aã]o encontrada)$/i.test(lower)) {
    return { valid: false, reason: 'Title is an HTTP error status code or server error message' };
  }

  // Generic placeholder / dummy titles
  if (/^(untitled|sem t[ií]tulo|unknown title|undefined|null|nan)$/i.test(lower)) {
    return { valid: false, reason: 'Title is a placeholder or undefined value' };
  }

  return { valid: true };
}

export class DeduplicationEngine {
  private logger = new Logger('Deduplication');
  // Tags are a small, shared catalog. Reloading the entire catalog for every
  // work made metadata sync needlessly DB-heavy. The short TTL keeps manual
  // curation visible quickly while making the cache bounded by catalog size.
  private tagCatalog: Map<string, string> | null = null;
  private tagCatalogExpiresAt = 0;
  private tagCatalogFlight: Promise<Map<string, string>> | null = null;
  private readonly tagCatalogTtlMs = 60_000;

  constructor(private supabase: SupabaseClient) {}

  /** Merge a work whose exact source mapping and canonical work ID are already loaded. */
  async mergeKnownMappedWork(candidate: CandidateWork, workId: string, source: string): Promise<DeduplicationResult> {
    const validation = validateEditorialTitle(candidate.title);
    if (!validation.valid) {
      return {
        workId: null,
        mappingId: '',
        status: 'FAILED',
        slug: this.sanitizeSlug(candidate.slug || candidate.title || 'invalid-title'),
        reason: `INVALID_EDITORIAL_TITLE: ${validation.reason}`,
      };
    }

    const metadataDelta = await this.applyMetadataPrecedence(workId, candidate, source);
    return { workId, mappingId: '', status: 'EXISTING_MAPPING', slug: this.sanitizeSlug(candidate.slug || candidate.title), metadataDelta };
  }

  /**
   * Persist a mapping only after its caller has independently established a
   * high-confidence cross-provider identity match.  This is deliberately
   * narrower than resolveWork(): a complementary-source probe must never
   * create a second canonical work merely because a remote search result
   * changed between its summary and detail responses.
   */
  async attachVerifiedMapping(candidate: CandidateWork, workId: string, source: string): Promise<DeduplicationResult> {
    const validation = validateEditorialTitle(candidate.title);
    if (!validation.valid) {
      return {
        workId: null,
        mappingId: '',
        status: 'FAILED',
        slug: this.sanitizeSlug(candidate.slug || candidate.title || 'invalid-title'),
        reason: `INVALID_EDITORIAL_TITLE: ${validation.reason}`,
      };
    }

    const { data: existingMapping, error: existingError } = await this.supabase
      .from('importer_work_mappings')
      .select('id, work_id')
      .eq('source', source)
      .eq('source_work_id', candidate.sourceWorkId)
      .maybeSingle();
    if (existingError) throw new Error(`MAPPING_LOOKUP_FAILED:${existingError.message}`);
    if (existingMapping?.work_id && existingMapping.work_id !== workId) {
      return {
        workId: null,
        mappingId: String(existingMapping.id || ''),
        status: 'AMBIGUOUS',
        slug: this.sanitizeSlug(candidate.slug || candidate.title),
        reason: 'CROSS_SOURCE_MAPPING_OWNED_BY_ANOTHER_WORK',
      };
    }

    const now = new Date().toISOString();
    const { data: mapping, error: mappingError } = await this.supabase
      .from('importer_work_mappings')
      .upsert({
        source,
        source_work_id: candidate.sourceWorkId,
        work_id: workId,
        source_slug: this.sanitizeSlug(candidate.slug || candidate.title),
        source_title: candidate.title,
        sync_status: 'SYNCED',
        metadata: candidate.rawMetadata || {},
        last_synced_at: now,
      }, { onConflict: 'source,source_work_id' })
      .select()
      .single();
    if (mappingError) throw new Error(`MAPPING_ATTACH_FAILED:${mappingError.message}`);

    const metadataDelta = await this.applyMetadataPrecedence(workId, candidate, source);
    return {
      workId,
      mappingId: String(mapping?.id || existingMapping?.id || ''),
      status: 'EXISTING_MAPPING',
      slug: this.sanitizeSlug(candidate.slug || candidate.title),
      metadataDelta,
    };
  }

  private async resolveCandidateCover(candidate: CandidateWork): Promise<string | null> {
    if (candidate.coverId) return candidate.coverId;
    if (!candidate.loadCoverId) return null;

    const coverId = await candidate.loadCoverId();
    candidate.coverId = coverId;
    return coverId;
  }

  /**
   * Resolve or register a work conservatively.
   * Never blindly overwrite or perform destructive merges on fuzzy matches.
   */
  async resolveWork(candidate: CandidateWork): Promise<DeduplicationResult> {
    const { source, sourceWorkId, title, slug } = candidate;

    // Validate title against non-editorial garbage (Cloudflare, login walls, HTTP errors)
    const titleVal = validateEditorialTitle(title);
    if (!titleVal.valid) {
      this.logger.warn('Rejected candidate work due to non-editorial title', {
        source,
        sourceWorkId,
        title,
        reason: titleVal.reason,
      });
      return {
        workId: null,
        mappingId: '',
        status: 'FAILED',
        slug: this.sanitizeSlug(slug || title || 'invalid-title'),
        reason: `INVALID_EDITORIAL_TITLE: ${titleVal.reason}`,
      };
    }

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
      // Reconsider only the legacy ambiguity produced by a same-source
      // claim. Earlier versions treated a coarse creator fallback (often a
      // scan group) as an identity collision. The current matcher preserves
      // real title/slug/alias collisions, while allowing those false
      // positives to be resolved on their next normal sync.
      const retryableSameSourceClaimAmbiguity =
        existingMapping.sync_status === 'AMBIGUOUS' &&
        existingMapping.metadata?.ambiguity_reason === 'Work already claimed by another ID from the same source';

      if (existingMapping.sync_status === 'AMBIGUOUS' && !retryableSameSourceClaimAmbiguity) {
        return {
          workId: existingMapping.work_id,
          mappingId: existingMapping.id,
          status: 'AMBIGUOUS',
          slug: existingMapping.source_slug,
          reason: 'Marked as AMBIGUOUS in mapping table pending editorial review',
        };
      }

      if (retryableSameSourceClaimAmbiguity) {
        this.logger.info('Reconsidering same-source claim ambiguity with identity-scoped matching', {
          source,
          sourceWorkId,
        });
      }

      if (existingMapping.work_id) {
        // Update last_synced_at
        await this.supabase
          .from('importer_work_mappings')
          .update({
            last_synced_at: new Date().toISOString(),
            // Keep source-refetch metadata durable as well as applying the
            // non-destructive canonical merge below. A shallow JSON merge
            // preserves unrelated historical provenance and mapping fields.
            metadata: { ...(existingMapping.metadata || {}), ...(candidate.rawMetadata || {}) },
          })
          .eq('id', existingMapping.id);

        // Apply metadata precedence on re-sync
        const metadataDelta = await this.applyMetadataPrecedence(existingMapping.work_id, candidate, source);

        return {
          workId: existingMapping.work_id,
          mappingId: existingMapping.id,
          status: 'EXISTING_MAPPING',
          slug: existingMapping.source_slug,
          metadataDelta,
        };
      }
    }
    // 2. Candidate Matching & Canonical Resolution across all sources
    const initialTitles = [title, ...(candidate.aliases || [])]
      .map(t => decodeHtmlEntities(t || '').trim())
      .filter(t => t.length > 0);
      
    // Automatically enrich with curated canonical translation synonyms
    const synonymTitles = initialTitles.flatMap(t => getCanonicalSynonyms(t));
    const rawIncomingTitles = mergeAlternativeTitles([], [...initialTitles, ...synonymTitles]);
      
    const cleanSlug = this.sanitizeSlug(slug || title);
    const incomingSlugs = Array.from(new Set([
      cleanSlug,
      ...rawIncomingTitles.map((t) => this.sanitizeSlug(t)).filter((value) => value !== 'obra'),
    ]));
    const incomingTitles = Array.from(new Set(rawIncomingTitles));
    
    if (incomingSlugs.length === 0) {
       incomingSlugs.push(cleanSlug);
       incomingTitles.push(title);
    }

    // Step 2A: Query existing mappings in importer_work_mappings by source_slug OR source_title
    const [byMapSlugRes, byMapTitleRes] = await Promise.all([
      this.supabase.from('importer_work_mappings').select('work_id, source, source_slug, source_title').in('source_slug', incomingSlugs).not('work_id', 'is', null),
      this.supabase.from('importer_work_mappings').select('work_id, source, source_slug, source_title').in('source_title', incomingTitles).not('work_id', 'is', null),
    ]);

    const mappedWorkIds = new Set<string>();
    for (const m of [...(byMapSlugRes.data || []), ...(byMapTitleRes.data || [])]) {
      if (m.work_id) mappedWorkIds.add(m.work_id);
    }

    // Step 2B: Query works table by slug, aliases, title, or mapped workIds
    const workQueries: PromiseLike<any>[] = [
      this.supabase.from('works').select('id, title, slug, author, artist, aliases, synopsis, kind, status, year').in('slug', incomingSlugs),
      this.supabase.from('works').select('id, title, slug, author, artist, aliases, synopsis, kind, status, year').overlaps('aliases', incomingTitles),
      this.supabase.from('works').select('id, title, slug, author, artist, aliases, synopsis, kind, status, year').in('title', incomingTitles),
    ];
    if (mappedWorkIds.size > 0) {
      workQueries.push(
        this.supabase.from('works').select('id, title, slug, author, artist, aliases, synopsis, kind, status, year').in('id', Array.from(mappedWorkIds))
      );
    }

    const workQueryResults = await Promise.all(workQueries);
    for (const res of workQueryResults) {
      if (res.error) throw res.error;
    }

    const candidateMap = new Map<string, any>();
    for (const res of workQueryResults) {
      for (const w of res.data || []) {
        candidateMap.set(w.id, w);
      }
    }
    // The first three queries are identity lookups (title, slug, aliases, or
    // an existing source mapping).  Keep that provenance separate from the
    // optional creator fallback below: a scan group can be listed as the
    // "author" of hundreds of unrelated works, so its result alone must not
    // turn an otherwise-new source work into a same-source collision.
    const identityCandidateWorkIds = new Set(candidateMap.keys());

    // Title/alias remains the cheap primary narrowing path. Only when it
    // yields no candidate do we use exact creator fields to surface a small
    // corroborative set for translated titles. This avoids catalog scans.
    if (candidateMap.size === 0 && (candidate.author || candidate.artist)) {
      const creatorQueries: PromiseLike<any>[] = [];
      if (candidate.author?.trim()) {
        let query: any = this.supabase.from('works').select('id, title, slug, author, artist, aliases, synopsis, kind, status, year').in('author', [candidate.author.trim()]);
        if (typeof query?.limit === 'function') query = query.limit(24);
        creatorQueries.push(query);
      }
      if (candidate.artist?.trim()) {
        let query: any = this.supabase.from('works').select('id, title, slug, author, artist, aliases, synopsis, kind, status, year').in('artist', [candidate.artist.trim()]);
        if (typeof query?.limit === 'function') query = query.limit(24);
        creatorQueries.push(query);
      }
      const creatorResults = await Promise.all(creatorQueries);
      for (const res of creatorResults) {
        if (res.error) throw res.error;
        for (const work of res.data || []) candidateMap.set(work.id, work);
      }
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
      const hasIdentityCandidateClaimedBySameSource = matchedWorks.some(
        (work) => identityCandidateWorkIds.has(work.id) && claimedBySameSource.has(work.id)
      );

      let bestMatch: any = null;
      let highestScore = -1;
      let secondScore = -1;
      const ambiguousMetadataCandidates: Array<{ work: any; score: number }> = [];

      for (const w of matchedWorks) {
        if (claimedBySameSource.has(w.id)) continue;

        // Use matchWorkCandidate with strict safety constraints (Novel vs Comic, Seasons, Spin-offs)
        const matchResult = matchWorkCandidate(
          {
            title: w.title,
            slug: w.slug,
            aliases: w.aliases || [],
            kind: w.kind,
            author: w.author,
            artist: w.artist,
            synopsis: w.synopsis,
            year: w.year,
          },
          {
            title: candidate.title,
            slug: cleanSlug,
            aliases: incomingTitles,
            kind: candidate.kind,
            author: candidate.author,
            artist: candidate.artist,
            synopsis: candidate.synopsis,
            year: candidate.year,
          }
        );

        const metadataEvidence = evaluateMetadataMatchEvidence(
          { title: w.title, kind: w.kind, author: w.author, artist: w.artist, synopsis: w.synopsis, year: w.year },
          { title: candidate.title, kind: candidate.kind, author: candidate.author, artist: candidate.artist, synopsis: candidate.synopsis, year: candidate.year }
        );

        // A title-similarity score is useful for discovery, but it is never
        // sufficient authority to attach a source work to an existing
        // canonical work.  Without independent creator + synopsis
        // corroboration, retain it for review instead of allowing every one
        // of its chapters to inherit a potentially wrong work_id.
        if (matchResult.matched && matchResult.matchMethod === 'FUZZY_HIGH' && !metadataEvidence.autoMatch) {
          ambiguousMetadataCandidates.push({ work: w, score: matchResult.confidenceScore });
          continue;
        }

        if (!matchResult.matched) {
          if (metadataEvidence.ambiguous) {
            ambiguousMetadataCandidates.push({ work: w, score: metadataEvidence.score });
          }
          if (!metadataEvidence.autoMatch) continue;
        }

        let score = matchResult.matched
          ? matchResult.confidenceScore
          : Math.max(0.9, metadataEvidence.score);

        // Additional signal: Author match bonus
        if (candidate.author && w.author) {
          const inAuthor = this.sanitizeSlug(candidate.author);
          const exAuthor = this.sanitizeSlug(w.author);
          if (inAuthor && exAuthor && (inAuthor.includes(exAuthor) || exAuthor.includes(inAuthor))) {
            score = Math.min(1.0, score + 0.05);
          }
        }

        // Additional signal: Synopsis basic overlap bonus
        if (candidate.synopsis && w.synopsis) {
          const s1 = candidate.synopsis.toLowerCase();
          const s2 = w.synopsis.toLowerCase();
          if (s1.length > 50 && s2.length > 50) {
            const w1 = s1.split(/\s+/).slice(0, 20);
            const w2 = s2.split(/\s+/).slice(0, 20);
            const common = w1.filter(word => w2.includes(word) && word.length > 3);
            if (common.length >= 4) {
              score = Math.min(1.0, score + 0.05);
            }
          }
        }

        w._score = score;
        if (score > highestScore) {
          secondScore = highestScore;
          highestScore = score;
          bestMatch = w;
        } else if (score > secondScore) {
          secondScore = score;
        }
      }

      const validMatches = matchedWorks.filter(w => !claimedBySameSource.has(w.id) && w._score !== undefined && w._score >= 0.80);
      let isAmbiguous = false;

      // High confidence threshold: >= 0.85
      if (!bestMatch || highestScore < 0.85) {
        if (ambiguousMetadataCandidates.length > 0 || hasIdentityCandidateClaimedBySameSource || (highestScore >= 0.60 && highestScore < 0.85)) {
          isAmbiguous = true;
        }
      } else {
        // If second best match is very close (< 0.10 difference), consider ambiguous
        if (validMatches.length > 1 && secondScore >= highestScore - 0.10) {
          isAmbiguous = true;
        }
      }

      if (isAmbiguous) {
        this.logger.warn('Ambiguous work candidate detected - flagging for review (ZERO works created)', {
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
              metadata_candidates: ambiguousMetadataCandidates.map(({ work, score }) => ({ id: work.id, title: work.title, score })),
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

      if (bestMatch && highestScore >= 0.85) {
        // High-confidence single match: Link this source to the canonical work_id
        const matched = bestMatch;

        // Non-destructively merge missing incoming aliases into canonical work
        // Matching may use curated synonyms as lookup hints, but persistence
        // must contain only names actually supplied by this source.  Never
        // turn an internal translation synonym into a public alias.
        const suppliedTitles = [title, ...(candidate.aliases || [])]
          .map((value) => decodeHtmlEntities(value || '').trim())
          .filter(Boolean);
        const newAliases = mergeAlternativeTitles(matched.aliases, suppliedTitles, matched.title);
        const existingAliasKeys = new Set((matched.aliases || []).map((alias: string) => this.tagLookupKey(alias)));
        const directAliasesAdded = newAliases.filter((alias) => !existingAliasKeys.has(this.tagLookupKey(alias))).length;
        const aliasesChanged = newAliases.length !== (matched.aliases || []).length
          || newAliases.some((alias, index) => alias !== matched.aliases[index]);
        if (aliasesChanged) {
          const { error: aliasUpdateError } = await this.supabase.from('works')
            .update({ aliases: newAliases }).eq('id', matched.id);
          if (aliasUpdateError) {
            throw new Error(`WORK_ALIAS_PERSIST_FAILED:${aliasUpdateError.message}`);
          }
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
        const metadataDelta = await this.applyMetadataPrecedence(matched.id, candidate, source);

        return {
          workId: matched.id,
          mappingId: insertedMapping?.id ?? '',
          status: 'EXISTING_MAPPING',
          slug: matched.slug,
          metadataDelta: {
            aliasesAdded: directAliasesAdded + metadataDelta.aliasesAdded,
            tagsAdded: metadataDelta.tagsAdded,
          },
        };
      }
    }

    // 3. No match exists anywhere -> Genuinely new candidate. Only this path
    // needs a source cover before the canonical work is created.
    await this.resolveCandidateCover(candidate);
    // Acquire transactional advisory lock to prevent race condition between concurrent workers.
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

    const pool = typeof (this.supabase as any).sql === 'function' ? (this.supabase as any) : null;
    let lockHash: number | null = null;
    if (pool) {
      try {
        const hashRes = await pool.sql(`SELECT hashtext('work_create:' || $1) as h`, [uniqueSlug]);
        if (hashRes.rows?.[0]?.h !== undefined) {
          lockHash = parseInt(String(hashRes.rows[0].h), 10);
          await pool.sql(`SELECT pg_advisory_lock($1)`, [lockHash]);
        }
      } catch (err: any) {
        this.logger.warn('Advisory lock not acquired, falling back to slug check', { error: err?.message });
      }
    }

    try {
      // Re-check after acquiring lock in case concurrent worker created it
      const { data: recheck } = await this.supabase
        .from('works')
        .select('id, title, slug, aliases')
        .eq('slug', uniqueSlug)
        .maybeSingle();

      if (recheck) {
        this.logger.info(`Work was created concurrently by another worker: ${recheck.id} (${recheck.slug}). Linking mapping.`);
        const { data: insertedMapping } = await this.supabase
          .from('importer_work_mappings')
          .upsert({
            source,
            source_work_id: sourceWorkId,
            work_id: recheck.id,
            source_slug: uniqueSlug,
            source_title: title,
            sync_status: 'SYNCED',
            metadata: candidate.rawMetadata || {},
            last_synced_at: new Date().toISOString(),
          }, { onConflict: 'source,source_work_id' })
          .select()
          .single();

        const metadataDelta = await this.applyMetadataPrecedence(recheck.id, candidate, source);
        return {
          workId: recheck.id,
          mappingId: insertedMapping?.id ?? '',
          status: 'EXISTING_MAPPING',
          slug: recheck.slug,
          metadataDelta,
        };
      }

      const now = new Date().toISOString();
      const isAdultSource = ADULT_SOURCES.has(source) || candidate.contentRating === 'ADULT_18';
      const contentRating = isAdultSource ? 'ADULT_18' : (candidate.contentRating || 'GENERAL');
      const ageRating = isAdultSource ? Math.max(18, candidate.ageRating ?? 18) : (candidate.ageRating ?? 12);

      const validKinds = new Set(['MANGA', 'MANHWA', 'MANHUA', 'WEBTOON']);
      const validStatuses = new Set(['ONGOING', 'COMPLETED', 'HIATUS', 'CANCELLED']);
      const safeKind = candidate.kind && validKinds.has(candidate.kind) ? candidate.kind : 'MANHWA';
      const safeStatus = candidate.status && validStatuses.has(candidate.status) ? candidate.status : 'ONGOING';

      const initialProv: Record<string, { source: string; updated_at: string }> = {};
      if (title) initialProv.title = { source, updated_at: now };
      if (candidate.synopsis) {
        initialProv.synopsis = { source, updated_at: now };
        initialProv.description = { source, updated_at: now };
      }
      if (candidate.author) initialProv.author = { source, updated_at: now };
      if (candidate.artist) initialProv.artist = { source, updated_at: now };
      initialProv.kind = { source, updated_at: now };
      initialProv.status = { source, updated_at: now };
      if (candidate.year) initialProv.year = { source, updated_at: now };
      initialProv.age_rating = { source, updated_at: now };
      initialProv.content_rating = { source, updated_at: now };
      if (isAdultSource) {
        initialProv.adult_source = { source, updated_at: now };
      }
      if (candidate.coverId) initialProv.cover = { source, updated_at: now };
      if (candidate.aliases && candidate.aliases.length > 0) initialProv.aliases = { source, updated_at: now };

      const newWorkId = crypto.randomUUID();
      const initialAliases = mergeAlternativeTitles([], (candidate.aliases || []).map((a) => decodeHtmlEntities(a)), title);
      const { error: insertWorkErr } = await this.supabase.from('works').insert({
        id: newWorkId,
        slug: uniqueSlug,
        title: decodeHtmlEntities(title).slice(0, 200),
        aliases: initialAliases,
        synopsis: decodeHtmlEntities(candidate.synopsis || '').slice(0, 5000),
        description: decodeHtmlEntities(candidate.synopsis || '').slice(0, 10000),
        author: candidate.author?.slice(0, 100) || '',
        artist: candidate.artist?.slice(0, 100) || '',
        kind: safeKind,
        status: safeStatus,
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
      const tagsAdded = await this.syncWorkTags(newWorkId, candidate, isAdultSource, safeKind, source);

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
        metadataDelta: { aliasesAdded: initialAliases.length, tagsAdded },
      };
    } finally {
      if (lockHash !== null && pool) {
        try {
          await pool.sql(`SELECT pg_advisory_unlock($1)`, [lockHash]);
        } catch {}
      }
    }
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
  async applyMetadataPrecedence(workId: string, candidate: CandidateWork, source: string): Promise<MetadataMergeDelta> {
    const { data: work, error } = await this.supabase
      .from('works')
      .select('id, title, aliases, synopsis, description, author, artist, kind, status, year, age_rating, cover_id, metadata_provenance, content_rating')
      .eq('id', workId)
      .maybeSingle();

    if (error || !work) {
      this.logger.warn('Could not load work for metadata precedence', { workId, error: error?.message });
      return { aliasesAdded: 0, tagsAdded: 0 };
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
      const currentVal = fieldName === 'cover' ? work.cover_id : (work as any)[fieldName];
      const isCurrentEmpty = currentVal === null || currentVal === undefined || currentVal === '' || (Array.isArray(currentVal) && currentVal.length === 0);
      if (isCurrentEmpty) return true;

      // 4.5 UNKNOWN should never overwrite a known value
      if ((fieldName === 'kind' || fieldName === 'status') && candidateValue === 'UNKNOWN' && !isCurrentEmpty && currentVal !== 'UNKNOWN') return false;

      // 5. Kuro can upgrade any non-manual field
      if (source === 'kuro') return true;

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

    let aliasesAdded = 0;
    // Aliases are an append-only union of real source titles. A manual editor
    // may protect the display title, but their existing aliases must not turn
    // into a wall that discards a native/romanized/English name supplied by a
    // later confirmed mapping.
    const sourceSuppliedTitles = [candidate.title, ...(candidate.aliases || [])]
      .filter((value): value is string => typeof value === 'string' && value.trim().length > 0);
    if (sourceSuppliedTitles.length > 0) {
      const existingAliases = Array.isArray(work.aliases) ? work.aliases : [];
      const mergedAliases = mergeAlternativeTitles(
        existingAliases,
        sourceSuppliedTitles.map((alias) => decodeHtmlEntities(alias)),
        updates.title || work.title
      );
      const existingAliasKeys = new Set(existingAliases.map((alias: string) => this.tagLookupKey(alias)));
      aliasesAdded = mergedAliases.filter((alias) => !existingAliasKeys.has(this.tagLookupKey(alias))).length;

      const aliasesChanged = mergedAliases.length !== existingAliases.length
        || mergedAliases.some((alias, index) => alias !== existingAliases[index]);
      if (aliasesChanged || (source === 'kuro' && existingAliases.length === 0)) {
        updates.aliases = mergedAliases;
        // Preserve the audit signal that an editor curated this field while
        // retaining every editor-entered value in the union above.
        if (prov.aliases?.source !== 'manual') prov.aliases = { source, updated_at: now };
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

    // Cover Precedence:
    // MANUAL/ADMIN > HEALTHY CANONICAL > NEW VALID SOURCE > FALLBACK.
    // A healthy canonical cover is always preserved, so defer the source
    // download/upload until it is actually needed by a new or unhealthy work.
    if (candidate.coverId || candidate.loadCoverId) {
      if (prov.cover?.source === 'manual') {
        this.logger.debug('Preserving manual cover for work', { workId });
      } else if (work.cover_id) {
        // Verify if existing canonical cover is healthy in media table
        const { data: existingMedia } = await this.supabase
          .from('media')
          .select('id, storage_ready, bytes, width, height')
          .eq('id', work.cover_id)
          .maybeSingle();

        const isHealthy =
          existingMedia &&
          existingMedia.storage_ready === true &&
          (existingMedia.bytes || 0) >= 1500 &&
          (existingMedia.width || 0) >= 100 &&
          (existingMedia.height || 0) >= 140;

        if (isHealthy) {
          // Preserve healthy canonical cover - do NOT overwrite
          this.logger.debug('Preserving healthy canonical cover for work', {
            workId,
            coverId: work.cover_id,
            source,
          });
        } else {
          // Existing cover is broken, corrupted or missing - repair only if
          // a valid source cover can be obtained.
          const coverId = await this.resolveCandidateCover(candidate);
          if (coverId) {
            updates.cover_id = coverId;
            prov.cover = { source, updated_at: now };
            this.logger.info('Replacing broken/unhealthy cover with valid candidate cover', {
              workId,
              oldCoverId: work.cover_id,
              newCoverId: coverId,
              source,
            });
          }
        }
      } else {
        // Work has null cover: adopt valid candidate cover
        const coverId = await this.resolveCandidateCover(candidate);
        if (coverId) {
          updates.cover_id = coverId;
          prov.cover = { source, updated_at: now };
          this.logger.info('Adopting valid cover for work with null cover', {
            workId,
            coverId,
            source,
          });
        }
      }
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
    const tagsAdded = await this.syncWorkTags(workId, candidate, isCurrentlyAdult || isAdultCandidate, updates.kind || work.kind, source);
    return { aliasesAdded, tagsAdded };
  }

  /**
   * Synchronize canonical adult tags and upstream genres to public.work_tags
   */
  /**
   * Normalize and resolve a tag name to its canonical form
   */
  private normalizeTagName(raw: string): string {
    const adultTag = canonicalAdultTagName(decodeHtmlEntities(raw));
    if (adultTag) return adultTag;

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
      
      'pornhwa': 'Pornhwa',
      'porn hwa': 'Pornhwa',

      // Provider vocabulary is commonly English while the canonical catalog
      // is Portuguese. Normalize before lookup to avoid Action/Horror/etc.
      // becoming duplicate, non-canonical tags.
      'action': 'Ação',
      'adventure': 'Aventura',
      'comedy': 'Comédia',
      'fantasy': 'Fantasia',
      'horror': 'Terror',
      'psychological': 'Psicológico',
      'reincarnation': 'Reencarnação',
      'supernatural': 'Sobrenatural',
      'slice of life': 'Slice of life',
      'sci fi': 'Sci-Fi',
      'science fiction': 'Sci-Fi',
      'martial arts': 'Artes marciais',
      'school life': 'Escolar',
      'mystery': 'Mistério',
      'thriller': 'Suspense'
    };

    // These are display canonicalizations only: each key is an exact
    // case/spacing variant of the provider-declared category.  Do not fold
    // related adult concepts together (for example NTR and Netorare remain
    // distinct) and never infer them from title or synopsis text.
    const exactSpecificTags: Record<string, string> = {
      'milf': 'MILF',
      'bdsm': 'BDSM',
      'ntr': 'NTR',
      'netorare': 'Netorare',
      'netorase': 'Netorase',
      'netori': 'Netori',
      'vanilla': 'Vanilla',
      'hentai': 'Hentai',
      'ecchi': 'Ecchi',
      'yuri': 'Yuri',
      'yaoi': 'Yaoi',
      'harem': 'Harem',
      'anal': 'Anal',
    };

    let cleaned = decodeHtmlEntities(raw).replace(/\s+/g, ' ').trim();
    const lower = cleaned.toLowerCase().replace(/[’']/g, '').replace(/[-_]+/g, ' ').replace(/\s+/g, ' ');
    
    if (canonicalAliases[lower]) {
      return canonicalAliases[lower];
    }
    if (exactSpecificTags[lower]) return exactSpecificTags[lower];
    
    // Default capitalization (first letter upper)
    if (cleaned.length > 0) {
      cleaned = cleaned.charAt(0).toUpperCase() + cleaned.slice(1).toLowerCase();
    }
    return cleaned;
  }

  private isGarbageTag(raw: string): boolean {
    const pageGarbage = [
      'leia no nosso site', 'atualizacao', 'atualização', 'projeto da scan',
    ];
    const nonGenreFields = new Set([
      'completo', 'em andamento', 'em lancamento', 'em lançamento', 'cancelado', 'hiato', 'lancamento',
      // These are work type/status fields, not genres. They must never be
      // used as a substitute when an upstream provider omitted genres.
      'manga', 'manhwa', 'manhua', 'webtoon', 'comic', 'doujinshi'
    ]);
    const lower = decodeHtmlEntities(raw).trim().toLowerCase();
    return nonGenreFields.has(lower) || pageGarbage.some((value) => lower.includes(value));
  }

  private tagLookupKey(raw: string): string {
    return decodeHtmlEntities(raw).replace(/\s+/g, ' ').trim().toLowerCase();
  }

  private async getTagCatalog(): Promise<Map<string, string>> {
    if (this.tagCatalog && Date.now() < this.tagCatalogExpiresAt) {
      return this.tagCatalog;
    }
    if (this.tagCatalogFlight) return this.tagCatalogFlight;

    this.tagCatalogFlight = (async () => {
      const { data, error } = await this.supabase.from('tags').select('id, name, slug');
      if (error) throw error;

      const lookup = new Map<string, string>();
      for (const tag of data || []) {
        if (tag.name) {
          lookup.set(this.tagLookupKey(tag.name), tag.id);
          lookup.set(this.sanitizeSlug(tag.name), tag.id);
        }
        if (tag.slug) lookup.set(this.tagLookupKey(tag.slug), tag.id);
      }
      this.tagCatalog = lookup;
      this.tagCatalogExpiresAt = Date.now() + this.tagCatalogTtlMs;
      return lookup;
    })();

    try {
      return await this.tagCatalogFlight;
    } finally {
      this.tagCatalogFlight = null;
    }
  }

  /**
   * Legacy importer versions wrote type/status vocabulary as automatic tags.
   * This removes only those machine-generated associations; staff-curated
   * associations are deliberately untouched.
   */
  async removeLegacySystemGeneratedNonGenreTags(workId: string): Promise<void> {
    const tagLookup = await this.getTagCatalog();
    const legacyTagIds = [
      'manga', 'manhwa', 'manhua', 'webtoon', 'comic', 'doujinshi',
      'completo', 'em andamento', 'hiato', 'cancelado',
    ].map((key) => tagLookup.get(key)).filter((id): id is string => Boolean(id));
    if (legacyTagIds.length === 0) return;

    const { error } = await this.supabase
      .from('work_tags')
      .delete()
      .eq('work_id', workId)
      .eq('system_generated', true)
      .in('tag_id', legacyTagIds);
    if (error) throw error;

    this.logger.debug('WORK_METADATA_TAGS_LEGACY_TYPES_REMOVED', {
      workId,
      candidateTagCount: legacyTagIds.length,
    });
  }

  async syncWorkTags(
    workId: string,
    candidate: CandidateWork,
    _isAdult: boolean,
    _kind?: string,
    source?: string
  ): Promise<number> {
    const sourceCount = (candidate.genres || []).filter((tag): tag is string =>
      typeof tag === 'string' && tag.trim().length > 0
    ).length;
    let normalizedCount = 0;
    let persistedCount = 0;
    let createdCount = 0;
    let addedAssociations = 0;
    try {
      const tagLookup = await this.getTagCatalog();

      const targetTagIds = new Set<string>();
      const desiredTags = new Set<string>();

      // Age/content classification is persisted separately on `works`. Tags
      // are only labels explicitly returned by the provider, never guesses
      // based on a source name, cover, synopsis, title, kind or rating.
      if (Array.isArray(candidate.genres)) {
        for (const genre of candidate.genres) {
           if (!genre || this.isGarbageTag(genre)) continue;
           desiredTags.add(this.normalizeTagName(genre));
        }
      }
      normalizedCount = desiredTags.size;
      
      // Create unknown labels in one request. A single metadata response can
      // contain several genres, so serial upserts needlessly multiply the
      // REST/YSQL round-trips on the enrichment hot path.
      const missingTagsBySlug = new Map<string, { name: string; slug: string }>();
      for (const tName of desiredTags) {
         const lower = this.tagLookupKey(tName);
         const tSlug = tName === CANONICAL_ADULT_TAG_NAME
           ? CANONICAL_ADULT_TAG_SLUG
           : this.sanitizeSlug(lower);
         
         let tagId = tagLookup.get(lower) || tagLookup.get(tSlug);
         if (!tagId) {
            if (!missingTagsBySlug.has(tSlug)) missingTagsBySlug.set(tSlug, { name: tName, slug: tSlug });
         }
         
         if (tagId) {
            targetTagIds.add(tagId);
         }
      }

      const missingTags = [...missingTagsBySlug.values()];
      if (missingTags.length > 0) {
        const { data: newTags, error: createErr } = await this.supabase.from('tags')
          .upsert(missingTags.map((tag) => ({ ...tag, kind: 'GENRE' })), { onConflict: 'slug' })
          .select('id,name,slug');
        if (createErr) throw createErr;
        const idsBySlug = new Map((newTags || []).map((tag: any) => [String(tag.slug), String(tag.id)]));
        for (const tag of missingTags) {
          const tagId = idsBySlug.get(tag.slug);
          if (!tagId) throw new Error(`Unable to resolve canonical tag ${tag.slug}`);
          tagLookup.set(this.tagLookupKey(tag.name), tagId);
          tagLookup.set(tag.slug, tagId);
          targetTagIds.add(tagId);
          createdCount += 1;
        }
      }

      if (targetTagIds.size > 0) {
        const rows = Array.from(targetTagIds).map((tagId) => ({
          work_id: workId,
          tag_id: tagId,
          system_generated: true,
        }));
        const { data, error } = await this.supabase.from('work_tags')
          .upsert(rows, { onConflict: 'work_id,tag_id', ignoreDuplicates: true })
          .select('tag_id');
        if (error) throw error;
        persistedCount = targetTagIds.size;
        addedAssociations = data?.length || 0;
      }
      this.logger.debug('WORK_METADATA_TAGS', {
        source,
        workId,
        sourceCount,
        normalizedCount,
        persistedCount,
        createdCount,
      });
      if (sourceCount > 0 && persistedCount === 0) {
        this.logger.warn('WORK_METADATA_TAGS_EMPTY', {
          source,
          workId,
          sourceCount,
          normalizedCount,
        });
      }
    } catch (err: any) {
      this.logger.warn('WORK_METADATA_TAGS_PERSIST_FAILED', {
        source,
        workId,
        sourceCount,
        normalizedCount,
        persistedCount,
        error: err?.message || String(err),
      });
    }
    return addedAssociations;
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
