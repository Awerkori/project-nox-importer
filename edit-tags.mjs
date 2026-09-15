import fs from 'fs';
let content = fs.readFileSync('/home/awerkori/.Projects/project-nox-importer/src/core/deduplication.ts', 'utf8');

const oldSync = `  async syncWorkTags(
    workId: string,
    candidate: CandidateWork,
    isAdult: boolean,
    kind?: string
  ): Promise<void> {`;

const newSync = `  /**
   * Normalize and resolve a tag name to its canonical form
   */
  private normalizeTagName(raw: string): string {
    const canonicalAliases: Record<string, string> = {
      'bl': 'Yaoi',
      'boys love': 'Yaoi',
      'boy\\'s love': 'Yaoi',
      'boys-love': 'Yaoi',
      'shounen ai': 'Yaoi',
      'shounen-ai': 'Yaoi',
      'shonen ai': 'Yaoi',
      
      'gl': 'Yuri',
      'girls love': 'Yuri',
      'girl\\'s love': 'Yuri',
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
  ): Promise<void> {`;

content = content.replace(oldSync, newSync);

// Also need to pass source in applyMetadataPrecedence
content = content.replace(
  `await this.syncWorkTags(workId, candidate, isCurrentlyAdult || isAdultCandidate, updates.kind || work.kind);`,
  `await this.syncWorkTags(workId, candidate, isCurrentlyAdult || isAdultCandidate, updates.kind || work.kind, source);`
);

fs.writeFileSync('/home/awerkori/.Projects/project-nox-importer/src/core/deduplication.ts', content);
console.log('Patch 1 applied');
