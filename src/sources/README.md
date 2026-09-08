# Modular Source Adapters Architecture

This directory contains the source adapters for the Project Nox Importer.

## Architectural Principles
1. **Zero Engine Duplication**: Source adapters implement the `SourceAdapter` interface only. Queue management, deduplication, PostgreSQL leases, rate limiting, and Telegram media storage bridge remain centralized.
2. **Reference Implementation**: Source mappings are adapted directly from the Tachiyomi/Keiyoushi Kotlin extensions located at:
   `/home/awerkori/.Projects/Project-Nox/fonte-extensoes/src/pt/`
3. **Canonical Interface (`SourceAdapter`)**:
   - `fetchUpdatedWorks(cursor, { mode })`: Discovers works during bootstrap (historical pagination backwards) or maintenance (top-of-feed recent releases forwards).
   - `fetchWorkDetails(sourceWorkId)`: Resolves title, slug, coverUrl, synopsis, author, artist, kind, status, genres.
   - `fetchChapters(sourceWorkId)`: Returns chapter list with chapter numbers, titles, and IDs.
   - `fetchChapterPages(sourceChapterId, chapterNumber)`: Returns array of direct image URLs.

## Upcoming Adapters Pipeline
- `nexus`: Nexus Mangas (Implemented & in production)
- `kuro`: KuroMangas (`fonte-extensoes/src/pt/kuromangas`)
- `toonlivre`: Toon Livre (`fonte-extensoes/src/pt/mangalivre` / `toonbr`)
- `mangaflix`: MangaFlix (`fonte-extensoes/src/pt/mangaflix`)
- `manhastro`: Manhastro (`fonte-extensoes/src/pt/manhastro`)
