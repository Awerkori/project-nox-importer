import fs from 'fs';

const p = 'src/core/engine.ts';
let code = fs.readFileSync(p, 'utf8');

const target = `      // SAFEGUARD 1: Batch upsert into public.pages ONLY after ALL pages are verified
      if (!skipDownloadDueToExistingPages) {`;

const replacement = `      // SAFEGUARD 0: Re-check if published by another worker during download
      let { data: latePubCheck } = await this.supabase
        .from('chapters')
        .select('published_at')
        .eq('id', chapterId)
        .maybeSingle();

      if (latePubCheck?.published_at && job.payload.readerRepair !== true) {
        this.logger.info('Chapter published by concurrent worker during download, skipping upsert', { chapterId });
        await this.supabase.from('importer_chapter_mappings').upsert(
          {
            source: job.source,
            source_chapter_id: sourceChapterId,
            chapter_id: chapterId,
            work_id: workId,
            work_mapping_id: workMappingId,
            chapter_number: chapterNumber,
            chapter_sort_key: this.computeChapterSortKey(chapterNumber, chapterTitle),
            is_page_provider: false,
            status: 'COMPLETED',
            last_error: null,
          },
          { onConflict: 'source,source_chapter_id' }
        );
        return;
      }

      // SAFEGUARD 1: Batch upsert into public.pages ONLY after ALL pages are verified
      if (!skipDownloadDueToExistingPages) {`;

code = code.replace(target, replacement);
fs.writeFileSync(p, code);
console.log('Patched race condition!');
