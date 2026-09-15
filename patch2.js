import fs from 'fs';

const p = 'src/core/engine.ts';
let code = fs.readFileSync(p, 'utf8');

// Remove previous patch
const prevPatch = `    // Gracefully handle missing workMappingId (e.g. from manual gap revivals)
    if (!effectiveWorkMappingId) {
      const { data: wm } = await this.supabase
        .from('importer_work_mappings')
        .select('id')
        .eq('work_id', workId)
        .eq('source', job.source)
        .maybeSingle();
      if (wm?.id) {
        effectiveWorkMappingId = wm.id;
        workMappingId = wm.id; // Also patch the original variable just in case
      } else {
        this.logger.warn(\`Job \${job.id} is missing work_mapping_id and could not resolve it automatically.\`);
      }
    }`;
code = code.replace(prevPatch, '');

// Insert new patch right after payload extraction
const target2 = `    if (!sourceChapterId || !workId || chapterNumber === undefined) {
      throw new Error('Incomplete chapter import payload');
    }`;

const replacement2 = `    if (!sourceChapterId || !workId || chapterNumber === undefined) {
      throw new Error('Incomplete chapter import payload');
    }

    // Gracefully handle missing workMappingId (e.g. from manual gap revivals)
    if (!workMappingId) {
      const { data: wm } = await this.supabase
        .from('importer_work_mappings')
        .select('id')
        .eq('work_id', workId)
        .eq('source', job.source)
        .maybeSingle();
      if (wm?.id) {
        workMappingId = wm.id;
      }
    }`;

code = code.replace(target2, replacement2);
fs.writeFileSync(p, code);
console.log('Patched engine.ts before early exit');
