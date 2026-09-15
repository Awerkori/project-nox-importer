import fs from 'fs';

const p = 'src/core/engine.ts';
let code = fs.readFileSync(p, 'utf8');

const target = `    let effectiveSource = job.source;
    let effectiveSourceChapterId = sourceChapterId;
    let effectiveWorkMappingId = workMappingId;
    const initialSource = job.source;`;

const replacement = `    let effectiveSource = job.source;
    let effectiveSourceChapterId = sourceChapterId;
    let effectiveWorkMappingId = workMappingId;
    const initialSource = job.source;

    // Gracefully handle missing workMappingId (e.g. from manual gap revivals)
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

code = code.replace(target, replacement);
fs.writeFileSync(p, code);
console.log('Patched engine.ts');
