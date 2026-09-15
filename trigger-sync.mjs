import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

async function run() {
  const sources = ['yaoifanclub', 'megahentai', 'universohentai', 'hentaifusion', 'yuriverso'];
  const { data: mappings } = await supabase.from('importer_work_mappings')
    .select('id, source, source_work_id, work_id, metadata, works(title)')
    .in('source', sources)
    .limit(1);
    
  if (mappings && mappings.length > 0) {
     const m = mappings[0];
     console.log(`Found mapping: ${m.source} -> ${m.source_work_id} (${m.works?.title})`);
     
     await supabase.from('work_tags').delete().eq('work_id', m.work_id);
     
     const dedupe_key = `SYNC_WORK:${m.source}:${m.source_work_id}:${Date.now()}`;
     
     const { data: job, error } = await supabase.from('importer_queue').insert({
        source: m.source,
        task_type: 'SYNC_WORK',
        dedupe_key,
        payload: {
           sourceWorkId: m.source_work_id,
           url: '',
           workId: m.work_id,
           workMappingId: m.id
        },
        status: 'QUEUED'
     }).select('id').single();
     
     console.log('Inserted SYNC_WORK job:', job?.id, error?.message || 'Success');
     
     // wait 10 seconds then check tags
     await new Promise(r => setTimeout(r, 10000));
     
     const { data: tags } = await supabase.from('work_tags').select('tags(name)').eq('work_id', m.work_id);
     console.log('Tags after sync:', tags?.map(t => t.tags?.name));
     
     const { data: jobResult } = await supabase.from('importer_queue').select('status, result').eq('id', job?.id).single();
     console.log('Job status:', jobResult?.status);
  }
}
run();
