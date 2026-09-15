import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
import { DeduplicationEngine } from './src/core/deduplication';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env: any = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

function extractGenres(meta: any): string[] {
  if (!meta || typeof meta !== 'object') return [];
  const result = new Set<string>();
  const keys = ['genres', 'generos', 'tags', 'categoria', 'categories', 'work_genres'];
  for (const k of keys) {
    const v = meta[k];
    if (Array.isArray(v)) {
      for (const item of v) {
        if (typeof item === 'string') result.add(item);
        if (item && item.name) result.add(item.name);
      }
    } else if (typeof v === 'string') {
      const parts = v.split(/[,|/]/).map(s => s.trim()).filter(Boolean);
      for (const p of parts) result.add(p);
    }
  }
  return Array.from(result);
}

async function run() {
  const engine = new DeduplicationEngine(supabase);
  
  let page = 0;
  let totalWorksProcessed = 0;
  let worksCorrected = 0;
  
  console.log('Starting tag backfill...');
  
  while(true) {
    const { data: works, error } = await supabase.from('works').select('id, kind, content_rating, age_rating').range(page*1000, (page+1)*1000-1);
    if (!works || works.length === 0) break;
    
    // Process in parallel batches of 50
    const chunkSize = 50;
    for (let i = 0; i < works.length; i += chunkSize) {
       const chunk = works.slice(i, i + chunkSize);
       
       await Promise.all(chunk.map(async (work) => {
          const { data: mappings } = await supabase.from('importer_work_mappings').select('source, metadata').eq('work_id', work.id);
          if (!mappings || mappings.length === 0) return;
          
          const isAdult = work.content_rating === 'ADULT_18' || (work.age_rating && work.age_rating >= 18);
          
          let genresCombined = new Set<string>();
          let sources = new Set<string>();
          
          for (const mapping of mappings) {
             if (mapping.source) sources.add(mapping.source);
             const extracted = extractGenres(mapping.metadata);
             for (const g of extracted) genresCombined.add(g);
          }
          
          const allExtracted = Array.from(genresCombined);
          const { count: before } = await supabase.from('work_tags').select('*', {count: 'exact', head: true}).eq('work_id', work.id);
          
          for (const src of sources) {
             await engine.syncWorkTags(work.id, { genres: allExtracted, kind: work.kind } as any, isAdult, work.kind, src);
          }
          
          const { count: after } = await supabase.from('work_tags').select('*', {count: 'exact', head: true}).eq('work_id', work.id);
          if (after! > before!) {
             worksCorrected++;
          }
          totalWorksProcessed++;
       }));
       
       console.log(`Processed ${totalWorksProcessed} works... Corrected so far: ${worksCorrected}`);
    }
    page++;
  }
  console.log(`Finished! Total works: ${totalWorksProcessed}. Works with new tags: ${worksCorrected}`);
}
run();
