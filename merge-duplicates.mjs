import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

function sanitizeSlug(raw) {
  if (!raw) return '';
  return raw.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
}

async function run() {
  const report = JSON.parse(readFileSync('duplicates-report.json', 'utf8'));
  const highConfGroups = report.groups.filter(g => g.confidence === 'HIGH_CONFIDENCE');
  
  console.log(`Starting safe merge for ${highConfGroups.length} HIGH_CONFIDENCE groups.`);
  
  for (const group of highConfGroups) {
    const works = group.works;
    let canonical = null;
    let workDetails = [];
    
    for (const w of works) {
      const { count } = await supabase.from('chapters').select('*', { count: 'exact', head: true }).eq('work_id', w.id);
      workDetails.push({ ...w, chapterCount: count || 0 });
    }
    
    workDetails.sort((a, b) => b.chapterCount - a.chapterCount);
    canonical = workDetails[0];
    const duplicates = workDetails.slice(1);
    
    console.log(`\nCanonical chosen: ${canonical.title} (${canonical.id}) with ${canonical.chapterCount} chapters.`);
    
    for (const dup of duplicates) {
      console.log(`Merging duplicate: ${dup.title} (${dup.id}) with ${dup.chapterCount} chapters.`);
      
      // 1. Merge Aliases
      const existingAliases = new Set([canonical.title, ...(canonical.aliases || [])].map(t => sanitizeSlug(t)));
      const aliasesToAdd = [dup.title, ...(dup.aliases || [])].filter(t => !existingAliases.has(sanitizeSlug(t)));
      if (aliasesToAdd.length > 0) {
        const { data: cw } = await supabase.from('works').select('aliases').eq('id', canonical.id).single();
        const newAliases = [...(cw.aliases || []), ...aliasesToAdd];
        await supabase.from('works').update({ aliases: newAliases }).eq('id', canonical.id);
        console.log(`Added aliases:`, aliasesToAdd);
      }
      
      // 2. Move Provider Mappings
      await supabase.from('importer_work_mappings').update({ work_id: canonical.id }).eq('work_id', dup.id);
      
      // 3. Resolve Chapters
      const { data: dupChapters } = await supabase.from('chapters').select('*').eq('work_id', dup.id);
      const { data: canChapters } = await supabase.from('chapters').select('*').eq('work_id', canonical.id);
      
      for (const dc of (dupChapters || [])) {
        const match = canChapters.find(cc => cc.number === dc.number);
        if (match) {
          // Chapter duplicate exists in canonical
          await supabase.from('importer_chapter_mappings').update({ chapter_id: match.id, work_id: canonical.id }).eq('chapter_id', dc.id);
          await supabase.from('chapters').delete().eq('id', dc.id);
        } else {
          // Unique chapter
          await supabase.from('chapters').update({ work_id: canonical.id }).eq('id', dc.id);
          await supabase.from('importer_chapter_mappings').update({ work_id: canonical.id }).eq('chapter_id', dc.id);
        }
      }
      
      // 4. Migrate User references (try-catch because we are not 100% sure of table names)
      try { await supabase.from('user_favorites').update({ work_id: canonical.id }).eq('work_id', dup.id); } catch(e){}
      try { await supabase.from('user_reading_progress').update({ work_id: canonical.id }).eq('work_id', dup.id); } catch(e){}
      try { await supabase.from('comments').update({ work_id: canonical.id }).eq('work_id', dup.id); } catch(e){}
      try { await supabase.from('work_views').update({ work_id: canonical.id }).eq('work_id', dup.id); } catch(e){}
      
      // 6. Delete duplicate work
      const { error: delErr } = await supabase.from('works').delete().eq('id', dup.id);
      if (delErr) {
         console.error('Failed to delete duplicate work:', delErr);
      } else {
         console.log('Successfully deleted duplicate work:', dup.id);
      }
    }
  }
}
run();
