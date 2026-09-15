import { createClient } from '@supabase/supabase-js';
import { readFileSync, writeFileSync } from 'fs';
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
  console.log('Fetching all works...');
  let works = [];
  let page = 0;
  while (true) {
    const { data, error } = await supabase.from('works').select('id, title, slug, aliases, synopsis, author, kind').range(page * 1000, (page + 1) * 1000 - 1);
    if (error) { console.error('Error', error); break; }
    if (data.length === 0) break;
    works.push(...data);
    page++;
    console.log(`Fetched ${works.length} works...`);
  }
  
  console.log(`Found ${works.length} works. Grouping by normalized titles...`);
  
  const titleMap = new Map();
  const workMap = new Map();
  
  for (const w of works) {
    workMap.set(w.id, w);
    const titles = new Set([w.title, ...(w.aliases || [])].filter(Boolean).map(t => sanitizeSlug(t)));
    for (const t of titles) {
      if (!titleMap.has(t)) titleMap.set(t, []);
      titleMap.get(t).push(w.id);
    }
  }
  
  const processedGroups = new Set();
  const duplicateGroups = [];
  
  for (const [title, workIds] of titleMap.entries()) {
    if (workIds.length > 1) {
      const uniqueIds = Array.from(new Set(workIds));
      if (uniqueIds.length > 1) {
        const groupKey = uniqueIds.sort().join('|');
        if (!processedGroups.has(groupKey)) {
          processedGroups.add(groupKey);
          const groupWorks = uniqueIds.map(id => workMap.get(id));
          
          let confidence = 'LOW_CONFIDENCE';
          const authors = new Set(groupWorks.map(w => sanitizeSlug(w.author)).filter(Boolean));
          if (authors.size === 1 && groupWorks.length > 1 && groupWorks.every(w => w.author)) {
             confidence = 'HIGH_CONFIDENCE';
          } else if (title.length > 5) {
             confidence = 'MEDIUM_CONFIDENCE';
          }
          
          duplicateGroups.push({
            groupKey,
            confidence,
            shared_title: title,
            works: groupWorks.map(w => ({
              id: w.id,
              title: w.title,
              author: w.author,
              aliases: w.aliases,
            }))
          });
        }
      }
    }
  }
  
  console.log(`Found ${duplicateGroups.length} candidate duplicate groups.`);
  
  writeFileSync('duplicates-report-all.json', JSON.stringify({
    stats: {
       total_works: works.length,
       duplicate_groups: duplicateGroups.length,
    },
    groups: duplicateGroups
  }, null, 2));
  console.log('Report saved to duplicates-report-all.json');
}

run();
