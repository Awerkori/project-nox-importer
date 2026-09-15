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
  return raw
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

async function run() {
  console.log('Fetching all works...');
  const { data: works, error } = await supabase.from('works').select('id, title, slug, aliases, synopsis, author, kind');
  if (error) {
    console.error('Error fetching works', error);
    return;
  }
  
  console.log(`Found ${works.length} works. Grouping by normalized titles...`);
  
  const titleMap = new Map(); // normalizedTitle -> [workId]
  const workMap = new Map();  // workId -> work object
  
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
          
          // Determine confidence
          // Check if authors match or if synopsis is similar
          let confidence = 'LOW_CONFIDENCE';
          const authors = new Set(groupWorks.map(w => sanitizeSlug(w.author)).filter(Boolean));
          if (authors.size === 1 && groupWorks.length > 1 && groupWorks.every(w => w.author)) {
             confidence = 'HIGH_CONFIDENCE';
          } else {
             // Maybe title match is strong enough for medium?
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
  
  let highConf = 0;
  let affectedJobsTotal = 0;
  let potentialAvoided = 0;
  
  for (const g of duplicateGroups) {
    if (g.confidence === 'HIGH_CONFIDENCE') highConf++;
    
    // Check jobs for these works
    const workIds = g.works.map(w => w.id);
    const { data: jobs } = await supabase.from('importer_queue')
      .select('id, task_type, payload')
      .in('task_type', ['IMPORT_CHAPTER'])
      .in('status', ['QUEUED']);
      
    // Filter jobs belonging to this group
    const groupJobs = (jobs || []).filter(j => workIds.includes(j.payload?.workId));
    affectedJobsTotal += groupJobs.length;
    
    // Very naive estimation: if multiple works have jobs for the same chapter number, they are redundant
    const chapterNumbers = new Set();
    for (const j of groupJobs) {
      const c = j.payload?.chapterNumber;
      if (c !== undefined) {
         if (chapterNumbers.has(c)) {
           potentialAvoided++;
         } else {
           chapterNumbers.add(c);
         }
      }
    }
  }
  
  console.log(`High Confidence Groups: ${highConf}`);
  console.log(`Queued IMPORT_CHAPTER jobs for these duplicate works: ${affectedJobsTotal}`);
  console.log(`Estimated duplicate downloads avoidable: ${potentialAvoided}`);
  
  writeFileSync('duplicates-report.json', JSON.stringify({
    stats: {
       total_works: works.length,
       duplicate_groups: duplicateGroups.length,
       high_confidence: highConf,
       affected_jobs: affectedJobsTotal,
       estimated_avoided: potentialAvoided
    },
    groups: duplicateGroups
  }, null, 2));
  console.log('Report saved to duplicates-report.json');
}

run();
