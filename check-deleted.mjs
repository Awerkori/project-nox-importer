import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

const envRaw = readFileSync('/home/awerkori/.Projects/project-nox-importer/.env', 'utf8');
const env = {};
for (const line of envRaw.split('\n')) { 
  const m = line.match(/^([A-Z_]+)=(.*)$/); 
  if (m) env[m[1]] = m[2].trim(); 
}
const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

const deletedIds = [
'7b191b50-61a3-48eb-b93f-71df00486461',
'b77d6d8d-95ba-4ac4-88ac-f9acc50847f2',
'98bc306d-709a-40e0-a578-8cea39b12af3',
'f3e386c1-9d52-4bcf-bfbd-94e24ab2a3eb',
'9f6aac66-450b-43f9-8145-7de5f8d8b4ea',
'36a417e0-d474-431a-9612-d4daa674a4f5',
'f8225a4c-cb3e-4ee5-ba4d-1a1a643bc9e8',
'61f2bd9f-c27c-4c9f-82b1-dc102413bb38',
'f64dcfdf-fbfc-4d0c-8e0a-d95fd071bd3d'
];

async function run() {
  const { data, error } = await supabase.from('importer_queue').select('id').in('payload->>workId', deletedIds);
  console.log('Jobs pointing to deleted duplicates:', data?.length);
}
run();
