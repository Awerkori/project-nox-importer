import dotenv from 'dotenv';
dotenv.config();
import { createClient } from '@supabase/supabase-js';

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function main() {
  const { data, error } = await sb.from('importer_queue').insert({
    task_type: 'IMPORT_CHAPTER',
    source: 'mangaonlinetv',
    priority: 100,
    dedupe_key: 'mangaonlinetv:chapter:https://mangaonline.tv/manga/one-piece/capitulo-51/-repair2',
    status: 'QUEUED',
    payload: {
      workId: 'e1b4bce1-5fbd-4fdf-8930-ca4afc26771d',
      chapterTitle: 'Capítulo 51',
      readerRepair: true,
      sourceWorkId: 'one-piece',
      chapterNumber: 51,
      workMappingId: '130caa23-477b-4470-b2bc-a3fbe6577e63',
      staffRequested: false,
      sourceChapterId: 'https://mangaonline.tv/manga/one-piece/capitulo-51/',
      expectedPageCount: null
    }
  }).select('id');
  if (error) console.error(error);
  else console.log("Enqueued new repair job:", data);
}
main();
