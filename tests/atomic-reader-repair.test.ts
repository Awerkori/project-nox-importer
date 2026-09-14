import { PGlite } from '@electric-sql/pglite';
import { expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
it('atomically shrinks a repaired chapter and preserves old pages if replacement media is invalid', async () => {
 const db = new PGlite();
 const chapter='00000000-0000-4000-8000-000000000001', media='00000000-0000-4000-8000-000000000002';
 try {
  await db.exec(`CREATE ROLE service_role;CREATE TABLE chapters(id uuid,origin text);CREATE TABLE media(id uuid,storage_ready boolean,status text,bytes int,mime text,width int,height int);CREATE TABLE pages(chapter_id uuid,position int,media_id uuid,width int,height int,UNIQUE(chapter_id,position));INSERT INTO chapters VALUES('${chapter}','IMPORTER');INSERT INTO media VALUES('${media}',true,'ACTIVE',100,'image/jpeg',800,1200);INSERT INTO pages SELECT '${chapter}',n,'${media}',800,1200 FROM generate_series(1,5)n;`);
  await db.exec(readFileSync(new URL('../migrations/20260914171000_atomic_reader_repair.sql',import.meta.url),'utf8'));
  await expect(db.query('SELECT importer_replace_pages($1,$2)',[chapter,JSON.stringify([{media_id:'00000000-0000-4000-8000-000000000099'}])])).rejects.toThrow('Incomplete');
  expect((await db.query('SELECT count(*) n FROM pages')).rows[0].n).toBe(5);
  await db.query('SELECT importer_replace_pages($1,$2)',[chapter,JSON.stringify([{media_id:media},{media_id:media}])]);
  expect((await db.query('SELECT count(*) n FROM pages')).rows[0].n).toBe(2);
 } finally {await db.close();}
},15000);
