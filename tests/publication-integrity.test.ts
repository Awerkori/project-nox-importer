import { PGlite } from '@electric-sql/pglite';
import { it } from 'vitest';import fs from 'node:fs';import assert from 'node:assert/strict';
it('enforces stored page integrity before publication', async () => {
const db=new PGlite();await db.exec(`CREATE TABLE chapters(id int,origin text,published_at timestamptz);CREATE TABLE pages(chapter_id int,position int,media_id int);CREATE TABLE media(id int,storage_ready boolean,status text,bytes int,mime text,width int,height int);CREATE TABLE importer_chapter_mappings(chapter_id int,is_page_provider bool,status text,page_count int,updated_at timestamptz);`);
await db.exec(fs.readFileSync(new URL('../migrations/20260914041000_importer_publication_integrity.sql', import.meta.url),'utf8'));
await db.exec("INSERT INTO chapters VALUES(1,'IMPORTER',null)");
await assert.rejects(db.exec("UPDATE chapters SET published_at=now()"),/INTEGRITY/);
await db.exec("INSERT INTO pages VALUES(1,1,1);INSERT INTO media VALUES(1,false,'ACTIVE',100,'image/jpeg',800,1200)");
await assert.rejects(db.exec("UPDATE chapters SET published_at=now()"),/INTEGRITY/);
await db.exec("UPDATE media SET storage_ready=true;UPDATE pages SET position=2");await assert.rejects(db.exec("UPDATE chapters SET published_at=now()"),/INTEGRITY/);
await db.exec("UPDATE pages SET position=1;INSERT INTO importer_chapter_mappings VALUES(1,true,'STAGED',2,now())");await assert.rejects(db.exec("UPDATE chapters SET published_at=now()"),/INTEGRITY/);
await db.exec("UPDATE importer_chapter_mappings SET page_count=1;UPDATE chapters SET published_at=now()");assert.ok((await db.query('SELECT published_at FROM chapters')).rows[0]['published_at']);
await db.exec("UPDATE chapters SET published_at=null;DELETE FROM importer_chapter_mappings;INSERT INTO pages VALUES(1,1,1),(1,3,1)");
await assert.rejects(db.exec("UPDATE chapters SET published_at=now()"),/INTEGRITY/);
console.log('PASS: zero pages, missing storage, position gaps, manifest mismatch rejected; complete chapter accepted');await db.close();

}, 15000);
