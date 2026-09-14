import { PGlite } from '@electric-sql/pglite';
import { it } from 'vitest';
import fs from 'node:fs';import assert from 'node:assert/strict';
it('bounds admission while reserving discovery and ignoring expired leases', async () => {
const db=new PGlite();
await db.exec(`CREATE ROLE service_role; CREATE TABLE settings(key text primary key,value text); CREATE TABLE importer_queue(task_type text,status text,lease_expires_at timestamptz);`);
await db.exec(fs.readFileSync(new URL('../migrations/20260914034000_atomic_importer_admission.sql', import.meta.url),'utf8').split('CREATE OR REPLACE FUNCTION public.importer_acquire_job')[0]);
const check=async(t=null)=>(await db.query('select importer_admission_available($1) ok',[t])).rows[0].ok;
assert.equal(await check(),true);
await db.exec("INSERT INTO importer_queue SELECT 'IMPORT_CHAPTER','IMPORTING',now()+interval '5 min' FROM generate_series(1,4)");
assert.equal(await check(),false);assert.equal(await check('IMPORT_CHAPTER'),false);assert.equal(await check('DISCOVERY'),true);
await db.exec("INSERT INTO importer_queue SELECT 'SYNC_WORK','IMPORTING',now()+interval '5 min' FROM generate_series(1,2)");
assert.equal(await check('DISCOVERY'),false);
await db.exec("UPDATE importer_queue SET lease_expires_at=now()-interval '1 min'");assert.equal(await check(),true);
await db.exec("INSERT INTO settings VALUES ('importer_admission_limits','{\"chapters\":999}'); UPDATE importer_queue SET lease_expires_at=now()+interval '5 min'; INSERT INTO importer_queue SELECT 'IMPORT_CHAPTER','IMPORTING',now()+interval '5 min' FROM generate_series(1,12)");assert.equal(await check(),false);
console.log('PASS: admission ceiling, discovery reserve, expired leases, hard maximum');await db.close();

}, 15000);
