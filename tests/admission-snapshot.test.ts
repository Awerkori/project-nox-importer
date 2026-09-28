import { describe, expect, it } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { readFileSync } from 'node:fs';
import { AdmissionController } from '../src/core/scheduler/admission-controller.js';

describe('bounded admission snapshot', () => {
  it('preserves per-work counts, attempt limits and frontiers with one SQL roundtrip', async () => {
    const db = new PGlite();
    try {
      await db.exec(`CREATE TABLE importer_queue (task_type text,status text,payload jsonb,attempts int,max_attempts int,chapter_sort_key numeric);
        CREATE TABLE chapters (work_id uuid,number numeric,published_at timestamptz);
        CREATE TABLE importer_chapter_mappings (work_id uuid,status text,chapter_sort_key numeric);
        CREATE TABLE importer_sources (id text PRIMARY KEY,status text,cooldown_until timestamptz);
        INSERT INTO importer_sources VALUES ('s','ACTIVE',NULL);`);
      const id='00000000-0000-0000-0000-000000000001', empty='00000000-0000-0000-0000-000000000002';
      for (const [status,attempts,key] of [['QUEUED',0,3],['QUEUED',7,2],['IMPORTING',1,4],['PAUSED_BY_STAFF',0,5],['COMPLETED',0,1]]) {
        await db.query(`INSERT INTO importer_queue VALUES ('IMPORT_CHAPTER',$1,$2, $3,7,$4)`,[status,JSON.stringify({workId:id}),attempts,key]);
      }
      await db.query(`INSERT INTO chapters VALUES ($1,1,now()),($1,2,now()),($1,3,NULL)`,[id]);
      await db.query(`INSERT INTO importer_chapter_mappings VALUES ($1,'STAGED',4),($1,'COMPLETED',1),($1,'PENDING',5)`,[id]);
      const sql=readFileSync('src/core/scheduler/admission-controller.ts','utf8').split('const snapshot = await this.runQuery(`')[1].split('`, [activeWorks')[0];
      const {rows}=await db.query<any>(sql,[[id,empty],['s','s']]);
      expect(rows.find(r=>r.work_id===id)).toMatchObject({queued_cnt:1,importing_cnt:1,paused_cnt:1,min_queued:'3',min_sort_key:'3',pub_cnt:2,max_pub:'2',staged_cnt:1,min_staged:'4',unimported_cnt:2,source_status:'ACTIVE'});
      expect(rows.find(r=>r.work_id===empty)).toMatchObject({queued_cnt:0,pub_cnt:0,unimported_cnt:0});
    } finally { await db.close(); }
  });

  it('coalesces periodic, watchdog and vacate-triggered admission until the cycle completes', async () => {
    const c=new AdmissionController({} as any,{} as any,{query:async()=>({rows:[]})});
    let finish!:()=>void, calls=0;
    (c as any).executeAdmissionCycle=()=>{calls++;return new Promise<void>(r=>{finish=r;});};
    const first=c.runAdmissionCycle(),second=c.runAdmissionCycle();
    expect(first).toBe(second); expect(calls).toBe(1);
    finish(); await first;
    const third=c.runAdmissionCycle(); expect(calls).toBe(2); finish(); await third;
  });

  it('does not admit P2 work beyond the effective chapter capacity', async () => {
    const state={getConfig:()=>({}),getActiveWorks:()=>[]} as any;
    const sentinel={isProtectiveStopActive:async()=>false} as any;
    const pool={query:async(sql:string)=> {
      if (sql.includes('priority >= 100')) return {rows:[{p0_cnt:'0'}]};
      if (sql.includes("status = 'IMPORTING'")) return {rows:[{cnt:'3'}]};
      return {rows:[]};
    }};
    const c=new AdmissionController(state,sentinel,pool);
    c.setChapterCapacityProvider(()=>3);
    await expect(c.canAdmitNewWork()).resolves.toMatchObject({
      allowed:false,
      reason:'WORKERS_FULLY_UTILIZED: 3/3 chapters in-flight',
    });
  });
});
