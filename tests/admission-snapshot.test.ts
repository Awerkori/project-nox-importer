import { describe, expect, it } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { readFileSync } from 'node:fs';
import { AdmissionController } from '../src/core/scheduler/admission-controller.js';

describe('bounded admission snapshot', () => {
  it('keeps publication frontier per work while rotating P1 admission fairly by source', () => {
    const source = readFileSync('src/core/scheduler/admission-controller.ts', 'utf8');

    // Regression: ranking source candidates by the smallest chapter number
    // across different works permanently hid later-frontier P1 works.
    expect(source).toMatch(
      /rotation_rank,[\s\S]{0,300}frontier_rank/,
    );
    expect(source).toMatch(
      /\[P1_COHORT_ROTATED\]/,
    );
    expect(source).toMatch(
      /status = 'QUEUED' OR status = 'PAUSED_BY_STAFF' OR \(status = 'RETRY' AND next_run_at <= NOW\(\)\)/,
    );
  });

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

  it('holds P2 admission whenever a visible work still has P1 backlog, including paused window jobs', async () => {
    const state={getConfig:()=>({}),getActiveWorks:()=>[]} as any;
    const sentinel={isProtectiveStopActive:async()=>false} as any;
    const pool={query:async(sql:string)=> {
      if (sql.includes('priority >= 100')) return {rows:[{p0_cnt:'0'}]};
      if (sql.includes("AND (q.status = 'QUEUED'")) return {rows:[{status:'QUEUED'}]};
      return {rows:[]};
    }};
    const c=new AdmissionController(state,sentinel,pool);
    await expect(c.canAdmitNewWork()).resolves.toMatchObject({
      allowed:false,
      reason:'P1_BACKLOG_WAITING: existing catalog work must advance before P2 admission',
      metrics:{p1Claimable:1,p1AvailableChapters:1,p1WorksWaiting:1},
    });
  });

  it('keeps paused/retry backlog active and persists the P2-to-P1 transition after first publication', async () => {
    const active=new Map<string, any>();
    const work={workId:'00000000-0000-0000-0000-000000000011',workTitle:'Lifecycle Work',lane:'P2',state:'FILLING',primarySource:'s',admittedAt:new Date().toISOString(),lastActivityAt:new Date().toISOString(),totalChapters:8,publishedChapters:0,queuedChapters:0,inFlightChapters:0,frontierSortKey:null,criticalGapSortKey:null,criticalGapUnblockCount:0};
    active.set(work.workId,work);
    const state={getConfig:()=>({enabled:true,shadowMode:false,maxActiveBackfillWorks:1,maxActiveNewWorks:1,slidingWindowMin:4,slidingWindowSize:8}),getActiveWorks:()=>Array.from(active.values()),getActiveWork:(id:string)=>active.get(id),setActiveWork:(w:any)=>active.set(w.workId,w),removeActiveWork:(id:string)=>active.delete(id)} as any;
    const sentinel={isProtectiveStopActive:async()=>false} as any;
    const pool={query:async(sql:string,params:any[])=> {
      if (sql.includes('queued_cnt') && sql.includes('retry_cnt')) return {rows:[{work_id:work.workId,queued_cnt:'0',importing_cnt:'0',paused_cnt:'6',retry_cnt:'1',min_sort_key:'3',pub_cnt:'1',max_pub:'1',staged_cnt:'0',min_staged:null,unimported_cnt:'7',source_status:'ACTIVE',cooldown_until:null}]};
      if (sql.includes('SET priority = 75')) return {rows:[]};
      if (sql.includes("status = 'IMPORTING'")) return {rows:[{cnt:'1'}]};
      return {rows:[]};
    }};
    const c=new AdmissionController(state,sentinel,pool);
    c.setChapterCapacityProvider(()=>1);
    await c.runAdmissionCycle();
    expect(active.get(work.workId)).toMatchObject({lane:'P1',state:'FILLING',publishedChapters:1,queuedChapters:0});
  });

  it('rotates a drained P1 window instead of letting its paused backlog monopolize the cohort', async () => {
    const active = new Map<string, any>();
    const work = {workId:'00000000-0000-0000-0000-000000000012',workTitle:'Window Work',lane:'P1',state:'FILLING',primarySource:'s',admittedAt:new Date().toISOString(),lastActivityAt:new Date().toISOString(),totalChapters:12,publishedChapters:1,queuedChapters:0,inFlightChapters:0,frontierSortKey:2,criticalGapSortKey:null,criticalGapUnblockCount:0};
    active.set(work.workId, work);
    const state = {getActiveWorks:()=>Array.from(active.values()),setActiveWork:(w:any)=>active.set(w.workId,w),removeActiveWork:(id:string)=>active.delete(id)} as any;
    const pool = {query:async(sql:string) => {
      if (sql.includes('queued_cnt') && sql.includes('retry_cnt')) return {rows:[{work_id:work.workId,queued_cnt:'0',importing_cnt:'0',paused_cnt:'8',retry_cnt:'0',min_sort_key:'2',pub_cnt:'1',max_pub:'1',staged_cnt:'0',min_staged:null,unimported_cnt:'8',source_status:'ACTIVE',cooldown_until:null}]};
      return {rows:[]};
    }};
    const controller = new AdmissionController(state, {} as any, pool as any);
    await (controller as any).reconcileActiveWorks();
    expect(active.has(work.workId)).toBe(false);
  });

  it('repairs a legacy visible P2 window in a bounded, idempotent work-scoped batch', async () => {
    const db = new PGlite();
    try {
      const id = '00000000-0000-0000-0000-000000000021';
      await db.exec(`
        CREATE TABLE works (id uuid PRIMARY KEY, published boolean);
        CREATE TABLE importer_work_mappings (work_id uuid, sync_status text, updated_at timestamptz);
        CREATE TABLE importer_queue (id integer PRIMARY KEY, task_type text, status text, payload jsonb, priority integer, chapter_sort_key numeric, next_run_at timestamptz, updated_at timestamptz);
        INSERT INTO works VALUES ('${id}', true);
        INSERT INTO importer_work_mappings VALUES ('${id}', 'ACTIVE', now());
        INSERT INTO importer_queue VALUES
          (1, 'IMPORT_CHAPTER', 'QUEUED', '{"workId":"${id}"}', 50, 1, now(), now()),
          (2, 'IMPORT_CHAPTER', 'PAUSED_BY_STAFF', '{"workId":"${id}"}', 50, 2, now(), now());
      `);
      const state = { getConfig: () => ({ slidingWindowSize: 8 }) } as any;
      const sentinel = {} as any;
      const controller = new AdmissionController(state, sentinel, { query: (sql: string, params?: any[]) => db.query(sql, params) });

      await (controller as any).repairVisibleP2LifecycleBacklog();
      const { rows } = await db.query<any>(`SELECT priority FROM importer_queue ORDER BY status`);
      expect(rows.map((row) => Number(row.priority))).toEqual([75, 75]);

      const second = await (controller as any).repairVisibleP2LifecycleBacklog();
      expect(second).toBeUndefined();
    } finally {
      await db.close();
    }
  });

  it('treats visible legacy priority-50 work as P1 before P2 discovery', () => {
    const source = readFileSync('src/core/scheduler/admission-controller.ts', 'utf8');
    expect(source.indexOf('await this.repairVisibleP2LifecycleBacklog();'))
      .toBeLessThan(source.indexOf('await this.reconcileActiveWorks();'));
    expect(source).toMatch(/w\.published IS TRUE[\s\S]{0,1000}q\.priority >= 50 AND q\.priority < 75/);
    expect(source).toMatch(/q\.status = 'QUEUED'[\s\S]{0,80}q\.status = 'IMPORTING'/);
    expect(source).toMatch(/status = CASE WHEN pw\.id IS NOT NULL THEN 'QUEUED'/);
  });
});
