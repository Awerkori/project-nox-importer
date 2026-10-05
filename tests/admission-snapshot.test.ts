import { describe, expect, it } from 'vitest';
import { PGlite } from '@electric-sql/pglite';
import { readFileSync } from 'node:fs';
import { AdmissionController } from '../src/core/scheduler/admission-controller.js';
import { SOURCE_EXECUTION_ELIGIBILITY_SQL } from '../src/core/source-eligibility.js';

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
    // The hot path must stay on executable rows; paused backlog is only
    // inspected by the explicit fallback when the executable frontier is
    // insufficient. This prevents a full GROUP BY over the staff-paused
    // catalog on every admission cycle.
    expect(source).toMatch(
      /q\.status = 'QUEUED' OR \(q\.status = 'RETRY' AND q\.next_run_at <= NOW\(\)\)/,
    );
    // Source health must be applied before the GROUP BY. Otherwise a large
    // unresolved blocked-source backlog is still aggregated on every cycle
    // and competes with claims on the bounded YSQL pool.
    const periodicAdmissionStart = source.indexOf('const loadP1Candidates =');
    const periodicAdmissionSql = source.slice(periodicAdmissionStart, periodicAdmissionStart + 10000);
    expect(periodicAdmissionSql).toMatch(
      /JOIN importer_sources s ON s\.id = q\.source[\s\S]{0,800}SOURCE_EXECUTION_ELIGIBILITY_SQL[\s\S]{0,700}GROUP BY q\.payload->>'workId', q\.source/,
    );
    expect(source).toMatch(/WHERE rotation_rank <= \$3 OR frontier_rank <= \$3/);
    expect(source).toMatch(
      /loadP1Candidates\(false\)[\s\S]{0,180}loadP1Candidates\(true\)/,
    );
    // Admission must not rotate a work solely because a stale queue row is
    // still present after another source published its canonical chapter.
    expect(source).toMatch(
      /queue_candidate_groups[\s\S]{0,1800}canonical_chapter\.published_at IS NOT NULL[\s\S]{0,300}canonical_chapter\.number = q\.chapter_sort_key/,
    );
    const p1PressureStart = source.indexOf('const ready = await this.runQuery');
    const p1PressureSql = source.slice(p1PressureStart, p1PressureStart + 9000);
    expect(p1PressureSql).toMatch(
      /JOIN works w ON w\.id = \(q\.payload->>'workId'\)::uuid[\s\S]{0,900}w\.published IS TRUE/,
    );
    expect(source).toMatch(
      /loadP2Candidates\(false\)[\s\S]{0,180}loadP2Candidates\(true\)/,
    );
    expect(source).toMatch(
      /loadOnDemandCandidates\(false\)[\s\S]{0,180}loadOnDemandCandidates\(true\)/,
    );
    expect(source).toContain("predecessor.chapter_sort_key < q.chapter_sort_key");
    expect(source).toContain('predecessor_canonical.published_at IS NOT NULL');
    expect(source).toContain("staged_frontier.status IN ('STAGED', 'WAITING_FOR_GAP')");
  });

  it('preserves per-work counts, attempt limits and frontiers with one SQL roundtrip', async () => {
    const db = new PGlite();
    try {
      await db.exec(`CREATE TABLE importer_queue (task_type text,status text,payload jsonb,attempts int,max_attempts int,chapter_sort_key numeric,source text,next_run_at timestamptz);
        CREATE TABLE chapters (work_id uuid,number numeric,published_at timestamptz);
        CREATE TABLE importer_chapter_mappings (work_id uuid,status text,chapter_sort_key numeric,is_gap boolean DEFAULT false);
        CREATE TABLE importer_confirmed_gaps (work_id uuid,start_sort_key numeric,end_sort_key numeric);
        CREATE TABLE importer_sources (id text PRIMARY KEY,status text,cooldown_until timestamptz,enabled boolean,blocked_reason text,blocked_details jsonb);
        INSERT INTO importer_sources (id,status,cooldown_until,enabled,blocked_reason,blocked_details) VALUES ('s','ACTIVE',NULL,true,NULL,NULL);`);
      const id='00000000-0000-0000-0000-000000000001', empty='00000000-0000-0000-0000-000000000002';
      for (const [status,attempts,key] of [['QUEUED',0,3],['QUEUED',7,2],['IMPORTING',1,4],['PAUSED_BY_STAFF',0,5],['COMPLETED',0,1]]) {
        await db.query(`INSERT INTO importer_queue VALUES ('IMPORT_CHAPTER',$1,$2, $3,7,$4,'s',now())`,[status,JSON.stringify({workId:id}),attempts,key]);
      }
      await db.query(`INSERT INTO chapters VALUES ($1,1,now()),($1,2,now()),($1,3,NULL)`,[id]);
      await db.query(`INSERT INTO importer_chapter_mappings (work_id,status,chapter_sort_key) VALUES ($1,'STAGED',4),($1,'COMPLETED',1),($1,'PENDING',5)`,[id]);
      const sql=readFileSync('src/core/scheduler/admission-controller.ts','utf8')
        .split('const snapshot = await this.runQuery(`')[1]
        .split('`, [activeWorks')[0]
        .replace('${SOURCE_EXECUTION_ELIGIBILITY_SQL}', SOURCE_EXECUTION_ELIGIBILITY_SQL);
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
    expect(first).toBe(second); await Promise.resolve(); expect(calls).toBe(1);
    finish(); await first;
    const third=c.runAdmissionCycle(); await Promise.resolve(); expect(calls).toBe(2); finish(); await third;
  });

  it('serializes periodic and on-demand scans on the bounded database pool', async () => {
    const c = new AdmissionController({} as any, {} as any, { query: async () => ({ rows: [] }) });
    let finishPeriodic!: () => void;
    let onDemandStarted = false;
    (c as any).executeAdmissionCycle = () => new Promise<void>((resolve) => { finishPeriodic = resolve; });
    (c as any).executeOnDemandAdmission = async () => {
      onDemandStarted = true;
      return null;
    };

    const periodic = c.runAdmissionCycle();
    const onDemand = c.admitNextWorkOnDemand('P1', ['hanamiheaven']);
    await Promise.resolve();
    expect(onDemandStarted).toBe(false);

    finishPeriodic();
    await periodic;
    await onDemand;
    expect(onDemandStarted).toBe(true);
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

  it('rotates a queued window when every queued row is source-ineligible or canonically satisfied', async () => {
    const active = new Map<string, any>();
    const work = {workId:'00000000-0000-0000-0000-000000000014',workTitle:'Blocked Window Work',lane:'P1',state:'FILLING',primarySource:'s',admittedAt:new Date().toISOString(),lastActivityAt:new Date().toISOString(),totalChapters:12,publishedChapters:4,queuedChapters:1,inFlightChapters:0,frontierSortKey:5,criticalGapSortKey:null,criticalGapUnblockCount:0};
    active.set(work.workId, work);
    const state = {getActiveWorks:()=>Array.from(active.values()),setActiveWork:(w:any)=>active.set(w.workId,w),removeActiveWork:(id:string)=>active.delete(id)} as any;
    const pool = {query:async(sql:string) => {
      if (sql.includes('queued_cnt') && sql.includes('retry_cnt')) return {rows:[{work_id:work.workId,queued_cnt:'1',claimable_cnt:'0',importing_cnt:'0',paused_cnt:'8',retry_cnt:'0',min_sort_key:'5',min_queued:'5',pub_cnt:'4',max_pub:'4',staged_cnt:'0',min_staged:null,unimported_cnt:'8',source_status:'ACTIVE',cooldown_until:null}]};
      return {rows:[]};
    }};
    const controller = new AdmissionController(state, {} as any, pool as any);
    await (controller as any).reconcileActiveWorks();
    expect(active.has(work.workId)).toBe(false);
  });

  it('caps a P1 work to one executable chapter and preserves its remaining backlog paused', async () => {
    const db = new PGlite();
    try {
      const id = '00000000-0000-0000-0000-000000000013';
      await db.exec(`
        CREATE TABLE importer_queue (
          id integer PRIMARY KEY, task_type text, status text, payload jsonb,
          priority integer, chapter_sort_key numeric, next_run_at timestamptz,
          updated_at timestamptz
        );
        INSERT INTO importer_queue VALUES
          (1, 'IMPORT_CHAPTER', 'QUEUED', '{"workId":"${id}"}', 75, 1, now(), now()),
          (2, 'IMPORT_CHAPTER', 'QUEUED', '{"workId":"${id}"}', 75, 2, now(), now()),
          (3, 'IMPORT_CHAPTER', 'QUEUED', '{"workId":"${id}"}', 75, 3, now(), now()),
          (4, 'IMPORT_CHAPTER', 'PAUSED_BY_STAFF', '{"workId":"${id}"}', 75, 4, now(), now());
      `);
      const controller = new AdmissionController({} as any, {} as any, {
        query: (sql: string, params?: any[]) => db.query(sql, params),
      });

      await expect((controller as any).enforceP1FairWindow(id)).resolves.toBe(1);
      const { rows } = await db.query<any>(
        `SELECT status, chapter_sort_key FROM importer_queue ORDER BY chapter_sort_key`,
      );
      expect(rows).toEqual([
        { status: 'QUEUED', chapter_sort_key: '1' },
        { status: 'PAUSED_BY_STAFF', chapter_sort_key: '2' },
        { status: 'PAUSED_BY_STAFF', chapter_sort_key: '3' },
        { status: 'PAUSED_BY_STAFF', chapter_sort_key: '4' },
      ]);
    } finally { await db.close(); }
  });

  it('does not replenish a P1 window before it rotates', () => {
    const source = readFileSync('src/core/scheduler/admission-controller.ts', 'utf8');
    expect(source).toMatch(/const P1_FAIR_WINDOW_CHAPTERS = 1/);
    expect(source).toMatch(/if \(work\.lane === 'P1' && work\.criticalGapSortKey === null\) continue;/);
    expect(source).toMatch(/\[P1_FAIR_WINDOW_CAPPED\]/);
  });

  it('repairs a legacy visible P2/P3 window in a bounded, idempotent work-scoped batch', async () => {
    const db = new PGlite();
    try {
      const id = '00000000-0000-0000-0000-000000000021';
      await db.exec(`
        CREATE TABLE works (id uuid PRIMARY KEY, published boolean);
        CREATE TABLE importer_queue (id integer PRIMARY KEY, task_type text, status text, payload jsonb, priority integer, chapter_sort_key numeric, next_run_at timestamptz, updated_at timestamptz);
        INSERT INTO works VALUES ('${id}', true);
        INSERT INTO importer_queue VALUES
          (1, 'IMPORT_CHAPTER', 'QUEUED', '{"workId":"${id}"}', 20, 1, now(), now()),
          (2, 'IMPORT_CHAPTER', 'PAUSED_BY_STAFF', '{"workId":"${id}"}', 20, 2, now(), now());
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

  it('revives only legacy cancelled rows whose canonical mapping is still queued', async () => {
    const db = new PGlite();
    try {
      const work = '00000000-0000-0000-0000-000000000031';
      await db.exec(`
        CREATE TABLE importer_queue (
          id integer PRIMARY KEY, task_type text, status text, source text,
          payload jsonb, cancel_reason text, cancelled_by text, cancelled_at timestamptz,
          updated_at timestamptz, locked_by text, locked_at timestamptz,
          lease_expires_at timestamptz, cancel_requested boolean, next_run_at timestamptz,
          last_error text, last_error_at timestamptz, retry_reason text,
          last_recovered_error text, recovered_at timestamptz
        );
        CREATE TABLE importer_chapter_mappings (
          work_id uuid, source text, source_chapter_id text, status text, is_gap boolean
        );
        CREATE TABLE importer_staff_requests (work_id uuid, status text);
        INSERT INTO importer_queue (id,task_type,status,source,payload,updated_at,last_error)
          VALUES (1,'IMPORT_CHAPTER','CANCELLED_BY_STAFF','source-a',
            '{"workId":"${work}","sourceChapterId":"ch-1"}',now()-interval '1 day','Unknown error'),
                 (2,'IMPORT_CHAPTER','CANCELLED_BY_STAFF','source-a',
            '{"workId":"${work}","sourceChapterId":"ch-2"}',now()-interval '1 day','explicit pause');
        INSERT INTO importer_chapter_mappings VALUES
          ('${work}','source-a','ch-1','QUEUED',false),
          ('${work}','source-a','ch-2','QUEUED',false);
        INSERT INTO importer_staff_requests VALUES ('${work}','ACTIVE');
      `);
      // The active staff request protects both rows. Remove it before testing
      // that the legacy row is revived while explicit metadata remains gated.
      await db.exec(`DELETE FROM importer_staff_requests; UPDATE importer_queue SET cancel_reason='TEMPORARY_PAUSE', cancelled_by='staff', cancelled_at=now() WHERE id=2;`);
      const state = { getConfig: () => ({}) } as any;
      const controller = new AdmissionController(state, {} as any, {
        query: (sql: string, params?: any[]) => db.query(sql, params),
      });

      await expect((controller as any).recoverOrphanedCancelledChapterJobs()).resolves.toBe(1);
      const { rows } = await db.query<any>(`SELECT id,status,retry_reason FROM importer_queue ORDER BY id`);
      expect(rows).toEqual([
        { id: 1, status: 'QUEUED', retry_reason: 'ORPHANED_CANCELLED_MAPPING_RECOVERY' },
        { id: 2, status: 'CANCELLED_BY_STAFF', retry_reason: null },
      ]);
    } finally { await db.close(); }
  });

  it('reopens only legacy controlled-recovery failures on healthy sources', async () => {
    const db = new PGlite();
    try {
      const work = '00000000-0000-0000-0000-000000000032';
      await db.exec(`
        CREATE TABLE importer_queue (
          id integer PRIMARY KEY, task_type text, status text, source text,
          payload jsonb, retry_reason text, last_error text, attempts integer,
          locked_by text, locked_at timestamptz, lease_expires_at timestamptz,
          cancel_requested boolean, next_run_at timestamptz, last_error_at timestamptz,
          last_recovered_error text, recovered_at timestamptz, updated_at timestamptz
        );
        CREATE TABLE importer_chapter_mappings (
          id integer PRIMARY KEY, work_id uuid, source text, source_chapter_id text,
          status text, chapter_id uuid, is_gap boolean, last_error text, updated_at timestamptz
        );
        CREATE TABLE importer_sources (id text PRIMARY KEY, status text);
        CREATE TABLE importer_staff_requests (work_id uuid, status text);
        INSERT INTO importer_sources VALUES ('healthy','ACTIVE'),('blocked','UPSTREAM_BLOCKED');
        INSERT INTO importer_queue (id,task_type,status,source,payload,retry_reason,last_error,attempts,updated_at)
          VALUES (1,'IMPORT_CHAPTER','FAILED','healthy','{"workId":"${work}","sourceChapterId":"ch-1"}',
            'TRANSIENT_NETWORK','[RETRY_BUDGET_EXHAUSTED] Controlled recovery cancelled stalled in-flight work',5,now()-interval '1 day'),
                 (2,'IMPORT_CHAPTER','FAILED','blocked','{"workId":"${work}","sourceChapterId":"ch-2"}',
            'TRANSIENT_NETWORK','[RETRY_BUDGET_EXHAUSTED] Controlled recovery cancelled stalled in-flight work',5,now()-interval '1 day');
        INSERT INTO importer_chapter_mappings (id,work_id,source,source_chapter_id,status,is_gap)
          VALUES (11,'${work}','healthy','ch-1','FAILED',false),(12,'${work}','blocked','ch-2','FAILED',false);
      `);
      const controller = new AdmissionController({ getConfig: () => ({}) } as any, {} as any, {
        query: (sql: string, params?: any[]) => db.query(sql, params),
      });

      await expect((controller as any).recoverLegacyTransientFailures()).resolves.toBe(1);
      const { rows } = await db.query<any>(`SELECT q.id,q.status,q.retry_reason,m.status mapping_status FROM importer_queue q JOIN importer_chapter_mappings m ON m.id=q.id+10 ORDER BY q.id`);
      expect(rows).toEqual([
        { id: 1, status: 'QUEUED', retry_reason: 'LEGACY_TRANSIENT_FAILURE_RECOVERY', mapping_status: 'QUEUED' },
        { id: 2, status: 'FAILED', retry_reason: 'TRANSIENT_NETWORK', mapping_status: 'FAILED' },
      ]);
    } finally { await db.close(); }
  });

  it('reopens a transient frontier only after a newer source recovery', async () => {
    const db = new PGlite();
    try {
      const work = '00000000-0000-0000-0000-000000000033';
      await db.exec(`
        CREATE TABLE importer_queue (
          id integer PRIMARY KEY, task_type text, status text, source text,
          payload jsonb, retry_reason text, last_error text, attempts integer,
          chapter_sort_key numeric,
          last_error_at timestamptz, last_recovered_error text, recovered_at timestamptz,
          updated_at timestamptz, locked_by text, locked_at timestamptz,
          lease_expires_at timestamptz, cancel_requested boolean, next_run_at timestamptz
        );
        CREATE TABLE importer_chapter_mappings (
          id integer PRIMARY KEY, work_id uuid, source text, source_chapter_id text,
          status text, is_gap boolean, last_error text, updated_at timestamptz
        );
        CREATE TABLE importer_sources (
          id text PRIMARY KEY, status text, enabled boolean, blocked_details jsonb,
          updated_at timestamptz
        );
        CREATE TABLE chapters (work_id uuid, number numeric, published_at timestamptz);
        CREATE TABLE importer_staff_requests (work_id uuid, status text);
        INSERT INTO importer_sources VALUES
          ('recovered','ACTIVE',true,'{"recovered_at":"2026-10-05T20:00:00Z"}',now()),
          ('not-recovered','ACTIVE',true,NULL,now());
        INSERT INTO importer_queue (id,task_type,status,source,payload,retry_reason,last_error,attempts,last_error_at,updated_at)
          VALUES
            (1,'IMPORT_CHAPTER','FAILED','recovered','{"workId":"${work}","sourceChapterId":"ch-1","chapterNumber":29}',
              'TRANSIENT_NETWORK','[RETRY_BUDGET_EXHAUSTED] timeout',5,now()-interval '2 days',now()-interval '2 days'),
            (2,'IMPORT_CHAPTER','FAILED','not-recovered','{"workId":"${work}","sourceChapterId":"ch-2","chapterNumber":30}',
              'TRANSIENT_NETWORK','[RETRY_BUDGET_EXHAUSTED] timeout',5,now()-interval '2 days',now()-interval '2 days');
        INSERT INTO importer_chapter_mappings (id,work_id,source,source_chapter_id,status,is_gap)
          VALUES (11,'${work}','recovered','ch-1','PENDING',false),(12,'${work}','not-recovered','ch-2','PENDING',false);
      `);
      const controller = new AdmissionController({ getConfig: () => ({}) } as any, {} as any, {
        query: (sql: string, params?: any[]) => db.query(sql, params),
      });

      await expect((controller as any).recoverSourceRecoveredTransientFailures()).resolves.toBe(1);
      const { rows } = await db.query<any>(`SELECT q.id,q.status,q.retry_reason,m.status mapping_status
        FROM importer_queue q JOIN importer_chapter_mappings m ON m.id=q.id+10 ORDER BY q.id`);
      expect(rows).toEqual([
        { id: 1, status: 'QUEUED', retry_reason: 'SOURCE_RECOVERY_RETRY', mapping_status: 'PENDING' },
        { id: 2, status: 'FAILED', retry_reason: 'TRANSIENT_NETWORK', mapping_status: 'PENDING' },
      ]);
    } finally { await db.close(); }
  });

  it('treats every visible legacy below-P1 work as P1 before P2 discovery', () => {
    const source = readFileSync('src/core/scheduler/admission-controller.ts', 'utf8');
    expect(source.indexOf('await this.repairVisibleP2LifecycleBacklog();'))
      .toBeLessThan(source.indexOf('await this.reconcileActiveWorks();'));
    expect(source).toMatch(/w\.published IS TRUE[\s\S]{0,1000}q\.priority < 75/);
    expect(source).toMatch(/q\.status = 'QUEUED'[\s\S]{0,80}q\.status = 'IMPORTING'/);
    expect(source).toMatch(/status = CASE WHEN pw\.id IS NOT NULL THEN 'QUEUED'/);
  });
});
