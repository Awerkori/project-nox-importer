import {beforeEach,afterEach,expect,it,vi} from 'vitest';
import {AdaptiveAutotuner} from '../src/core/concurrency.js';
import {diagnostics} from '../src/core/diagnostics.js';
beforeEach(()=>{
  vi.spyOn(diagnostics,'getMemorySnapshot').mockReturnValue({rssMb:150,heapUsedMb:80,heapTotalMb:120,externalMb:10,arrayBuffersMb:5});
  vi.spyOn((diagnostics as any).lagMonitor,'getMetrics').mockReturnValue({avgLagMs:5,maxLagMs:15,recentLagMs:5});
});
afterEach(()=>{vi.restoreAllMocks();});
const pressure=(dbPressure=0)=>({siteHealth:'YELLOW',pressureScore:15,pressureReason:'Mild site latency',consecutive5xx:0,pressureBreakdown:{sitePressure:15,dbPressure,memoryPressure:0,eventLoopPressure:0,storagePressure:0,sourcePressure:0,publicationPressure:0}} as any);
it('does not collapse below the healthy floor on repeated isolated mild WAN warnings',()=>{
  const tuner=new AdaptiveAutotuner({initialConcurrency:10,maxConcurrency:10,minConcurrency:1,healthyConcurrencyFloor:8});
  for(let i=0;i<10;i++)tuner.evaluateCycle(pressure());
  expect(tuner.getCurrentConcurrency()).toBe(8);
});
it('retains survival capacity when mild site latency accompanies actual DB pressure',()=>{
  const tuner=new AdaptiveAutotuner({initialConcurrency:8,maxConcurrency:10,minConcurrency:1,healthyConcurrencyFloor:8});
  for(let i=0;i<10;i++)tuner.evaluateCycle(pressure(15));
  expect(tuner.getCurrentConcurrency()).toBe(1);
});
it('does not scale up from low capacity during a mild warning',()=>{
  const tuner=new AdaptiveAutotuner({initialConcurrency:2,maxConcurrency:6,minConcurrency:1,healthyConcurrencyFloor:6});
  for(let i=0;i<5;i++)tuner.evaluateCycle(pressure());
  expect(tuner.getCurrentConcurrency()).toBe(2);
});
