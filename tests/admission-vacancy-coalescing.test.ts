import {it,expect,vi} from 'vitest';
import {AdmissionController} from '../src/core/scheduler/admission-controller.js';
it('coalesces polling vacancy hints but immediately handles an actual vacated work',async()=>{
 vi.useFakeTimers();vi.setSystemTime(new Date('2026-09-28T00:00:00Z'));
 try{
  const controller=Object.create(AdmissionController.prototype) as any;
  Object.assign(controller,{isRunning:true,isReplenishingCycle:false,immediateReplenishTimer:null,lastVacancyReplenishAt:0,runAdmissionCycle:vi.fn().mockResolvedValue(undefined)});
  controller.triggerImmediateReplenishment('PRODUCTIVE_SLOT_VACANCY');await vi.advanceTimersByTimeAsync(50);
  controller.triggerImmediateReplenishment('PRODUCTIVE_SLOT_VACANCY');await vi.advanceTimersByTimeAsync(50);
  expect(controller.runAdmissionCycle).toHaveBeenCalledTimes(1);
  controller.triggerImmediateReplenishment('WORK_GAP_BLOCKED_VACATED');await vi.advanceTimersByTimeAsync(50);
  expect(controller.runAdmissionCycle).toHaveBeenCalledTimes(2);
 }finally{vi.useRealTimers();}
});
