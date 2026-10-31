import {describe,it,expect} from 'vitest';
import {incidents,transitions,trackCatalog} from '../scripts/monitoring/sentinel-policy.mjs';
describe('sentinel incident handling',()=>{
 it('opens, deduplicates, reminds and recovers',()=>{
  let a=transitions({}, {heartbeat_stale:'old'}, 10000000);expect(a.events[0].type).toBe('opened');
  let b=transitions(a.state,{heartbeat_stale:'old'},10001000);expect(b.events).toEqual([]);
  let c=transitions(b.state,{heartbeat_stale:'old'},14000000);expect(c.events[0].type).toBe('reminder');
  expect(transitions(c.state,{},14001000,3600000,['heartbeat_stale']).events).toEqual([]);
  expect(transitions(c.state,{},14001000).events[0].type).toBe('recovered');
 });
 it('detects a due monitor without claiming source availability',()=>{
  const h={observedAt:new Date(10000000).toISOString()};
  expect(incidents({heartbeat:h,p0:{at:new Date(10000000).toISOString(),next:new Date(1).toISOString()}},10000000).p0_overdue).toContain('may include cooled sources');
 });
 it('tracks a catalog owner without treating a lease renewal as proof of progress',()=>{
  const tasks=trackCatalog({},[{id:'a',lease_owner:'worker'}],1000);
  expect(trackCatalog(tasks,[{id:'a',lease_owner:'worker'}],2000).a.firstSeen).toBe(1000);
  expect(trackCatalog(tasks,[{id:'a',lease_owner:'new'}],2000).a.firstSeen).toBe(2000);
  expect(incidents({heartbeat:{observedAt:new Date(2000000).toISOString()},catalog:{at:new Date(2000000).toISOString(),tasks}},2000000).catalog_prolonged).toContain('inspect before recovery');
 });
 it('flags old missing publication markers without assuming they are defects',()=>{
  const now=10000000;
  const out=incidents({heartbeat:{observedAt:new Date(now).toISOString()},receipts:{at:new Date(now).toISOString(),oldest:new Date(1).toISOString(),count:2}},now);
  expect(out.unpublished_receipts).toContain('may include legitimate presentation blocks');
 });
 it('does not infer healthy state from missing heartbeat',()=>{
  expect(incidents({heartbeat:null},10000000)).toHaveProperty('heartbeat_stale');
  expect(incidents({error:'timeout'},10000000)).toHaveProperty('database_unreachable');
 });
});
