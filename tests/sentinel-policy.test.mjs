import {describe,it,expect} from 'vitest';
import {incidents,transitions} from '../scripts/monitoring/sentinel-policy.mjs';
describe('sentinel incident handling',()=>{
 it('opens, deduplicates, reminds and recovers',()=>{
  let a=transitions({}, {heartbeat_stale:'old'}, 10000000);expect(a.events[0].type).toBe('opened');
  let b=transitions(a.state,{heartbeat_stale:'old'},10001000);expect(b.events).toEqual([]);
  let c=transitions(b.state,{heartbeat_stale:'old'},14000000);expect(c.events[0].type).toBe('reminder');
  expect(transitions(c.state,{},14001000,3600000,['heartbeat_stale']).events).toEqual([]);
  expect(transitions(c.state,{},14001000).events[0].type).toBe('recovered');
 });
 it('does not infer healthy state from missing heartbeat',()=>{
  expect(incidents({heartbeat:null},10000000)).toHaveProperty('heartbeat_stale');
  expect(incidents({error:'timeout'},10000000)).toHaveProperty('database_unreachable');
 });
});
