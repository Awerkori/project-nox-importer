import { expect, it } from 'vitest';
import { PublicationSafetyBarrier } from '../src/core/publication-safety-barrier';
it('permits only a gap that can unblock staged content while CLOSED', async () => {
 let staged = false;
 const q:any={select:()=>q,eq:()=>q,gt:()=>q,limit:async()=>({data:staged?[{id:'staged'}]:[],error:null}),maybeSingle:async()=>({data:{value:'CLOSED'},error:null})};
 const barrier=new PublicationSafetyBarrier({from:()=>q} as any);
 expect(await barrier.canProcessChapter()).toBe(false);
 expect(await barrier.canProcessChapter('work',5)).toBe(false);
 staged=true;
 expect(await barrier.canProcessChapter('work',5)).toBe(true);
 expect(await barrier.canAcquireChapters()).toBe(false);
});
