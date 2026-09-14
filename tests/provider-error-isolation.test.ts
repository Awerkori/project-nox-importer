import { expect, it } from 'vitest';
import { callProvider, RetryPolicy } from '../src/core/retry-policy';
it('keeps a provider timeout out of system pressure classification', async () => {
 let error:unknown;
 try {await callProvider(async()=>{throw new Error('request aborted by timeout');});}catch(e){error=e;}
 expect(RetryPolicy.classify(error)).toMatchObject({sourceStage:'provider',retryClass:'QUEUE_RETRY_TIMEOUT'});
 expect(RetryPolicy.classify(new Error('database timeout'))).toMatchObject({sourceStage:'system'});
});
it('preserves the provider status used for cooldown', async () => {
 let error:unknown;
 try {await callProvider(async()=>{throw Object.assign(new Error('429 rate limit'),{status:429,retryAfter:120});});}catch(e){error=e;}
 expect(RetryPolicy.classify(error)).toMatchObject({sourceStage:'provider',retryClass:'QUEUE_RETRY_429',retryAfterSeconds:120});
});
