import { CANONICAL_FRONTIER_CLAIM_FILTER, CANONICAL_PUBLISHED_CLAIM_FILTER, CANONICAL_ACTIVE_CLAIM_FILTER } from '../src/core/scheduler/work-affinity-scheduler.js';
import { SOURCE_EXECUTION_ELIGIBILITY_SQL } from '../src/core/source-eligibility.js';
console.log('--- PUBLISHED ---', CANONICAL_PUBLISHED_CLAIM_FILTER);
console.log('--- FRONTIER ---', CANONICAL_FRONTIER_CLAIM_FILTER);
console.log('--- ACTIVE ---', CANONICAL_ACTIVE_CLAIM_FILTER);
console.log('--- ELIG ---', SOURCE_EXECUTION_ELIGIBILITY_SQL);
