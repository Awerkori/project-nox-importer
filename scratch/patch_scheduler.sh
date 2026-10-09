#!/bin/bash
sed -i 's/enabled: false,/enabled: true,/' src/core/scheduler/state-store.ts
sed -i 's/${CANONICAL_FRONTIER_CLAIM_FILTER}//g' src/core/scheduler/work-affinity-scheduler.ts
