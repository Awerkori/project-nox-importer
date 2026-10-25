-- Parallel query on this single small YB node added seconds of Gather overhead
-- (cross-source deferred refill: 9.4 s with 2 workers vs 1 ms without).
-- The importer main pool already used 0; apply to every app session. Reversible:
-- ALTER ROLE admin IN DATABASE project_nox_prod RESET max_parallel_workers_per_gather;
ALTER ROLE admin IN DATABASE project_nox_prod SET max_parallel_workers_per_gather = 0;
