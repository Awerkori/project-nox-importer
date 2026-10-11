# Importer Lite: historical lessons

This is an evidence map, not a claim that every incident was fully validated in
production. Commit subjects and tests establish the implemented correction;
only a controlled benchmark can establish sustained production capacity.

| Historical failure | Evidence in 1.0 | Lite preventive rule / owner | Regression evidence | Operational signal |
|---|---|---|---|---|
| Admission probes competed with claims | PR #250 deferred probes behind claim pressure | Claim reads only `importer_lite_ready_works`; discovery/catalog run outside claim | `importer_lite_claim_task` has no source/catalog join | ready age; DB pool wait |
| Blocked work/frontier hid useful work | PR #300, #350, #380 | only `READY` work projection is claimable; one work gets one turn | source-selector large-backlog test; claim SQL | ready count and oldest-ready age |
| P2/P1 contention and priority inversions | PRs #390, #400, #401, #411–#419 | one numeric ordering STAFF(0), P0(1), P1(2), P2(3); staff trigger promotes durable tasks | claim query ordering and staff trigger | ready count by priority |
| P0 candidate race could hand a slot to a lower lane or repeatedly favor one stale candidate | PR #409 (`retain P0 after raced claim`) | ready-work row is atomically claimed; a monitor never enters the normal projection; only durable, executable chapter tasks receive P0 | integration: 1,300 monitors plus P1/P2; monitor-to-P0 materialization | P0 ready age, monitor next-run time, empty monitor probes |
| P0/STAFF presence probes could add YSQL work while no eligible task existed | PR #168 cached empty STAFF claims; PR #334 cached P0 admission probe; PR #339/#362 optimized P0 scans | one indexed monitor claim, one permit, shared 15s empty backoff; normal claims read only the ready-work projection | integration: monitor lane is invisible to normal claims | pool wait, monitor limiter active/queued, next monitor |
| A prolonged source outage could turn a P0 watch into an abandoned record | P0 recovery history and recurring source unavailability | one monitor row has capped recurrence backoff and resets after success; it does not consume normal claims | integration: max-attempt monitor stays recoverable while P1 claims | monitor reason, next-run, P1 ready age |
| New STAFF request could conflict with lower-priority work already in flight | historical scheduler treated priority and capacity in overlapping paths; PR #400 established strict claim order | STAFF preempts only new READY claims. RUNNING tasks retain lease/finalization; finish re-evaluates active panel rows so cancellation restores base priority | integration: P0/P1/P2 finish while duplicate STAFF overlay arrives/cancels | running leases, stored-unpublished age, STAFF ready age |
| Unverified numeric gap stalled publication | PR #395 | no inferred predecessors in claim/publish; dependencies must be materialized/explicit | chapter identity tests | blocked reasons by code |
| Numeric Reader identity could collapse distinct source chapters | numeric display storage cannot distinguish all textual source identities | canonical identity remains textual; an incompatible legacy numeric uniqueness rule blocks the later chapter rather than merging it | integration: `12.1` versus `12.10` collision | `CHAPTER_NUMBER_COLLISION_REQUIRES_READER_MIGRATION` |
| Catalog maintenance consumed data-plane capacity | PR #398, #412 | catalog tasks are normal bounded tasks, not a side scheduler; execution is network outside transactions | runtime has one worker pool | catalog task latency; DB wait |
| A large manifest caused per-chapter YSQL work | cataloguing hundreds of chapters multiplies round trips and pool holds | source mappings and queue rows are batched; ready projection refreshes once per work | integration: 1,300-chapter batch | catalog DB time; pool wait |
| Empty claims and YSQL pressure | PR #399, #413, #414, #418 | indexed ready-work projection, short claim transaction, no correlated canonical lookup | migration indexes and function | claim latency; empty claims |
| P0 lost in a race | PR #409 | source priority never changes task priority; task rank lives durably in the row | priority rank unit coverage and SQL order | claimed priority distribution |
| An old source chapter could be mistaken for a P0 release | a later manifest reconciliation can reveal historical content | persist source-manifest order; P0 promotes only entries beyond its known frontier, older entries are P1 repairs | integration: P0 historical repair | P0/P1 materialisation counts |
| STAGED/paused work was invisible | PR #416, #417 | media-complete import enqueues PUBLISH immediately; reconciler repairs only bounded missing publication tasks | repository reconciler | stored-but-unpublished age |
| RSS pressure permanently reduced capacity | PR #420 | Lite uses fixed, global limits and does not alter concurrency from RSS; pressure must backpressure admission, not permanently shrink limits | legacy memory tests retained; Lite limits are explicit | RSS/heap/external + productive slots |
| Telegram state existed only in RAM | direct storage `metadataMap` and historical recovery code | upload intent/receipt journal is durable; uncertain outcomes are BLOCKED, never blindly duplicated | upload state schema; executor path | INTENT/UNKNOWN uploads |
| A restart could leave an upload outcome unclassified | no transaction spans Telegram and YSQL | bounded reconciler turns `UNKNOWN` and stale intents into one explicit per-task block, never a blind resend | integration: unknown upload recovery | old unknown-upload age |
| Source failures were observed only by a chapter worker | discovery/catalog failures could keep being retried without a shared cooldown | all source-facing stages write one durable health record; success clears transient degradation | integration: discovery source health | source health/cooldown, failures |
| Crash recovery could keep re-leasing the same task | lease expiry alone does not prove the work remains safe to retry forever | reconciler respects `max_attempts` for expired leases and due retries | integration: crash-exhausted lease | exhausted-task count |

## Causal map retained from the audit

1. Expensive or repeated admission queries increased pool wait; slow claims left
   chapter workers idle; the growing backlog made selection more expensive.
   Lite breaks this at the ready-work projection and never asks metadata,
   source health, publication or predecessor questions while claiming.
2. A blocked prefix plus `STAGED` barrier rules could make completed media
   invisible. Lite creates a publish task immediately after the durable media
   checkpoint. A bounded reconciler only restores missing transitions.
3. Memory pressure led to autotuning that could remain reduced after the active
   buffers had gone away. Lite has explicit global limits and reports pressure;
   it has no competing capacity tuner.
4. Source preference was coupled to scheduling in legacy recovery paths. Lite
   chooses task priority first, then selects a confirmed healthy source route;
   cooldown never grants exclusivity to a preferred source.
5. Empty P0 monitoring used to be easy to mistake for executable high-priority
   work. Lite stores a monitor as a durable, indexed observation with its own
   one-slot budget. Its sole output is a real P0 chapter task, so quiet tracked
   works cannot suppress P1/P2 claims.
6. Strict STAFF admission must not become a finalization barrier. Lite has no
   priority check after a task is RUNNING; durable media and publication
   complete under their existing lease, then future admission observes STAFF.

## Known boundaries

The current Reader table still represents its visible chapter number as a
numeric column. Lite stores a textual canonical chapter identity and display
number in `importer_lite_chapters` to prevent importer-side collapse. If an
installed Reader uniqueness rule nevertheless collides, Lite blocks the later
task as `CHAPTER_NUMBER_COLLISION_REQUIRES_READER_MIGRATION` rather than
merging two identities. Before cutover, the Reader’s rendering and uniqueness
contract must be verified for two legitimate chapters that share a numeric
display value (for example an extra and a regular edition). Telegram has no transaction shared with YSQL:
when Telegram succeeds but its receipt cannot be durably written, Lite blocks
that page as `UPLOAD_OUTCOME_UNKNOWN` instead of risk a duplicate upload. A
recovery connector can be added only after a reliable channel-message lookup
is validated for the configured bots.
