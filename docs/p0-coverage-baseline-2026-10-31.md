# P0 baseline — 2026-10-31 23:24 UTC

Read-only production snapshot, non-atomic across queries. The public eligibility query counts distinct published non-cancelled works with an enabled chapter-ingestion source and a mapping in WAITING_ADMISSION, SYNCED, ACTIVE or PENDING; it does not filter source health. Active means at least one monitoring task in READY, RUNNING or RETRY_WAIT. A recorded successful check is `checkpoint.lastCheckedAt`; it is a latest value, not an event history or proof that every relevant mapping was checked.

* 10,674 eligible public works; 10,669 with an active monitor; 60 with a recorded last check in 30 minutes and 127 in 60 minutes; 6,076 have no recorded check on a monitor. Never-checked cannot be inferred from missing checkpoint data.
* All monitored works, including non-public: 143 distinct works / 144 monitor rows with recorded checks in 60 minutes, across 38 source IDs. 33,036 monitor rows, 33,015 active; 32,314 rows due, not distinct works; 25,665 rows lack recorded last-check data.
* Enabled sources without a recorded monitor check in the preceding hour: brscans, montetai, ninjascan, pinkrosa, universohentai. This is not a source-health diagnosis.
* Independent recent publication-events query: 670 publications in 60 minutes, 666 P1 and 4 P0/STAFF. Earlier r847 homologation reports 659 publications, 151 monitor checks and zero P2 discovery turns. The code of the 151 counter was not located, so its precise semantics are unverified.
* Current P0 concurrency is six; source-round-robin claims one monitor at a time per source turn, independent of source backlog size. Existing successful cadence extends beyond one hour for many works. Latest-value checkpoints do not establish recurring visits.

The local 30-minute cadence change is a scheduling prerequisite only. It does not increase throughput, fix per-source backlog fairness or establish durable event history. Do not deploy it in isolation against an already overloaded monitor queue; measure capacity, SQL pressure and source limits first.
