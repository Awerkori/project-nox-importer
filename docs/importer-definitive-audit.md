# Project Nox — Importer Definitivo: historical audit

Generated catalogue: `node scripts/generate-importer-pr-catalog.mjs` writes one row per accessible pull request to `docs/importer-pr-catalog.csv`. Each row records the GitHub objective/state/merge commit, the files changed by that commit in local history, a repeatable component classification, and any machine-verifiable revert relation. The catalogue is intentionally generated rather than hand-maintained.

## Scope and evidence

- 879 PRs were accessible at the audit boundary: 868 merged, 5 open and 6 closed without merge.
- The historical Lite cutover replaced overlapping legacy controllers with durable work projection, numeric priority, canonical barriers, bounded media controls and a distinct monitor lane. Those are retained.
- Public production evidence at the audit boundary was 88 canonical publications in 30 minutes (2.93/min), not the 10/min target. The public site sample remained responsive. This is a baseline, not a success claim.
- #878's per-source page-permit policy was measured as a regression: aggregate media wall time rose from roughly 56 to 70 seconds and global page capacity was under-used. #879 reverts that policy. A merge is not treated as deployment evidence; runtime fingerprint must be checked before throughput comparisons.
- The terminal check at 2026-10-28 00:46 UTC found production still on #878 (`c8e5011` / `c06f…`), not #879. It recorded 25 public publications in 10 minutes (2.5/min), YSQL SQL p95 1.10 s, transaction p95 4.43 s, and all three primary-pool clients busy with three waiters. Its 24-page global limiter averaged only 5.4 active requests (p95 12; peak 19), while #878's source shares were still present. This is the current comparison baseline; no capacity result may be attributed to the revert until its fingerprint changes.

## Critical historical decisions

| PRs | Decision retained or rejected | Evidence / consequence |
| --- | --- | --- |
| #42, #48, #56, #70 | durable priorities, real occupancy, canonical/cohort identity and strict frontier | Required to avoid duplicate/out-of-order chapter publication. |
| #110, #141, #170, #182, #197 | decouple visible publication, preserve Reader/YSQL headroom, work/source fairness and release tracking | Correct direction; later work must not recreate a global serial claim path. |
| #801, #802, #804 | ordered chapter barriers, 48-page buffer rule, multi-work claims | Preserve; they address actual ordering and idle-worker regressions. |
| #807, #808, #841, #842 | bound YSQL claim/metadata work, guarded chapter admission, bounded P1 frontier and durable batches | Preserve; no pool or connection expansion is part of this pass. |
| #850, #856–#860, #863, #866, #870, #874–#877 | release media permits promptly, P0 source round-robin, P0/P1 source reservation, isolated metadata work, source-aware readiness | Retain the successful boundaries, but remove the assumption that an old P2 catalog pass has enrolled every existing work for P0. |
| #878 → #879 | per-page fairness divided the fixed 24 download permits among sources | Rejected. Source fairness belongs at chapter admission; page-level partitioning idled useful capacity. |

## P0 diagnosis

`monitoring=true` catalog rows are intentionally stored at P2 priority: they are *observations*, not normal P2 claims. A successful observation materializes a genuinely new newest chapter as P0; confirmed historical chapters remain P1. Therefore a `P0_MONITOR []` presentation alone does not show that P0 is idle.

The real structural gap is coverage. At the audit boundary, approximately 8,158 public ongoing/hiatus works had an active operational mapping, while only about 1,537 had a recurring monitor. The monitor executor was able to run, but most eligible existing works had never been enrolled. Existing monitor tasks were concentrated in MangaFlix and Manhastro; task state alone could not establish catalogue coverage.

Read-only source samples confirmed live snapshots from MangaFlix, MangoToons, Manhastro and Nexus. The samples did not justify fabricating missing releases. They did show that a P1 backlog (for example, MangoToons historical chapters) is distinct from P0 release discovery.

## Consolidated architecture

1. **Durable P0 coverage producer.** A separate source cursor enrolls a bounded slice of published ongoing/hiatus mappings into the existing monitor lane. It rotates providers, honours source health/chapter-ingestion policy, and never scans the whole catalogue in a claim transaction.
2. **One P0 execution path.** The existing two-permit monitor lane remains the only network executor. It continues to create P0 only for a real newest release and P1 for confirmed historical repairs.
3. **No duplicate source snapshot.** A live normal catalog task is never converted or shadowed. A completed first catalog pass may be safely revived as its recurring watch; a new identity is used only where no historical task exists.
4. **P0 is independent of P2 campaigns.** A bounded P2 metadata campaign cannot freeze monitor turns. Emergency/manual protective stops and confirmed site protection still apply.
5. **Work-conserving media.** Keep the global media envelope and chapter-level source/work fairness. Do not reintroduce per-page source shares from #878.
6. **Observable control plane.** The heartbeat reports the last P0 coverage enrollment result, making an absent release distinguishable from absent monitoring coverage without adding a heavy diagnostic scan.

This changes enrollment, not chapter admission, page concurrency, buffers, connection limits, canonical order, Staff precedence, or P2 discovery.

## Validation sequence

1. Confirm the runtime contains the #879 revert before comparing performance.
2. Confirm the new heartbeat reports `p0MonitorCoverage` advancing sources and durable monitor tasks grow gradually rather than in a boot-time burst.
3. Verify monitor turns run while P1/P2 are active. If a real new release appears, verify it becomes P0, respects its work's canonical barrier, and becomes publicly visible.
4. Measure public publications in short 2–3 minute candidate windows. Only a configuration with a credible 10/min trend receives the full 10-minute proof window.
5. Preserve the source page envelope and YSQL/site safeguards; tune only the measured bottleneck after coverage is proven.
