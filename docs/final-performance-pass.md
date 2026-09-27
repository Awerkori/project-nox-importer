# Final importer engineering pass — in progress

Baseline main: `932134e1417bb3c138ba8cedb6a021bb31e84c83`.
Observability deployment: PR #119, `c3b97f78afd5cc9e8930eb9b4153d6908a5862a0`.
Target is **not yet proved**. All windows below use UTC and canonical `chapters.published_at`, not completed jobs.

## Production evidence

Discloud: 512 MB / 0.50 vCPU, Node 20.20.2. Effective env: maximum 10 chapter runners,
download batch 8, lease 300 s, heartbeat 20 s. Yugabyte `max_connections=13`;
importer pool remains 6. No pool or concurrency increase has been made.

The panel initially identified PR #117; the backed-up compiled concurrency artifact matched
PR #118 by SHA-256. Rollback artifact and env are preserved privately, outside the repository.

At 22:02 UTC after instrumentation deployment, runtime counters showed 3,702 SQL queries /
17 completed chapter executions (217.8 queries/execution, including maintenance), SQL p50/p95
168/659 ms, pool wait p50/p95 399/1,657 ms, six connections with none idle, seven waiting clients.
This is execution-amortized cost, **not** queries/canonical-visible chapter.

Full window 22:02–22:07 UTC:

| UTC minute | Canonical | Rate bucket |
| --- | ---: | ---: |
| 22:02 | 3 | 3 |
| 22:03 | 6 | 6 |
| 22:04 | 7 | 7 |
| 22:05 | 3 | 3 |
| 22:06 | 7 | 7 |

26 total, 5.2/min, median 6, minimum 3, maximum 7, population SD 1.83.
26 durable events, zero canonical/event timestamp mismatches, zero bucket-minute mismatches.
Effective concurrency declined from 8 to 5; pool pressure must be addressed before tuning.
Workload included Fleur Blanche, Hanami Heaven, Manhastro, Megahentai and Montetai, not a
hand-selected fast-source benchmark. Failed Mangotoons attempts also consumed capacity.

Ten probes per route: Home 10/10 HTTP 200 (TTFB p50/p95 2,294/6,164 ms), work 10/10 HTTP 200
(538/1,857 ms), Reader 0/10 with 10 HTTP 404. An older published Yugabyte chapter also returned
`Capítulo indisponível`. Site main `3f64d0d` still assigns Supabase to `locals.db`; Reader uses
that client for editorial chapters. This is a separate pre-existing consistency issue; do
not report the site as healthy or conceal these 404s.

## Second patch, awaiting deployment

- Admission: one bounded cohort snapshot replaces four SQL requests per active work;
  periodic/vacate/watchdog triggers share one in-flight cycle. No editorial-state cache.
- `EXPLAIN ANALYZE` on the actual six-work cohort: 19.1 ms server execution, 75.6 ms client
  elapsed; indexed queue/work/chapter/source access, no new indexes needed.
- Work sync now fetches only that work's active jobs, eliminating the global queue transfer
  and JavaScript filtering (historical statement: ~26k rows/call, mean 3.53 s server time).
- Shared per-job cancellation polling bounds page fan-out to one query per two seconds;
  heartbeat cancellation remains independent.
- Chapter timings distinguish additive wall-time stages from sums of overlapping page
  service times. Source fetch and metadata are actually measured; DB/media detail is DEBUG.
- Diagnostic TTL persistence corrected for production `settings.value TEXT`, with a real
  PostgreSQL-compatible regression test and rate-limited persistence warnings.
- Removed embedded Telegram bot credential fallbacks. Existing private checkpoint/env
  configuration remains the source of credentials; no tokens are written to this report.

## Validation rules

Use `NOX_AUDIT_ENV_FILE=... node scripts/production-window.mjs 5 output.json` for a bounded,
read-only window. It closes its pool and does not download chapter images. Never commit env,
rollback backups or credential-bearing logs. Extend to ten minutes only after target and
health are satisfactory; completed executions are not canonical visible chapters.

Remaining work includes lease fencing, maintenance lifecycle, classified queue hygiene,
resource-efficient concurrency tuning and a final representative production window.
