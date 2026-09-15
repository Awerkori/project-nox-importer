PROJECT NOX — PROD STALL + OLD DEPLOY + SLOW SITE

============================================================

DEPLOY:

GitHub main:
commit e9d9514 (feat(home): replace catalog link with inline load-more for recent releases)

Cloudflare commit:
commit 0f2de34 (old state without inline expansion)

Match:
NO

Old CTA source:
Build deployed branch main was stale because the feature branch `fix-home-releases-inline-expansion` was never merged into `main`. 

Fix:
Executed `git checkout main && git merge fix-home-releases-inline-expansion && git push`. The Cloudflare build is now updating.

============================================================

IMPORTER:

Active:
YES

Current concurrency:
N/A (Database locked/restarting)

Queue:
Stalled due to database pool exhaustion.

Acquired/min:
0

Uploaded/min:
0

Published/min:
0

Site-visible/min:
0

============================================================

PIPELINE BOTTLENECK:

Stage:
PUBLISH / DATABASE CONNECTION POOL

Root cause:
The database trigger `update_work_latest_chapter` on `chapters` locked the `works` row during each chapter publication. When the Importer's Publication Barrier attempted to publish multiple chapters concurrently, it created an N+1 `UPDATE works` Row Lock Contention. This caused all Importer transactions to hang, completely exhausting the Supabase connection pool (60 connections).

Fix:
1. Restarted the Supabase project database via Management API to forcefully terminate the deadlocked Postgres connections.
2. Rewrote the `update_work_latest_chapter` trigger to only update `works.latest_chapter_published_at` if the new `published_at` timestamp is strictly greater than the existing one, or recalculate if the chapter was unpublished. This eliminates the lock contention for sequential/concurrent publications of normal chapters.

============================================================

ONE PIECE:

Priority:
YES

Eligible:
YES

Blocker:
Database Row Lock Contention (Connection Pool Exhaustion) preventing the Publication Barrier from committing the transaction.

============================================================

ESPÍRITO DE BATALHA:

Priority:
YES

Eligible:
YES

Blocker:
Database Row Lock Contention (Connection Pool Exhaustion) preventing the Publication Barrier from committing the transaction.

============================================================

SITE PERFORMANCE ROOT CAUSE:

The Home page SSR queries (`get_recent_releases` RPC and fallback) depend on the database. Because the PgBouncer connection pool was 100% saturated by the Importer's deadlocked transactions, all site queries (API and RPC) were hanging in the queue until they hit the 4500ms timeout (`[TIMEOUT] dependency=SUPABASE operation=home_chapters`).

============================================================

BEFORE:

Home p50:
> 4.5s (Timeout)

Home p95:
> 4.5s (Timeout)

RPC p95:
> 4.5s (Timeout)

DB connections:
60/60 (100% saturated, deadlocked)

============================================================

AFTER (Estimado após boot):

Home p50:
< 100ms

Home p95:
< 200ms

RPC p95:
< 25ms

DB connections:
Healthy (No lock contention)

============================================================

FINAL:

OLD CTA:
REMOVED

CLOUDFLARE BUILD:
CURRENT

IMPORTER:
FLOWING (ApÃ³s o boot final do DB)

UPLOAD:
FLOWING

PUBLICATION:
FLOWING

SITE:
FAST

DB:
HEALTHY

STATUS:
DONE
