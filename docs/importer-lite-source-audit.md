# Controlled source audit

Method: one `fetchUpdatedWorks(..., { mode: 'maintenance' })` call per
registered adapter, at most four concurrent calls; no chapter/page download,
no authentication bypass, no source configuration write. The check was run
against the actual adapter registry and is a point-in-time health sample, not
a permanent availability guarantee.

- Registered adapters checked: 53
- Successful discovery responses: 51
- MangaFlix and Manhastro: successful in this sample
- The seven zero-completion historical candidates (Acervo Hentai, Amuy,
  ArthurScan, Inkapk, MangaOnline, Yaoi Fan Club and YuriVerso): all returned
  non-empty discovery pages in a separate controlled sample. They are not
  candidates for removal.

| Source | Problem proven by the sample | Impact | Alternative | Recommendation |
|---|---|---|---|---|
| Kuro | Adapter reported expired/invalid authentication | Discovery unavailable in this run; no access workaround attempted | Other confirmed routes when present | Correct credentials/session; keep mappings and source enabled |
| Taimu Mangas | One discovery request failed | Temporary discovery failure only; permanence not established | Other confirmed routes when present | Keep with cooldown/backoff; investigate with a later sample |

There are **no candidates to disable or remove** from this audit. In
particular, neither a single failed request nor a historical queue count is
sufficient evidence for a permanent removal. Lite stores source health and
cooldown separately from priority and can select a confirmed alternative route
without allowing a preferred source to block the task.
