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
| Kuro | Recurrent Cloudflare authentication challenge in the production runtime | Retired from discovery, import and metadata lanes; historical published chapters remain available | Other confirmed routes when present | Permanently retired; do not recreate mappings or tasks |
| Taimu Mangas | One discovery request failed | Temporary discovery failure only; permanence not established | Other confirmed routes when present | Keep with cooldown/backoff; investigate with a later sample |

Aside from Kuro's subsequent permanent retirement, this audit found no
additional candidate for removal. Lite stores source health and cooldown
separately from priority and can select a confirmed alternative route without
allowing a preferred source to block the task.
