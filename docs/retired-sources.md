# Retired sources

This is a decision history, not a permanent blacklist. Retiring an adapter
removes it from the active importer registry and normal probes; it does not
delete historical works, chapters, mappings, or provenance already stored by
Project Nox. A source may be restored after a fresh end-to-end validation
(catalog, details, chapters, Reader pages and an image download).

The audited retirement is also persisted as `DISABLED` with both ingestion
flags off in `importer_sources`; this only stops future admission and preserves
the historical rows.

## MegaHentai (`megahentai`)

- **Known URL:** `https://megahentai.biz`
- **Retired:** 2026-10-17 UTC
- **Reason:** the public Reader no longer exposes chapter images to the
  importer.
- **Last observed failure:** five current works, including multiple first
  chapters, returned only an external “unlock”/advertising jump in
  `#content.cap`; none exposed the `gall`/`static` image URLs required by the
  current Mihon implementation.
- **Tried:** importer parser and the current Mihon selector/Reader contract
  against catalog, detail, chapter and Reader pages.
- **Why this is not sustainable:** following or automating an advertising
  unlock/gate is neither a stable content route nor an acceptable importer
  dependency.
- **Re-evaluate if:** the public Reader again returns direct chapter image URLs
  from an official, non-gated endpoint.

## Osaka Scan (`osakascan`)

- **Known URL:** `https://www.osakascan.com`
- **Retired:** 2026-10-17 UTC
- **Reason:** the original Blogger/Zeist feed endpoint is no longer usable.
- **Last observed failure:** HTTPS failed during TLS negotiation and the HTTP
  Blogger feed returned the provider’s “domain expired” page. The current Mihon
  extension still references the same domain and feed structure.
- **Tried:** current importer adapter, the current Mihon extension’s feed/API
  route, both HTTPS and HTTP public endpoints.
- **Why this is not sustainable:** there is no live official catalog, Reader
  or successor endpoint to import from.
- **Re-evaluate if:** an official successor domain is verified with a working
  catalog, chapters and public Reader images.
