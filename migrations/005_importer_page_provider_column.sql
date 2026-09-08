-- Migration 005: Add is_page_provider column to importer_chapter_mappings
-- Distinguishes physical page provider source from linked canonical chapter mappings

ALTER TABLE public.importer_chapter_mappings
ADD COLUMN IF NOT EXISTS is_page_provider BOOLEAN NOT NULL DEFAULT true;

COMMENT ON COLUMN public.importer_chapter_mappings.is_page_provider IS
'True se esta fonte efetuou o download e upload real das páginas; False se o mapeamento foi vinculado a um capítulo canônico pré-existente.';
