ALTER TABLE public.chapters ADD COLUMN IF NOT EXISTS is_fresh_release boolean DEFAULT false;
