import dotenv from "dotenv";
dotenv.config({ path: "/home/awerkori/.Projects/project-nox-importer/.env" });
import { createClient } from "@supabase/supabase-js";
import { HostRateLimiter } from "../build/core/rate-limiter.js";
import { SourceRegistry } from "../build/sources/registry.js";
import { NoxWorkerStorageProvider } from "../build/storage/worker.js";
import { ImporterEngine } from "../build/core/engine.js";
import { getConfig } from "../build/config.js";

async function main() {
  const jobId = "e97b2e0f-0736-4436-a7a9-913c4a00be33"; // Chapter 4 of work 14941714
  console.log("=== PROJECT NOX: CONTROLLED DRAIN & GAP VERIFICATION ===");
  const config = getConfig();
  config.WORKER_ID = "emergency-recovery-1";

  const supabase = createClient(config.SUPABASE_URL, config.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false },
  });

  // Check state of chapter 4, 5, 6, 7 before run
  const { data: beforeMappings } = await supabase
    .from("importer_chapter_mappings")
    .select("chapter_number, status, is_gap")
    .eq("work_id", "14941714-69b9-4183-bf65-6c4ca27dbea7")
    .in("chapter_number", [4, 5, 6, 7])
    .order("chapter_number", { ascending: true });

  console.log("Mappings before:", beforeMappings);

  // Set barrier to CAUTION so gap can process while backfill remains blocked
  await supabase.from("settings").upsert({
    key: "publication_safety_barrier",
    value: "CAUTION"
  });
  console.log("Publication safety barrier set to: CAUTION");

  // Lock the job for recovery worker
  const { data: job, error: lockErr } = await supabase
    .from("importer_queue")
    .update({
      status: "IMPORTING",
      locked_by: "emergency-recovery-1",
      locked_at: new Date().toISOString(),
      lease_expires_at: new Date(Date.now() + 600000).toISOString(),
      updated_at: new Date().toISOString()
    })
    .eq("id", jobId)
    .select("*")
    .single();

  if (lockErr || !job) {
    console.error("Lock error:", lockErr);
    return;
  }

  console.log("Locked Chapter 4 Job:", {
    id: job.id,
    source: job.source,
    workId: job.payload?.workId,
    chapterNumber: job.payload?.chapterNumber
  });

  const storage = new NoxWorkerStorageProvider(config.NOX_MANGA_URL, config.NOX_STORAGE_BRIDGE_TOKEN, fetch);
  const rateLimiter = new HostRateLimiter(2.0);
  const registry = new SourceRegistry(rateLimiter, config.NOX_STORAGE_BRIDGE_TOKEN, config.NOX_MANGA_URL);
  const engine = new ImporterEngine(supabase, storage, registry, rateLimiter, config);

  const t0 = Date.now();
  console.log("Executing Chapter 4 import and cascade...");
  await engine['executeJobDirectly'](job);
  console.log(`Chapter 4 execution finished in ${Date.now() - t0}ms`);

  // Check state of chapter 4, 5, 6, 7 after run
  const { data: afterMappings } = await supabase
    .from("importer_chapter_mappings")
    .select("chapter_number, status, is_gap")
    .eq("work_id", "14941714-69b9-4183-bf65-6c4ca27dbea7")
    .in("chapter_number", [4, 5, 6, 7])
    .order("chapter_number", { ascending: true });

  console.log("\nMappings after:", afterMappings);

  // Check published chapters in public.chapters
  const { data: pubChapters } = await supabase
    .from("chapters")
    .select("number, published_at")
    .eq("work_id", "14941714-69b9-4183-bf65-6c4ca27dbea7")
    .in("number", [1, 2, 3, 4, 5, 6, 7])
    .order("number", { ascending: true });

  console.log("\nPublished chapters in public.chapters:", pubChapters);
}

main().catch(console.error);
