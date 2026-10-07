import pg from "pg";
import fs from "fs";
import { PublicationBarrier } from "./build/core/publication.js";
import { createClient } from "@supabase/supabase-js";

const envVars = Object.fromEntries(
  fs.readFileSync("/home/awerkori/.config/project-nox/yugabyte.env", "utf8")
    .split("\n")
    .filter(l => l.includes("=") && !l.startsWith("#"))
    .map(l => [l.split("=")[0].trim(), l.substring(l.indexOf("=") + 1).trim().replace(/^['"]|['"]$/g, "")])
);

process.env.YUGABYTE_HOST = envVars.YUGABYTE_HOST;
process.env.YUGABYTE_PORT = envVars.YUGABYTE_PORT || "5433";
process.env.YUGABYTE_USER = envVars.YUGABYTE_USER;
process.env.YUGABYTE_PASSWORD = envVars.YUGABYTE_PASSWORD;
process.env.YUGABYTE_DATABASE = envVars.YUGABYTE_DATABASE;
process.env.DIRECT_DB_POOL_MAX = "3";

const supabase = createClient("https://placeholder.supabase.co", "placeholder", {
  auth: { persistSession: false }
});

async function main() {
  console.log("Running live sweep with updated publication logic...");
  const barrier = new PublicationBarrier(supabase);
  const count = await barrier.sweepStagedPublications(40, 6);
  console.log(`Sweep completed: published ${count} chapter(s).`);
}

main().catch(console.error);
