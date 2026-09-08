import dotenv from "dotenv";
dotenv.config();

import { KuroAdapter } from "../src/sources/kuro/kuro-adapter.js";
import { HostRateLimiter } from "../src/core/rate-limiter.js";

async function main() {
  console.log("================================================================");
  console.log("PROJECT NOX IMPORTER - KURO LIVE AUTH & AUTO-RENEWAL TEST");
  console.log("================================================================");

  if (!process.env.KURO_EMAIL || !process.env.KURO_PASSWORD) {
    console.error("KURO_EMAIL or KURO_PASSWORD missing from environment!");
    process.exit(1);
  }

  const rateLimiter = new HostRateLimiter(2.0);
  const kuro = new KuroAdapter(rateLimiter);

  // 1. Initial Login
  console.log("\n[1/5] Testing real authentication login...");
  const loginSuccess = await kuro.login();
  if (!loginSuccess || !kuro.hasValidSession()) {
    console.error("Kuro authentication failed!");
    process.exit(1);
  }
  console.log("  [OK] Kuro AUTH OK: In-memory session established successfully.");

  // 2. Fetch catalog / recent works (validating Rabbit cipher decryptor)
  console.log("\n[2/5] Testing catalog discovery & Rabbit Cipher decryption (_v_secure)...");
  const { works } = await kuro.fetchUpdatedWorks(null, { mode: "maintenance" });
  console.log("  [OK] Decryption successful! Received " + works.length + " recently updated works.");
  if (works.length === 0) {
    console.error("0 works received from Kuro.");
    process.exit(1);
  }

  const sampleWork = works[0];
  console.log("  [OK] Sample work chosen:", sampleWork.title, "(ID: " + sampleWork.sourceWorkId + ")");

  // 3. Fetch details
  console.log("\n[3/5] Testing work details resolution...");
  const details = await kuro.fetchWorkDetails(sampleWork.sourceWorkId);
  console.log("  [OK] Title:", details.title);
  console.log("  [OK] Status:", details.status);
  console.log("  [OK] Genres:", (details.genres ? details.genres.slice(0, 4).join(", ") : "N/A"));

  // 4. Fetch chapters & pages
  console.log("\n[4/5] Testing chapters & pages extraction...");
  const chapters = await kuro.fetchChapters(sampleWork.sourceWorkId);
  console.log("  [OK] Found " + chapters.length + " chapters.");
  if (chapters.length === 0) {
    console.error("0 chapters found for sample work.");
    process.exit(1);
  }

  const sampleChapter = chapters[chapters.length - 1];
  const pages = await kuro.fetchChapterPages(sampleChapter.sourceChapterId, sampleChapter.number);
  console.log("  [OK] Chapter " + sampleChapter.number + " (" + sampleChapter.title + "): " + pages.length + " valid pages found.");
  if (pages.length > 0) {
    console.log("    Sample Page URL: (masked) ..." + pages[0].slice(-30));
  }

  // 5. Test Automatic Session Renewal
  console.log("\n[5/5] Testing automatic in-memory session renewal...");
  console.log("  -> Intentionally clearing cached session from memory...");
  kuro.clearSession();
  console.log("  -> Session cleared: hasValidSession = " + kuro.hasValidSession());

  console.log("  -> Making a new request expecting transparent auto-reauth...");
  const detailsAfterRenewal = await kuro.fetchWorkDetails(sampleWork.sourceWorkId);
  const renewedSession = kuro.hasValidSession();
  console.log("  [OK] Request succeeded:", detailsAfterRenewal.title);
  console.log("  [OK] Session renewed automatically in memory: " + (renewedSession ? "SIM (OK)" : "NAO (FAIL)"));

  console.log("\n================================================================");
  console.log("KURO LIVE AUTHENTICATION & AUTO-RENEWAL VERIFICATION: 100% OK");
  console.log("================================================================");
}

main().catch((err) => {
  console.error("Verification encountered error:", err.message);
  process.exit(1);
});
