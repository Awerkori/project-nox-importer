import fs from 'fs';

let content = fs.readFileSync('src/core/engine.ts', 'utf-8');

// Ensure we don't patch twice
if (!content.includes('// TELEMETRY PATCH')) {
  // Inject at the beginning of handleImportChapter
  content = content.replace(
    'const tStart = Date.now();',
    `const tStart = Date.now();
    // TELEMETRY PATCH
    const telemetry = {
      jobId: job.id,
      source: effectiveSource,
      workId: workId.substring(0,8),
      chapter: chapterNumber,
      tStart,
      tDownloadStart: 0,
      tDownloadEnd: 0,
      tUploadStart: 0,
      tUploadEnd: 0,
      tStaged: 0,
      tPublished: 0,
      totalBytesDown: 0,
      totalBytesUp: 0,
      pages: 0
    };`
  );

  // Producer start
  content = content.replace(
    'const producerPromises = Array.from({ length: downloadConcurrency }, () => producer());',
    `telemetry.tDownloadStart = Date.now();
        const producerPromises = Array.from({ length: downloadConcurrency }, () => producer());`
  );

  // After producer finishes
  content = content.replace(
    'await Promise.all(producerPromises);',
    `await Promise.all(producerPromises);
          telemetry.tDownloadEnd = Date.now();`
  );

  // Consumer start
  content = content.replace(
    'const consumerPromises = Array.from({ length: uploadConcurrency }, () => consumer());',
    `telemetry.tUploadStart = Date.now();
        const consumerPromises = Array.from({ length: uploadConcurrency }, () => consumer());`
  );

  // After consumer finishes
  content = content.replace(
    'await Promise.all(consumerPromises);',
    `await Promise.all(consumerPromises);
          telemetry.tUploadEnd = Date.now();
          telemetry.totalBytesDown = totalBytes;
          telemetry.pages = expectedCount;`
  );

  // Find where it succeeds
  content = content.replace(
    `successfulExecution = true;
          break; // successfully rescued/processed`,
    `successfulExecution = true;
          telemetry.tStaged = Date.now();
          this.logger.info('TELEMETRY_JOB_STAGED', telemetry);
          break; // successfully rescued/processed`
  );
  
  fs.writeFileSync('src/core/engine.ts', content, 'utf-8');
  console.log("Telemetry patched into src/core/engine.ts");
} else {
  console.log("Already patched.");
}
