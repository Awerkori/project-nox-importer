import fs from 'fs';

let content = fs.readFileSync('src/core/publication.ts', 'utf-8');

if (!content.includes('// TELEMETRY PATCH')) {
  // Inside runCascadeUnderLock where chapters are published
  content = content.replace(
    'this.logger.info(`PUBLICATION_CASCADE: Success`, {',
    `// TELEMETRY PATCH
        this.logger.info('TELEMETRY_PUBLISHED', {
          workId: workId.substring(0,8),
          chapters: publishableMappingIds.length,
          tPublished: Date.now()
        });
        this.logger.info(\`PUBLICATION_CASCADE: Success\`, {`
  );
  
  fs.writeFileSync('src/core/publication.ts', content, 'utf-8');
  console.log("Telemetry patched into src/core/publication.ts");
} else {
  console.log("Already patched.");
}
