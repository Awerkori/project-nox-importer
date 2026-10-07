import fs from 'fs';

let content = fs.readFileSync('src/core/scheduler/admission-controller.ts', 'utf8');

content = content.replace(
  "  stop(): void {\n    this.isRunning = false;",
  "  stop(): void {\n    this.isRunning = false;\n    this.deadWorksCache.clear();"
);

fs.writeFileSync('src/core/scheduler/admission-controller.ts', content, 'utf8');
console.log('Patch applied!');
