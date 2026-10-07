import fs from 'fs';

let content = fs.readFileSync('src/core/scheduler/admission-controller.ts', 'utf8');

content = content.replace(
  "  private async executeAdmissionCycle(): Promise<void> {\n    const config = this.stateStore.getConfig();",
  "  private async executeAdmissionCycle(): Promise<void> {\n    const now = Date.now();\n    for (const [wid, ts] of this.deadWorksCache.entries()) { if (now - ts > 10 * 60 * 1000) this.deadWorksCache.delete(wid); }\n    const config = this.stateStore.getConfig();"
);

fs.writeFileSync('src/core/scheduler/admission-controller.ts', content, 'utf8');
console.log('Patch applied!');
