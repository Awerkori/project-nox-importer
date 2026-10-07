import fs from 'fs';

let content = fs.readFileSync('src/core/scheduler/admission-controller.ts', 'utf8');

// In admitWorks (Periodic) CTE
content = content.replace(
  "AND q.payload->>'workId' IS NOT NULL\n             ORDER BY q.priority DESC, q.chapter_sort_key ASC\n             LIMIT $5",
  "AND q.payload->>'workId' IS NOT NULL\n               AND NOT ((q.payload->>'workId') = ANY($7::text[]))\n             ORDER BY q.priority DESC, q.chapter_sort_key ASC\n             LIMIT $5"
);

content = content.replace(
  "          Math.max(64, backfillSlotsAvailable * 32),\n          p1SourceWindow,\n        ]",
  "          Math.max(64, backfillSlotsAvailable * 32),\n          p1SourceWindow,\n          Array.from(this.deadWorksCache.keys()).length > 0 ? Array.from(this.deadWorksCache.keys()) : ['00000000-0000-0000-0000-000000000000']\n        ]"
);

fs.writeFileSync('src/core/scheduler/admission-controller.ts', content, 'utf8');
console.log('Patch applied!');
