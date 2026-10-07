import fs from 'fs';

let content = fs.readFileSync('src/core/scheduler/admission-controller.ts', 'utf8');

if (!content.includes('private deadWorksCache = new Map<string, number>();')) {
  content = content.replace('private demandFlights = new Map<string, Promise<ActiveWork | null>>();', 'private demandFlights = new Map<string, Promise<ActiveWork | null>>();\n  private deadWorksCache = new Map<string, number>();');
}

// In admitWorks (Periodic)
content = content.replace(
  '              if (conf.confirmed) contiguous.push(cand);\n            } catch {}',
  '              if (conf.confirmed) contiguous.push(cand);\n              else this.deadWorksCache.set(cand.work_id, Date.now());\n            } catch {}'
);

// In findOnDemandFrontier
content = content.replace(
  '            if (conf.confirmed) return cand;\n          } catch {}',
  '            if (conf.confirmed) return cand;\n            else this.deadWorksCache.set(cand.work_id, Date.now());\n          } catch {}'
);

// In the periodic CTE
content = content.replace(
  "AND q.payload->>'workId' IS NOT NULL\n            ORDER BY q.priority DESC, q.chapter_sort_key ASC\n            LIMIT 32",
  "AND q.payload->>'workId' IS NOT NULL\n              AND NOT ((q.payload->>'workId') = ANY($3::text[]))\n            ORDER BY q.priority DESC, q.chapter_sort_key ASC\n            LIMIT 32"
);

// In the on-demand CTE
content = content.replace(
  "AND q.payload->>'workId' IS NOT NULL\n            ORDER BY q.priority DESC, q.chapter_sort_key ASC\n            LIMIT $5",
  "AND q.payload->>'workId' IS NOT NULL\n              AND NOT ((q.payload->>'workId') = ANY($7::text[]))\n            ORDER BY q.priority DESC, q.chapter_sort_key ASC\n            LIMIT $5"
);

// We need to change the parameters passed to runQuery in admitWorks
content = content.replace(
  "        return this.runQuery(query, [\n          allowedSources && allowedSources.length > 0 ? allowedSources : null,\n          saturatedSources && saturatedSources.length > 0 ? saturatedSources : null,\n        ]);",
  "        const deadIds = Array.from(this.deadWorksCache.keys());\n        return this.runQuery(query, [\n          allowedSources && allowedSources.length > 0 ? allowedSources : null,\n          saturatedSources && saturatedSources.length > 0 ? saturatedSources : null,\n          deadIds.length > 0 ? deadIds : ['00000000-0000-0000-0000-000000000000']\n        ]);"
);

// In on-demand loadOnDemandCandidates params
content = content.replace(
  "          16,\n          p1SourceWindow,\n        ]);",
  "          16,\n          p1SourceWindow,\n          Array.from(this.deadWorksCache.keys()).length > 0 ? Array.from(this.deadWorksCache.keys()) : ['00000000-0000-0000-0000-000000000000']\n        ]);"
);

// Expire deadWorksCache periodically
content = content.replace(
  "      if (this.loopTimer) clearTimeout(this.loopTimer);\n      this.isRunning = false;",
  "      if (this.loopTimer) clearTimeout(this.loopTimer);\n      this.isRunning = false;\n      this.deadWorksCache.clear();"
);

content = content.replace(
  "          await this.admitWorks(Math.min(2, Math.max(1, Math.floor(Math.random() * 3))));",
  "          const now = Date.now();\n          for (const [wid, ts] of this.deadWorksCache.entries()) { if (now - ts > 10 * 60 * 1000) this.deadWorksCache.delete(wid); }\n          await this.admitWorks(Math.min(2, Math.max(1, Math.floor(Math.random() * 3))));"
);

fs.writeFileSync('src/core/scheduler/admission-controller.ts', content, 'utf8');
console.log('Patch applied!');
