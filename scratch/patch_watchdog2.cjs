const fs = require('fs');
let code = fs.readFileSync('src/core/auto-heal-watchdog.ts', 'utf8');

const oldFrom = `FROM importer_queue q
          LEFT JOIN importer_sources s ON q.source = s.id
          WHERE q.status IN ('QUEUED', 'RETRY')`;

const newFrom = `FROM importer_queue q
          LEFT JOIN importer_sources s ON q.source = s.id
          JOIN works w ON w.id = (q.payload->>'workId')::uuid
          WHERE q.status IN ('QUEUED', 'RETRY')
            AND w.published IS TRUE`;

code = code.replace(oldFrom, newFrom);
fs.writeFileSync('src/core/auto-heal-watchdog.ts', code);
console.log('Patched watchdog 2');
