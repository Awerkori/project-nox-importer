const fs = require('fs');

const testFile = 'tests/auto-heal-watchdog.test.ts';
let code = fs.readFileSync(testFile, 'utf8');

const newTest = `
  it('Caso HH: ensures eligible_cnt query excludes canonically blocked successors (CANONICAL_FRONTIER_CLAIM_FILTER)', async () => {
    const watchdog = new AutoHealWatchdog({
      pool: mockPool,
      scheduler: mockScheduler,
      admissionController: mockAdmissionController,
      protectiveSentinel: mockProtectiveSentinel,
      onControlledRestart,
    });

    let eligibleQuery = '';
    mockPool.query.mockImplementation((sql) => {
      if (sql.includes('eligible_cnt')) {
        eligibleQuery = sql;
        return { rows: [{ eligible_cnt: '0', importing_cnt: '0', retry_cnt: '0' }] };
      }
      return { rows: [] };
    });

    await watchdog.collectTelemetry(false);
    expect(eligibleQuery).not.toBe('');
    
    // Check if CANONICAL_FRONTIER_CLAIM_FILTER logic is inside the query string
    expect(eligibleQuery).toContain('SELECT 1 FROM importer_queue predecessor');
    expect(eligibleQuery).toContain("predecessor.payload->>'workId' = q.payload->>'workId'");
    expect(eligibleQuery).toContain('predecessor.chapter_sort_key < q.chapter_sort_key');
    expect(eligibleQuery).toContain('SELECT 1 FROM chapters predecessor_canonical');
    expect(eligibleQuery).toContain("predecessor_canonical.work_id = (predecessor.payload->>'workId')::uuid");
  });
`;

code = code.replace(/}\);\n$/, newTest + '\n});\n');
fs.writeFileSync(testFile, code);
