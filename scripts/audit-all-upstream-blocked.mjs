import dotenv from 'dotenv';
dotenv.config();
import { createClient } from '@supabase/supabase-js';
import { CloudflareClassifier } from '../build/core/cloudflare-classifier.js';
import { HostRateLimiter } from '../build/core/rate-limiter.js';
import { SourceRegistry } from '../build/sources/registry.js';
import { SourceAdmissionGate } from '../build/core/source-admission-gate.js';

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function probeUrl(url, referer = '') {
  try {
    const headers = {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
      'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
      'Cache-Control': 'no-cache',
      'Sec-Ch-Ua': '"Chromium";v="124", "Google Chrome";v="124"',
      'Sec-Ch-Ua-Mobile': '?0',
      'Sec-Ch-Ua-Platform': '"Windows"',
      'Sec-Fetch-Dest': 'document',
      'Sec-Fetch-Mode': 'navigate',
      'Sec-Fetch-Site': referer ? 'same-origin' : 'none',
      'Sec-Fetch-User': '?1',
      'Upgrade-Insecure-Requests': '1'
    };
    if (referer) headers['Referer'] = referer;

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 10000);
    const resp = await fetch(url, { headers, signal: controller.signal });
    clearTimeout(timer);

    const bodyText = await resp.text();
    const serverHeader = resp.headers.get('server') || '';
    const cfRay = resp.headers.get('cf-ray') || '';
    const cfMitigated = resp.headers.get('cf-mitigated') || '';
    const contentType = resp.headers.get('content-type') || '';

    const classification = CloudflareClassifier.inspect(
      resp.status,
      Object.fromEntries(resp.headers.entries()),
      bodyText
    );

    return {
      status: resp.status,
      serverHeader,
      cfRay: Boolean(cfRay),
      cfMitigated: Boolean(cfMitigated),
      contentType,
      bodyLength: bodyText.length,
      classification
    };
  } catch (err) {
    return {
      status: 0,
      error: err.message,
      classification: { isBlocked: false, isChallenge: false, isRateLimit: false, reason: 'NETWORK_ERROR' }
    };
  }
}

async function main() {
  console.log('=== AUDIT OF ALL SOURCES WITH UPSTREAM_BLOCKED STATUS ===\n');

  const { data: sources, error } = await sb
    .from('importer_sources')
    .select('id, name, base_url, status, rate_limit_per_second, sync_interval_minutes')
    .eq('status', 'UPSTREAM_BLOCKED')
    .order('id');

  if (error) throw error;
  console.log(`Loaded ${sources.length} sources dynamically from database.\n`);

  const rateLimiter = new HostRateLimiter(2.0);
  const registry = new SourceRegistry(rateLimiter);

  const results = [];

  for (const s of sources) {
    console.log(`[PROBING] ${s.id} (${s.base_url})`);
    const baseProbe = await probeUrl(s.base_url);
    const adapter = registry.get(s.id);
    let gateResult = null;

    if (adapter) {
      try {
        const gate = new SourceAdmissionGate();
        const probeReport = await gate.executeProdProbe(adapter);
        gateResult = {
          passed: probeReport.overallStatus === 'PASS',
          failedStage: probeReport.stages.find(s => s.status === 'FAIL')?.stage || 'NONE',
          reason: probeReport.stages.find(s => s.status === 'FAIL')?.detail || (probeReport.overallStatus === 'PASS' ? 'ALL_STAGES_PASS' : 'FAILED'),
          isAsnBlock: probeReport.isAsnBlock,
          recommendedState: probeReport.recommendedState,
          stages: probeReport.stages
        };
      } catch (err) {
        gateResult = { passed: false, failedStage: 'EXCEPTION', reason: err.message };
      }
    } else {
      gateResult = { passed: false, failedStage: 'NO_ADAPTER', reason: 'Adapter not found in registry' };
    }

    let finalCategory = 'UPSTREAM_BLOCKED';
    if (gateResult.passed) {
      finalCategory = 'ACTIVE';
    } else if (baseProbe.classification?.isRateLimit) {
      finalCategory = 'RATE_LIMITED';
    } else if (baseProbe.status >= 500 && baseProbe.status <= 504) {
      finalCategory = 'TEMPORARILY_UNAVAILABLE';
    } else if (baseProbe.classification?.isBlocked || baseProbe.classification?.isChallenge || baseProbe.status === 403) {
      finalCategory = 'UPSTREAM_BLOCKED';
    }

    results.push({
      id: s.id,
      name: s.name,
      baseUrl: s.base_url,
      httpStatus: baseProbe.status,
      cfRay: baseProbe.cfRay,
      cfMitigated: baseProbe.cfMitigated,
      cfReason: baseProbe.classification?.reason,
      gatePassed: gateResult.passed,
      gateStage: gateResult.failedStage || 'PASSED',
      gateReason: gateResult.reason || 'ALL_STAGES_CLEAN',
      finalCategory
    });

    console.log(`  -> Status: ${baseProbe.status} | Gate: ${gateResult.passed ? 'PASS' : 'FAIL (' + gateResult.failedStage + ')'} | Cat: ${finalCategory}`);
  }

  console.log('\n============================================================');
  console.log('AUDIT REPORT SUMMARY:');
  console.log('============================================================\n');

  for (const r of results) {
    console.log(`Source: ${r.id}`);
    console.log(`  Name:         ${r.name}`);
    console.log(`  Base URL:     ${r.baseUrl}`);
    console.log(`  HTTP Status:  ${r.httpStatus}`);
    console.log(`  Cloudflare:   ${r.cfReason}`);
    console.log(`  Gate Stage:   ${r.gateStage}`);
    console.log(`  Gate Details: ${r.gateReason}`);
    console.log(`  Category:     ${r.finalCategory}`);
    console.log('------------------------------------------------------------');
  }
}

main().catch(console.error);
