import fs from 'node:fs';

async function probeDomain(baseUrl) {
  if (!baseUrl || !baseUrl.startsWith('http')) return { status: 'NO_URL', error: 'Missing or invalid URL' };
  try {
    const t0 = Date.now();
    const res = await fetch(baseUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
      },
      signal: AbortSignal.timeout(8000),
      redirect: 'follow'
    });
    const duration = Date.now() - t0;
    const finalUrl = res.url;
    const html = await res.text();

    let theme = 'custom';
    if (html.includes('wp-manga') || html.includes('madara') || html.includes('wp-content/uploads/WP-manga')) {
      theme = 'madara';
    } else if (html.includes('mangathemesia') || html.includes('theme-mangathemesia') || html.includes('class="bsx"')) {
      theme = 'mangathemesia';
    } else if (html.includes('zeistmanga') || html.includes('blogger.com') || html.includes('blogspot.com')) {
      theme = 'zeistmanga';
    } else if (html.includes('wp-content') || html.includes('wp-includes')) {
      theme = 'wordpress';
    }

    let isBlocked = false;
    let blockReason = null;
    if (res.status === 403 || res.status === 503) {
      if (html.includes('cf-browser-verification') || html.includes('challenge-running') || html.includes('Cloudflare')) {
        isBlocked = true;
        blockReason = 'CLOUDFLARE_CHALLENGE_403';
      } else {
        isBlocked = true;
        blockReason = `HTTP_${res.status}`;
      }
    }

    return {
      status: res.ok ? 'ONLINE' : (isBlocked ? 'BLOCKED' : `HTTP_${res.status}`),
      httpCode: res.status,
      finalUrl,
      durationMs: duration,
      detectedTheme: theme,
      blocked: isBlocked,
      blockReason,
      hasContent: html.length > 500
    };
  } catch (err) {
    let errorType = 'UNKNOWN_ERROR';
    if (err.message.includes('ENOTFOUND') || err.message.includes('EAI_AGAIN')) {
      errorType = 'DNS_DEAD_ENOTFOUND';
    } else if (err.message.includes('ECONNREFUSED')) {
      errorType = 'CONN_REFUSED';
    } else if (err.name === 'TimeoutError' || err.message.includes('timeout')) {
      errorType = 'TIMEOUT';
    } else if (err.message.includes('certificate')) {
      errorType = 'SSL_CERT_ERROR';
    }
    return {
      status: 'OFFLINE_OR_ERROR',
      httpCode: null,
      finalUrl: baseUrl,
      error: errorType,
      message: err.message.slice(0, 100)
    };
  }
}

async function main() {
  const comparison = JSON.parse(fs.readFileSync('extensions_importer_comparison.json'));
  const missing = comparison.filter(c => !c.inImporterDb);
  console.log(`Probing ${missing.length} missing extensions...`);

  const results = [];
  // Process in small batches of 6
  const BATCH_SIZE = 6;
  for (let i = 0; i < missing.length; i += BATCH_SIZE) {
    const chunk = missing.slice(i, i + BATCH_SIZE);
    const promises = chunk.map(async (ext) => {
      console.log(`Probing [${ext.extensionId}] (${ext.baseUrl})...`);
      const probe = await probeDomain(ext.baseUrl);
      return {
        ...ext,
        probe
      };
    });
    const chunkRes = await Promise.all(promises);
    results.push(...chunkRes);
  }

  fs.writeFileSync('missing_extensions_probed.json', JSON.stringify(results, null, 2));
  console.log('Saved probed results to missing_extensions_probed.json');

  const online = results.filter(r => r.probe.status === 'ONLINE');
  const blocked = results.filter(r => r.probe.status === 'BLOCKED');
  const offline = results.filter(r => r.probe.status === 'OFFLINE_OR_ERROR');
  const other = results.filter(r => r.probe.status !== 'ONLINE' && r.probe.status !== 'BLOCKED' && r.probe.status !== 'OFFLINE_OR_ERROR');

  console.log('\n--- PROBE RESULTS SUMMARY ---');
  console.log(`Total probed: ${results.length}`);
  console.log(`Online: ${online.length}`);
  console.log(`Blocked: ${blocked.length}`);
  console.log(`Offline/Error: ${offline.length}`);
  console.log(`Other: ${other.length}`);
}

main().catch(console.error);
