import https from 'https';

async function fetchTime(url) {
  return new Promise((resolve) => {
    const start = Date.now();
    https.get(url, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        resolve({
           status: res.statusCode,
           time: Date.now() - start
        });
      });
    }).on('error', (e) => {
      resolve({ status: 500, time: Date.now() - start, error: e.message });
    });
  });
}

function percentile(arr, p) {
    if (arr.length === 0) return 0;
    arr.sort((a, b) => a - b);
    const index = (p / 100) * (arr.length - 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);
    const weight = index % 1;
    if (upper >= arr.length) return arr[lower];
    return arr[lower] * (1 - weight) + arr[upper] * weight;
}

async function measureRoute(name, url, iterations = 20) {
   console.log(`Measuring ${name} (${url})...`);
   const times = [];
   const statuses = {};
   for (let i = 0; i < iterations; i++) {
       const { status, time } = await fetchTime(url);
       times.push(time);
       statuses[status] = (statuses[status] || 0) + 1;
       // Sleep 100ms
       await new Promise(r => setTimeout(r, 100));
   }
   
   console.log(`[${name}] p50: ${Math.round(percentile(times, 50))}ms | p95: ${Math.round(percentile(times, 95))}ms`);
   console.log(`[${name}] Statuses:`, statuses);
   return { p50: Math.round(percentile(times, 50)), p95: Math.round(percentile(times, 95)), statuses };
}

async function run() {
  const baseUrl = 'https://manga.project-nox-awerkori.workers.dev';
  await measureRoute('Home', `${baseUrl}/`);
  await measureRoute('Catalog', `${baseUrl}/catalogo`);
  
  // Need a work URL. Assuming 'minha-nova-familia-me-trata-muito-bem'
  await measureRoute('Work', `${baseUrl}/obra/minha-nova-familia-me-trata-muito-bem`);
  
  // Need a reader URL. Assuming /ler/some-id
  await measureRoute('Reader (Invalid)', `${baseUrl}/ler/invalid-id`);
}

run();
