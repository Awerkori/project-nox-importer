import crypto from 'crypto';

const BASE_URL = 'https://manga.project-nox-awerkori.workers.dev';
const TOKEN = 'FIzfNCLxC58nSriPHiXfo8PRYRz9zzKq';
const SECRET = 'prod-secret-9876543210-abcdef';

const sig = crypto.createHmac('sha256', SECRET).update(TOKEN).digest('base64');
const signedCookie = `${TOKEN}.${sig}`;
const cookieHeader = `better-auth.session_token=${signedCookie}; __Secure-better-auth.session_token=${signedCookie}`;

async function test() {
  const res = await fetch(`${BASE_URL}/admin/importer`, {
    headers: { 'Cookie': cookieHeader, 'Accept': 'text/html' }
  });
  const html = await res.text();
  console.log('HTTP Status:', res.status);
  const m = html.match(/ESTADO:[^<]+/);
  console.log('Executive state badge in HTML:', m ? m[0] : 'Not found');
  const pill = html.match(/Importer ● [^<]+/);
  console.log('Header liveness pill in HTML:', pill ? pill[0] : 'Not found');
  const staged = html.match(/Backlog STAGED[\s\S]*?font-mono[^>]*>([^<]+)/);
  console.log('Backlog STAGED in HTML:', staged ? staged[1].trim() : 'Not found');
}

test().catch(console.error);
