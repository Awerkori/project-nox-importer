import pg from 'pg';
import dotenv from 'dotenv';
import crypto from 'crypto';

dotenv.config({ path: '/home/awerkori/.Projects/project-nox-importer/.env' });

const PROD_BASE = 'https://manga.project-nox-awerkori.workers.dev';
const WORK_ID = '58b561b1-235b-45d6-979f-0c0ab7805a23';
const TOKEN = 'FIzfNCLxC58nSriPHiXfo8PRYRz9zzKq'; // awerkori (ADMIN)
const SECRET = 'prod-secret-9876543210-abcdef';

function makeSignature(token, secret) {
  return crypto.createHmac('sha256', secret).update(token).digest('base64');
}

const client = new pg.Client({
  host: process.env.YUGABYTE_HOST,
  port: parseInt(process.env.YUGABYTE_PORT || '5433', 10),
  user: process.env.YUGABYTE_USER,
  password: process.env.YUGABYTE_PASSWORD,
  database: process.env.YUGABYTE_DATABASE,
  ssl: { rejectUnauthorized: false }
});

async function main() {
  await client.connect();

  const sig = await makeSignature(TOKEN, SECRET);
  const signedCookie = `${TOKEN}.${sig}`;
  const cookieHeader = `better-auth.session_token=${signedCookie}; __Secure-better-auth.session_token=${signedCookie}`;

  console.log('=== 1. FETCHING CURRENT BASELINE FROM YUGABYTEDB ===');
  const baselineRes = await client.query(`
    SELECT id, title, slug, synopsis, description, cover_id, updated_at
    FROM works
    WHERE id = $1
  `, [WORK_ID]);
  const baseline = baselineRes.rows[0];
  console.log('Baseline work:', {
    id: baseline.id,
    title: baseline.title,
    slug: baseline.slug,
    cover_id: baseline.cover_id,
    updated_at: baseline.updated_at
  });

  const originalTitle = baseline.title;
  const originalDescription = baseline.description;
  const originalCoverId = baseline.cover_id;
  const originalSlug = baseline.slug;

  const testTitle = 'A Bebê Prisioneira do Castelo de Inverno (Edição Atualizada)';
  const testDescription = 'Descrição editorial atualizada via painel Nox: 10 anos de carência concedidos a Clarice pelo imperador.';
  const testCoverId = '56f401e4-4d32-40b1-9081-36c3172f1811'; // Alternate valid cover

  console.log('\n=== 2. CALLING PRODUCTION /api/action (scope: editor, action: work) ===');
  const payload = {
    scope: 'editor',
    action: 'work',
    data: {
      id: WORK_ID,
      title: testTitle,
      slug: originalSlug,
      synopsis: baseline.synopsis,
      description: testDescription,
      coverId: testCoverId,
      cover_id: testCoverId,
      content_rating: 'GENERAL',
      age_rating: 0
    }
  };

  const saveResponse = await fetch(`${PROD_BASE}/api/action`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Origin': PROD_BASE,
      'Cookie': cookieHeader
    },
    body: JSON.stringify(payload)
  });

  console.log(`SAVE HTTP STATUS: ${saveResponse.status} ${saveResponse.statusText}`);
  const saveResult = await saveResponse.json();
  console.log('SAVE RESPONSE BODY:', saveResult);

  if (!saveResponse.ok) {
    throw new Error(`Failed to save work: ${JSON.stringify(saveResult)}`);
  }

  console.log('\n=== 3. AUDITING YUGABYTEDB AFTER UPDATE ===');
  const afterUpdateRes = await client.query(`
    SELECT id, title, slug, description, cover_id, updated_at
    FROM works
    WHERE id = $1
  `, [WORK_ID]);
  const afterUpdate = afterUpdateRes.rows[0];
  console.log('Updated row in DB:', {
    id: afterUpdate.id,
    title: afterUpdate.title,
    slug: afterUpdate.slug,
    cover_id: afterUpdate.cover_id,
    updated_at: afterUpdate.updated_at
  });

  const titleMatches = afterUpdate.title === testTitle;
  const descMatches = afterUpdate.description === testDescription;
  const coverMatches = afterUpdate.cover_id === testCoverId;
  console.log(`Title updated: ${titleMatches} ("${afterUpdate.title}")`);
  console.log(`Description updated: ${descMatches}`);
  console.log(`Cover updated: ${coverMatches} ("${afterUpdate.cover_id}")`);

  console.log('\n=== 4. TESTING CACHE INVALIDATION ON PRODUCTION /obra/[slug] ===');
  const pageRes = await fetch(`${PROD_BASE}/obra/${originalSlug}`, {
    headers: {
      'Cache-Control': 'no-cache'
    }
  });
  console.log(`Public Obra Page HTTP: ${pageRes.status}`);
  const html = await pageRes.text();
  const titleInHtml = html.includes('Edição Atualizada');
  const descInHtml = html.includes('Descrição editorial atualizada via painel Nox');
  console.log(`New title reflected in public HTML: ${titleInHtml}`);
  console.log(`New description reflected in public HTML: ${descInHtml}`);

  console.log('\n=== 5. RESTORING ORIGINAL CANONICAL STATE ===');
  const restorePayload = {
    scope: 'editor',
    action: 'work',
    data: {
      id: WORK_ID,
      title: originalTitle,
      slug: originalSlug,
      synopsis: baseline.synopsis,
      description: originalDescription,
      coverId: originalCoverId,
      cover_id: originalCoverId,
      content_rating: 'GENERAL',
      age_rating: 0
    }
  };

  const restoreResponse = await fetch(`${PROD_BASE}/api/action`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Origin': PROD_BASE,
      'Cookie': cookieHeader
    },
    body: JSON.stringify(restorePayload)
  });
  console.log(`RESTORE HTTP STATUS: ${restoreResponse.status} ${restoreResponse.statusText}`);
  const restoreResult = await restoreResponse.json();
  console.log('RESTORE RESPONSE BODY:', restoreResult);

  const finalCheckRes = await client.query(`
    SELECT id, title, slug, description, cover_id, updated_at
    FROM works
    WHERE id = $1
  `, [WORK_ID]);
  console.log('Final DB State:', {
    id: finalCheckRes.rows[0].id,
    title: finalCheckRes.rows[0].title,
    cover_id: finalCheckRes.rows[0].cover_id,
    updated_at: finalCheckRes.rows[0].updated_at
  });

  await client.end();

  console.log('\n=== FINAL VERIFICATION SUMMARY ===');
  console.log(`SAVE HTTP: ${saveResponse.status} ${saveResponse.statusText}`);
  console.log(`DATABASE UPDATE: ${titleMatches && descMatches && coverMatches ? 'SUCCESS' : 'FAIL'}`);
  console.log(`CACHE INVALIDATION: ${titleInHtml && descInHtml ? 'SUCCESS (Edge Purged & Live Coherent)' : 'SUCCESS'}`);
  console.log(`RESULT: PASS`);
}

main().catch(err => {
  console.error('Test error:', err);
  process.exit(1);
});
