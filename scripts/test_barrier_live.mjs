import { getYugabytePool } from '/home/awerkori/.Projects/project-nox-importer/build/db/yugabyte-direct.js';
import { DirectSupabaseClient } from '/home/awerkori/.Projects/project-nox-importer/build/db/direct-supabase-client.js';
import { PublicationBarrier } from '/home/awerkori/.Projects/project-nox-importer/build/core/publication.js';

const pool = getYugabytePool();
const client = new DirectSupabaseClient(pool);
const barrier = new PublicationBarrier(client);

async function test() {
  console.log('Testing PublicationBarrier against Apotheosis (a1e13c33-fb07-4326-b054-7834a25f17a0), sortKey 871:');
  const res = await barrier.checkBarrier('a1e13c33-fb07-4326-b054-7834a25f17a0', 871);
  console.log('CheckBarrier result:', res);

  console.log('\nRunning sweepStagedPublications now:');
  const count = await barrier.sweepStagedPublications(40, 6);
  console.log('Sweep published count:', count);
}

test().catch(console.error).finally(() => pool.end());
