import { DirectSupabaseClient } from '../build/db/direct-supabase-client.js';
import { DirectTelegramStorageProvider } from '../build/storage/direct-telegram.js';
import { SourceRegistry } from '../build/sources/registry.js';
import { HostRateLimiter } from '../build/core/rate-limiter.js';
import { ImporterEngine } from '../build/core/engine.js';
import { getConfig } from '../build/config.js';

async function repair() {
  const config = getConfig();
  const supabase = new DirectSupabaseClient();
  const storage = new DirectTelegramStorageProvider();
  const rateLimiter = new HostRateLimiter(5.0);
  const registry = new SourceRegistry(rateLimiter, config.NOX_STORAGE_BRIDGE_TOKEN, config.NOX_MANGA_URL);
  const engine = new ImporterEngine(supabase, storage, registry, rateLimiter, config);

  const botUserId = await engine.resolveBotUserId();
  console.log('Bot User ID:', botUserId);

  const targetWorkIds = [
    '3fdff186-82df-4786-acd1-d8e428741620', // O Retorno do Cão de Caça dos Baskerville
    '0df99cda-844e-49f4-a978-f1d46a0c5a22', // Nugu Complex
    '05a9a7bd-1394-4fa0-a6f8-2f19bddb5d65', // Sakanai Hana ni Mizuyari
    '21424358-7787-4c62-a66b-d4443e47cfed', // O Filho Caçula do Conde é um Jogador
    '0c48fe3e-f297-4179-a5ec-26b2a2d10ed7'  // Hunger For You
  ];

  for (const workId of targetWorkIds) {
    console.log(`\n--- Repairing cover for workId: ${workId} ---`);
    const { data: work } = await supabase.from('works').select('id, title, slug, cover_id, published').eq('id', workId).single();
    console.log('Current work state:', work);

    const recoveredMediaId = await engine.ensureWorkHasCover(workId, botUserId);
    console.log('ensureWorkHasCover result:', recoveredMediaId);

    if (recoveredMediaId) {
      // With a valid cover recovered, publish the work if chapters are published
      await supabase.from('works').update({
        published: true,
        updated_at: new Date().toISOString()
      }).eq('id', workId);
      console.log(`Work ${work?.title} successfully repaired and published!`);
    } else {
      console.warn(`Could not recover cover for work ${work?.title}`);
    }
  }

  process.exit(0);
}

repair().catch(e => {
  console.error('Fatal repair error:', e);
  process.exit(1);
});
