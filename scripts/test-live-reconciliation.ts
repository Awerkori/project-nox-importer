import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
import { SourceRegistry } from '../src/sources/registry.js';
import { ExistingWorksReconciler } from '../src/core/reconciliation.js';
import { ImporterQueue } from '../src/core/queue.js';
import { HostRateLimiter } from '../src/core/rate-limiter.js';

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

const rateLimiter = new HostRateLimiter(2.0);
const registry = new SourceRegistry(rateLimiter);
const queue = new ImporterQueue(supabase, 'live-reconciliation-tester');
const reconciler = new ExistingWorksReconciler(supabase, queue, registry);

const TEST_WORKS = [
  { name: 'Case A: SandLand', id: '821d7f45-20b7-44fc-ae6a-e8ff203b2fe4' },
  { name: 'Case B: Céu Distante', id: 'c08a2531-7bf3-4324-979a-f7de0e66a62d' },
  { name: 'Case C: O Mestre da Espada Genial da Academia', id: '91a2197f-2e8c-4c7b-bda0-75e8053a22bf' },
  { name: 'Case D: Koko ni Iru yo!', id: '6bea7743-ed20-449f-9396-a9d37b50914c' },
  { name: 'Case E: Jogador de Nível Máximo', id: '95cce99b-9051-4fa8-b5bf-fed135cab184' },
];

async function runLiveAudit() {
  console.log('================================================================');
  console.log('LIVE OPERATIONAL AUDIT: CROSS-PROVIDER RECONCILIATION & MANIFEST');
  console.log('================================================================\n');

  for (const item of TEST_WORKS) {
    console.log(`>>> TESTING ${item.name} (${item.id})`);

    // ANTES
    const { data: mappingsBefore } = await supabase
      .from('importer_work_mappings')
      .select('source, source_work_id')
      .eq('work_id', item.id);

    const { data: chaptersBefore } = await supabase
      .from('chapters')
      .select('number')
      .eq('work_id', item.id)
      .order('number', { ascending: true });

    const numsBefore = (chaptersBefore || []).map((c) => Number(c.number));
    console.log(`[ANTES] Fontes Mapeadas: ${mappingsBefore?.map((m) => m.source).join(', ') || 'Nenhuma'}`);
    console.log(`[ANTES] Capítulos no DB: ${numsBefore.length} (Min: ${numsBefore[0]}, Max: ${numsBefore[numsBefore.length - 1]})`);

    // RECONCILE
    console.log(`[EXEC] Executando reconcileWorkManifest...`);
    const result = await reconciler.reconcileWorkManifest(item.id);

    // DEPOIS
    const { data: mappingsAfter } = await supabase
      .from('importer_work_mappings')
      .select('source, source_work_id, confidence_score, match_method')
      .eq('work_id', item.id);

    const { data: manifestCount } = await supabase
      .from('importer_chapter_manifest')
      .select('status, selected_source')
      .eq('work_id', item.id);

    const { data: healthRow } = await supabase
      .from('importer_work_health')
      .select('*')
      .eq('work_id', item.id)
      .maybeSingle();

    console.log(`[DEPOIS] Fontes Mapeadas (${mappingsAfter?.length}): ${mappingsAfter?.map((m) => `${m.source} (${m.match_method || 'MAP'})`).join(', ')}`);
    console.log(`[DEPOIS] Manifesto Total Conhecido: ${result.totalKnownChapters} capítulos`);
    console.log(`[DEPOIS] Capítulos Já Importados: ${result.totalImportedChapters}`);
    console.log(`[DEPOIS] Capítulos Enfileirados para Backfill: ${result.enqueuedCount}`);
    console.log(`[DEPOIS] Faltando Início: ${result.missingStart}`);
    console.log(`[DEPOIS] Gaps Detectados: ${JSON.stringify(result.gaps)}`);
    console.log(`[DEPOIS] Gaps Irresolvíveis (UNRESOLVED_GAP): ${JSON.stringify(result.unresolvedGaps)}`);
    console.log(`[DEPOIS] Status de Saúde: ${result.healthStatus}`);
    console.log(`[DEPOIS] Resumo dos Provedores:`, JSON.stringify(result.providersSummary));
    console.log('----------------------------------------------------------------\n');
  }

  console.log('Live operational audit completed successfully!');
}

runLiveAudit().catch((err) => {
  console.error('Fatal error in live audit:', err);
  process.exit(1);
});
