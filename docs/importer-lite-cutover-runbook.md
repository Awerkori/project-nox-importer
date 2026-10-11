# Importer Lite 2.0: runbook de ativacao e rollback

Este runbook e' deliberadamente operacional, nao um comando automatico. Ele
nao autoriza merge, deploy, migration de producao ou benchmark compartilhado.
Execute-o somente apos aprovacao explicita do responsavel e registre o SHA,
horarios, operador e resultados de cada gate.

## Invariantes

- O Reader compativel e' implantado antes do Lite. Nenhum processo Lite pode
  iniciar antes de a fase dois da identidade canonica estar concluida.
- Ha no maximo um produtor: v1 e Lite compartilham a chave
  `settings.importer_runtime_leader`. O lease e' uma protecao adicional, nao
  uma permissao para executar os dois runtimes durante a troca.
- Nao se apagam capitulos, paginas, midias ou tabelas Lite para fazer rollback.
  Publicacoes confirmadas permanecem validas.
- O primeiro Lite inicia com limites conservadores medidos em staging; nunca
  herda o valor legado de `MAX_CONCURRENT_CHAPTERS` como aprovacao de carga.

## Gate 0 — pacote, backup e ambiente de staging

1. Fixar os SHAs do Reader `feat/reader-canonical-chapter-identity` e do
   Importer `feat/importer-lite-v2`; executar seus builds e suites relevantes
   a partir desses SHAs.
2. Rotacionar qualquer credencial de DIScloud que tenha aparecido em script ou
   log historico e confirmar que os jobs operacionais a recebem apenas pelo
   ambiente protegido. Nunca registrar o valor na evidencia de cutover.
3. Criar e verificar um backup restauravel do banco de producao. Registrar a
   contagem de obras, capitulos, paginas, midias, mapeamentos de fonte e
   solicitacoes STAFF.
4. Restaurar uma copia sanitizada/descartavel com os papeis YSQL reais.
   Executar os verificadores guardados em modo preflight e, so' nela, as fases
   Reader 1, backfill, Reader 2 e migration Lite. Validar rollback logico
   (parar Lite, sem restaurar/apagar dados) nessa copia.
5. Com um bot e canal privados de validacao autorizados, testar recibo Telegram
   e Reader de ponta a ponta. Nao use midia, bots ou canais servidos pelo site.
6. Coletar baseline do site e limites reais de YSQL/Telegram. A meta de
   8--10 capitulos/minuto requer duas janelas de dez minutos em infraestrutura
   autorizada; ela nao e' demonstrada por esta preparacao local.
7. No painel DIScloud, verificar a integracao GitHub nativa da mesma aplicacao
   e registrar repositorio, branch e estado de Auto Deploy. Ela deve estar
   ausente ou desativada durante o merge/cutover: o workflow GitHub versionado
   e' manual e esta' sujeito ao gate de SHA abaixo. Nao assumir que a ausencia
   de `push` no workflow elimina um Auto Deploy configurado fora do repositorio.

## Gate 1 — fase aditiva Reader e backfill

1. Com o Reader e produtor v1 atuais ainda ativos, aplicar **somente** a
   migration Reader fase 1:
   `20261011090000_add_reader_canonical_chapter_identity.sql`.
   Ela e' aditiva e preserva a leitura/escrita legada durante o backfill.
   Nao implantar ainda o Reader novo: ele seleciona as novas colunas e o
   banco atual ainda nao as possui.
2. Executar o backfill limitado ate' nao restarem linhas nulas; verificar
   duplicatas por `(work_id, canonical_chapter_key)`, rotas UUID antigas, APIs,
   ordenacao de `12.1`, `12.10`, especiais e edicoes. Corrigir dados antes de
   continuar.
3. Implantar o Reader compativel sem ativar Lite somente depois de as colunas
   da fase 1 existirem. Confirmar que ele serve capitulos legados e que cache
   continua indexado pelo UUID do capitulo, nao pelo numero exibido.

## Gate 2 — fencing, fase 2 Reader e migration Lite

1. Pausar novas admissoes do produtor v1 e deixá-lo drenar uploads e
   publicacoes que ja' possuem lease. Nao interromper uploads em andamento.
2. Confirmar por logs/processo que ha' uma unica instancia v1 e por YSQL que
   `importer_runtime_leader` pertence a ela. Em seguida parar o processo v1 e
   esperar liberacao ou expiracao do lease de 45 segundos.
3. Confirmar que nao existe container/processo legado apto a reiniciar pelo
   auto-deploy. Desabilitar temporariamente esse produtor na configuracao da
   mesma aplicacao DIScloud; nao criar uma segunda aplicacao.
4. Aplicar a fase 2 Reader:
   `20261011091000_enable_reader_canonical_chapter_identity.sql`. Ela aborta
   se o backfill ou a unicidade nao estiverem prontos e remove apenas a
   restricao numerica incompatível depois dessas provas.
5. Aplicar a migration aditiva Lite
   `20261010090000_importer_lite_v2.sql`. Antes de producao, ela deve ter sido
   aplicada e verificada pelo `npm run validate:lite:staging` em staging.
   Registrar a contagem de linhas legadas antes/depois: a migration Lite nao
   pode altera-las.
6. Verificar que `canonical_chapter_key`, `display_number`, `chapter_order_key`,
   `chapter_kind`, `edition` e `language` existem; que `number` aceita nulo;
   que o indice `chapters_work_canonical_chapter_key_uq` existe; e que
   `chapters_work_id_number_key` nao existe. Caso contrario Lite deve falhar
   fechado com `READER_CANONICAL_IDENTITY_MIGRATION_REQUIRED`.

## Gate 3 — artefato DIScloud e primeira publicacao controlada

1. Publicar **um** artefato do SHA Lite aprovado na mesma aplicacao. O workflow
   atual `Deploy DIScloud` e' somente `workflow_dispatch` e aceita exclusivamente
   `main`: depois do merge, disparar manualmente o run na `main` e registrar o
   SHA do workflow, `build/BUILD_ID` e o hash do artefato. Um push por si so'
   nao implanta a aplicacao. Configurar
   `IMPORTER_RUNTIME_MODE=lite`, `IMPORTER_DB_MODE=direct` e
   `STORAGE_PROVIDER=direct_telegram`; conferir `IMPORTER_USER_ID`, TLS YSQL e
   variaveis de Telegram sem imprimir seus valores. Definir
   `LITE_CHAPTER_WORKERS=1` inicialmente e limites de download/upload obtidos
   em staging.
2. Conferir nos logs o SHA do build, `ImporterLite` (e ausencia de start do
   `ImporterEngine` legado), conexao direta YSQL/TLS e um unico lider Lite.
   Consultar somente leitura a chave `importer_runtime_leader` e confirmar que
   o owner e' o boot Lite atual.
3. Executar uma solicitacao STAFF controlada com capitulo e fonte ja'
   confirmados. Verificar task, pagina validada, recibo Telegram duravel,
   `published_at`, evento unico, pagina no Reader e URL UUID legada quando
   aplicavel. Verificar que uma tarefa P1/P2 ja' em finalizacao nao ficou
   bloqueada pela STAFF.
4. Permanecer no limite conservador ate' o benchmark progressivo demonstrar
   capacidade e saude. Observar p50/p95 do site, 5xx, espera do pool, RSS,
   heap, memoria externa, 429, idade de ready/stored e taxa de publicacao.

## Rollback sem perda de dados

1. Parar Lite e impedir sua reinicializacao automatica. Nao restaurar o banco
   e nao remover tabelas/linhas Lite ou capitulos ja' publicados.
2. Preservar logs, SHA, owner do lease, IDs das tarefas e recibos Telegram para
   diagnostico. Reconciliar uploads incertos em vez de reenviar cegamente.
3. So' reativar o produtor v1 se ele tiver sido validado contra o schema Reader
   fase 2 no staging restaurado, com
   `IMPORTER_LEGACY_READER_CANONICAL_IDENTITY=1`, e depois que o lease Lite
   tiver sido liberado ou expirado. Esse modo de compatibilidade usa o
   mapeamento duravel `source + source_chapter_id`, e nao a antiga unicidade
   numerica. Sem essa variavel o v1 e' propositalmente incompativel com a fase
   2 e nao deve ser iniciado. Caso a compatibilidade nao esteja comprovada,
   corrigir ou reimplantar o artefato Lite/Reader; nao executar rollback
   destrutivo de schema.
4. Confirmar novamente um unico owner, uma unica linha de publicacao por
   capitulo e Reader saudavel antes de admitir backlog.

## Evidencias para encerrar o cutover

Registre: SHAs, versoes das migrations, backup verificado, owner do lease,
configuracao (somente nomes/nao segredos), resultados de integridade Reader,
amostra Telegram, metricas do baseline, cada janela de benchmark e decisao de
rollback/continuidade. Ausencia de demanda elegivel nao e' falha de throughput;
fila cheia sem `published_at` tambem nao e' sucesso.
