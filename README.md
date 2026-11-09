# Project Nox · Importer

Serviço autônomo 24/7 responsável por rastrear fontes de catálogo, extrair metadados, baixar páginas, validar cabeçalhos e integridade binária de imagens, deduplicar arquivos via SHA-256 e enviar os blobs para armazenamento externo particionado no Telegram, persistindo a estrutura de obras, capítulos e metadados no banco de dados principal **YugabyteDB (YSQL)** para alimentar continuamente a [**Plataforma Web de Mangá do Project Nox**](https://github.com/Awerkori/project-nox-manga).

---

## 🎯 Para quem é este repositório?

Este repositório é voltado para **engenheiros de backend, infraestrutura e mantenedores** do ecossistema Project Nox. Trata-se de um serviço daemon de segundo plano, desacoplado do front-end público e de painéis administrativos de equipe.

---

## 🏛️ Arquitetura e Separação de Responsabilidades

| Camada | Tecnologia / Provedor | Responsabilidade Técnica |
| :--- | :--- | :--- |
| **Execução Contínua 24/7** | DIScloud (Node.js 22 daemon) | Ciclo autônomo ininterrupto, polling de fontes, controle de taxa e orquestração de esteiras |
| **Banco Principal & Filas**| YugabyteDB / YSQL (Sandbox Aeon)| Banco de dados relacional autoritativo, catálogo, metadados de mídia, controle de fila e leases atômicos |
| **Armazenamento de Mídia** | Telegram Bot API (`DirectTelegramStorageProvider`)| Armazenamento externo particionado de blobs de imagem em canais dedicados (shards 10 a 21) |
| **Consumo & Leitura Web**  | [Project Nox · Site](https://github.com/Awerkori/project-nox-manga) | Leitor web no Edge que consulta metadados no YSQL e transmite mídia do Telegram ao usuário |
| **Camada Transitória**     | Supabase (PostgreSQL) | Esquemas e rotinas legadas mantidos apenas para transição e compatibilidade histórica |
| **Adaptadores de Fonte**   | TypeScript Modular (`src/sources/`) | Scrapers e parsers especializados por plataforma (Madara, MangaThemesia, etc.) |

---

## 🗄️ Banco de Dados e Conexão Direta (YugabyteDB / YSQL)

O motor opera com conexão direta ao **YugabyteDB (YSQL)** via driver nativo `pg.Pool`, utilizando certificados SSL gerenciados (`EMBEDDED_YUGABYTE_CA` / `yugabyte-root.crt`) e parametrização defensiva:

1. **Topologia Real:** Instância **Sandbox de nó único** provisionada na nuvem gerenciada **Aeon**. O ambiente conta com capacidade restrita (`max_connections = 13` no nó gerenciado) e **não** constitui um cluster de alta disponibilidade ou distribuído geograficamente.
2. **Capacidade Estrita de Conexão:** Para não saturar o Connection Manager e reservar slots para a plataforma web, o Importer unifica suas operações em uma cota rígida de conexões (`DIRECT_DB_POOL_MAX = 1` e `METADATA_DB_POOL_MAX = 1`), mantendo o uso de conexões bem abaixo do teto de segurança de 60%.
3. **Rastreabilidade de Conexão:** Cada cliente do pool é identificado perante o banco através do parâmetro `application_name` dinâmico (`project-nox-importer-direct:<deploy_sha>:<pid>`), permitindo monitoramento preciso de sessões.

---

## 🔄 Gerenciamento Real de Filas, Concorrência e Leases

O controle de tarefas do Importer é desacoplado de stored procedures legadas e opera diretamente via instruções SQL atômicas sobre a tabela `importer_queue`:

1. **Aquisição Direta com `FOR UPDATE SKIP LOCKED` (`acquireJobsDirect`):**
   - As tarefas elegíveis (`QUEUED` ou `RETRY` com `next_run_at <= NOW()`) são selecionadas e bloqueadas atomicamente utilizando Common Table Expression (CTE) e `FOR UPDATE SKIP LOCKED`.
   - Na mesma transação, o status é alterado para `IMPORTING`, registrando `locked_by` (identificador do worker), `locked_at`, `lease_expires_at` (calculado a partir da duração do lease, tipicamente de 5 a 10 minutos) e incrementando `attempts`.
   - **Ordem de Prioridade Estrita:** Tarefas marcadas com `staffForced` ou prioridade emergencial (`priority >= 1000`) têm precedência máxima, seguidas por `priority DESC`, `chapter_sort_key ASC NULLS LAST` e `next_run_at ASC`.

2. **Batimentos Cardíacos e Recuperação de Tarefas Órfãs:**
   - O worker renova periodicamente o lease de tarefas em processamento longo via batimento cardíaco SQL direto (`heartbeatDirect`).
   - O watchdog em segundo plano (`recoverStalledLeasesDirect`) detecta tarefas cujo lease tenha expirado sem confirmação (por crash ou restart do container) e as devolve com segurança para o estado `QUEUED`, preservando o histórico de tentativas.

3. **Limites de Concorrência e Vazão do Motor Lite:**
   - **Capítulos Simultâneos:** `LITE_CHAPTER_WORKERS = 2` (controla o número máximo de capítulos processados concorrentemente no container).
   - **Download de Páginas:** `BATCH_PAGE_DOWNLOAD_CONCURRENCY = 4` (páginas baixadas em paralelo por capítulo).
   - **Upload de Mídia:** `TELEGRAM_MEDIA_CONCURRENCY = 6` (trava global compartilhada para envios simultâneos à API do Telegram).
   - **Orçamento de Banda:** `UPLOAD_RATE_LIMIT_BYTES_PER_SEC = 8 MiB/s` (gerenciado dinamicamente pelo `BandwidthLimiter`).

---

## 🖼️ Processamento de Imagens e Relação SQL vs. Arquivos Externos

O Project Nox estabelece uma separação rigorosa entre dados relacionais e armazenamento de blobs de imagem:

1. **O Banco SQL NÃO Armazena Binários de Imagem:**
   - Nenhuma imagem ou arquivo gráfico é gravado no banco de dados YugabyteDB.
   - O YSQL armazena metadados estruturados:
     - `public.media`: identificador `id`, tipo `mime`, largura `width`, altura `height`, tamanho em `bytes`, hash `sha256`, provedor `provider = 'telegram'`, identificador do arquivo `provider_key` (o `file_id` do Telegram) e o identificador do canal `storage_shard_id`.
     - `public.pages`: ordenação da página (`position`) e vínculo com o capítulo (`chapter_id`) e com a mídia (`media_id`).

2. **Validação Estrutural Binária (`inspectImage`):**
   - Todo arquivo baixado tem seus bytes inspecionados em memória para validar cabeçalhos, marcadores de fim e chunks (PNG, JPEG e WebP).
   - Dimensões mínimas e máximas são validadas para evitar explosão de memória.
   - Páginas falsas de desafio HTML (ex.: bloqueios Cloudflare retornados com status HTTP 200) são detectadas e rejeitadas antes de qualquer upload.

3. **Deduplicação por Hash Criptográfico (SHA-256):**
   - O hash SHA-256 é calculado a partir dos bytes da imagem para reaproveitamento de mídia e economia de tráfego de rede.

4. **Entrega e Particionamento no Telegram (Shards):**
   - O upload das imagens é feito através do `DirectTelegramStorageProvider` para canais particionados do Telegram (shards 10 a 21), utilizando um pool balanceado de bots de armazenamento com tratamento automático de códigos HTTP 429 (`flood_wait`).

---

## 🛡️ Mecanismos de Proteção, Resiliência e Auto-Cura

1. **Barreira de Segurança de Publicação (`PublicationSafetyBarrier`):**
   - O capítulo só é marcado como publicado (`published_at`) se **todas** as páginas esperadas tiverem sido rigorosamente baixadas, validadas e associadas (`validPages == expectedPages`).
   - A barreira impede a publicação de capítulos fora de ordem sequencial ou que criem lacunas narrativas no catálogo público (`WAITING_FOR_INITIAL_CHAPTERS`). Capítulos prontos mas dependentes de predecessores permanecem no estado `STAGED`.

2. **Deduplicação Conservadora de Obras:**
   - Previne junções errôneas de títulos baseadas apenas em similaridade fonética.
   - Chave estrita composta `source + source_work_id` para identificação segura no YSQL.

3. **Cão de Guarda de Auto-Cura (`AutoHealWatchdog`):**
   - Monitora o avanço das esteiras e liveness do processo.
   - Identifica mapeamentos estagnados em `IMPORTING`, filas travadas ou descompassos de catálogo, aplicando reconciliações graduais sem necessidade de reinicializações manuais.

---

## 💻 Procedimento de Instalação, Testes e Deploy

### Pré-requisitos
- **Node.js** 22 ou posterior
- Cluster ou instância **YugabyteDB / YSQL** com certificados configurados
- Arquivo `.env` configurado conforme `.env.example`

### Instalação
```bash
npm install
```

### Compilação do Projeto
```bash
# Compila o TypeScript via tsc e sincroniza o identificador de build (build/BUILD_ID)
npm run build
```

### Execução dos Testes Automatizados
```bash
# Suíte unitária focada de CI (liveness, auto-cura e contratos):
npx vitest run tests/auto-heal-watchdog.test.ts

# Suíte completa de testes:
npm test
```

### Execução Local
```bash
npm run dev
```

---

## ☁️ Hospedagem na DIScloud

O repositório inclui a configuração oficial em [`discloud.config`](discloud.config):

```ini
ID=projectnoximporter
TYPE=bot
MAIN=build/index.js
NAME=Project Nox Importer
RAM=624
AUTORESTART=true
VERSION=22
APT=tools,tesseract-ocr,tesseract-ocr-por,tesseract-ocr-eng
START=env DISCLOUD_RAM_MB=624 UV_THREADPOOL_SIZE=8 MALLOC_ARENA_MAX=1 MALLOC_TRIM_THRESHOLD_=131072 MALLOC_MMAP_THRESHOLD_=1 node --expose-gc --max-old-space-size=352 build/index.js
```

- **Topologia de Memória do Container:** Container dimensionado em 624 MiB de RAM (`RAM=624`) executando Node.js 22 com teto de heap V8 ajustado para 352 MiB (`--max-old-space-size=352`).
- **Prevenção de Fragmentação Nativa:** Configuração de alocador de memória com arena única (`MALLOC_ARENA_MAX=1`, `MALLOC_TRIM_THRESHOLD_=131072`, `MALLOC_MMAP_THRESHOLD_=1`) e liberação periódica de memória para mitigar retenção residual de RSS após rajadas de download e validação de mídia.
- **Suporte a OCR:** Instalação nativa via APT de pacotes do Tesseract OCR (`tesseract-ocr`, `tesseract-ocr-por`, `tesseract-ocr-eng`) para inspeção óptica e extração textual de capítulos quando necessário.

### Pipeline de Deploy Automatizado
- O deploy em produção é gerenciado pelo workflow do GitHub Actions [`.github/workflows/deploy-discloud.yml`](.github/workflows/deploy-discloud.yml).
- Disparado a partir da branch `main`, valida a compilação, executa os testes de liveness e publica o container na DIScloud utilizando a secret protegida `DISCLOUD_TOKEN`.

---

## 🔗 Projetos Relacionados

- 🌐 [**Project Nox · Plataforma Web de Mangá**](https://github.com/Awerkori/project-nox-manga) — Plataforma web oficial e leitor de catálogo.
- 📦 [**Project Nox · Extensões de Mangá**](https://github.com/Awerkori/manga-extensoes) — Distribuição de fontes para Mihon e forks.
- 💻 [**Project Nox · Código-fonte das Extensões de Mangá**](https://github.com/Awerkori/manga-fonte-extensoes) — Código em Kotlin das extensões.
- 🎫 [**Project Nox · Central de Pedidos**](https://github.com/Awerkori/project-nox-requests) — Canal oficial de chamados e solicitações.
- 📊 [**Project Nox · Tracker**](https://awerkori.github.io/project-nox-tracker/) — Painel público de status operacional.

---

## 🌟 Créditos e Reconhecimento

- **Project Nox**: Concepção e engenharia do motor de ingestão, algoritmos de deduplicação e pipeline de imagem.
- Equipe de infraestrutura, mantenedores de banco de dados e colaboradores do Project Nox.

---

## ⚖️ Licença e Copyright

Este projeto é software livre licenciado sob os termos da **GNU Affero General Public License versão 3.0** (`AGPL-3.0-only`). Consulte o arquivo [LICENSE](LICENSE) para obter o texto integral inalterado da licença e o arquivo [NOTICE](NOTICE) para informações detalhadas de autoria, dependências e governança.

```
Copyright (C) 2026 Project Nox Contributors & Awerkori

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, version 3 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.
```

### 📌 Interação Remota de Rede (GNU AGPLv3 Seção 13)
O Project Nox adota oficialmente a licença **GNU AGPL versão 3.0**. Nos termos da Seção 13 da AGPLv3, qualquer versão modificada deste software operada em servidores ou instâncias acessíveis remotamente através de uma rede de computadores deve oferecer a todos os usuários remotos a oportunidade de obter o Código-Fonte Correspondente (Corresponding Source) da aplicação em execução, sem custos e através de meio padrão de transmissão de software.

### 🛡️ Proteção de Identidade e Marca (AGPLv3 Seção 7(e))
A concessão da licença AGPLv3 refere-se estritamente aos direitos autorais do software. Os nomes "Project Nox" e "Awerkori", bem como logotipos associados, identificam a procedência e origem do projeto. Em conformidade com a Seção 7(e) da GNU AGPLv3, a licença **não concede** autorização para uso de nomes, marcas ou logotipos para insinuar homologação, endosso ou representação oficial sem autorização prévia por escrito.
