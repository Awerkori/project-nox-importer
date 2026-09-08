# Project Nox Importer 🌌

Serviço autônomo 24/7 responsável por rastrear, extrair metadados, baixar, validar, deduplicar e armazenar imagens pesadas de mangás/manhwas, alimentando o catálogo do **Project Nox Manga** sem intervenção manual.

---

## 🏛️ Arquitetura e Separação de Responsabilidades

| Camada | Tecnologia / Serviço | Responsabilidade |
| :--- | :--- | :--- |
| **Execução 24/7** | DIScloud (Node.js daemon) | Ciclo autônomo, polling, rate limiting, gerenciamento de leases (ONLINE @ TITAN 512MB) |
| **Banco & Fila** | Supabase PostgreSQL | Fila com locking atômico (`FOR UPDATE SKIP LOCKED`), mapeamentos, RLS estrito |
| **Storage de Mídia** | `StorageProvider` (Telegram / Mock) | Storage de documentos binários via bot Telegram (R$ 0,00) |
| **Consumo & Leitura**| Project Nox Manga (Cloudflare Worker) | Plataforma de leitura pública com streaming seguro |
| **Adapters** | Extensões / REST APIs | Adapters modulares para fontes de catálogo (Nexus, MangaFlix, Manhastro, Kuro) |

> [!NOTE]
> Este repositório é 100% isolado do front-end do Project Nox Manga e **não possui qualquer interação com a Central PROJECT NOX SCAN STAFF**.

---

## 🔒 Mecanismos de Proteção e Resiliência

1. **Fila com Lease e Heartbeat Atômico:**
   - As tarefas são adquiridas via função PostgreSQL `importer_acquire_job` com `FOR UPDATE SKIP LOCKED`.
   - Inclui colunas `locked_by`, `locked_at` e `lease_expires_at`.
   - Heartbeat periódico via `importer_renew_lease` renova a posse da tarefa durante downloads longos.
   - Tarefas com lease expirado (`lease_expires_at < now()`) são recuperadas com segurança após crash/restart da instância.

2. **Deduplicação Conservadora:**
   - Nunca realiza merges destrutivos baseados apenas em similaridade de títulos.
   - Ordem de prioridade: `source + source_work_id` -> mapeamento existente -> criação segura.
   - Qualquer colisão entre IDs externos ou slugs conflitantes é marcada como `sync_status = 'AMBIGUOUS'` para revisão editorial humana segura.

3. **Validação Binária Estrita & Deduplicação de Páginas:**
   - Inspeção binária byte-a-byte (`inspectImage`) para PNG, JPEG e WebP sem dependências externas pesadas.
   - Hash SHA-256 de cada imagem verificado contra `public.media` antes do upload para evitar duplicações no Telegram.
   - **Regra de Publicação Obrigatória:** Apenas capítulos com `validPages == expectedPages` são publicados (`published_at`). Se qualquer página falhar, a transação é abortada e o capítulo não entra no catálogo.

4. **Isolamento de Segurança (RLS):**
   - Todas as tabelas `importer_*` possuem Row Level Security ativado.
   - Permissões revogadas explicitamente de `public`, `anon` e `authenticated`. Apenas o `service_role` tem acesso de execução e gravação.

---

## 🚀 Instalação e Desenvolvimento Local

```bash
cd /home/awerkori/.Projects/project-nox-importer

# Instalar dependências
npm install

# Compilar TypeScript
npm run build

# Executar suíte de testes completa
npm test

# Executar em modo desenvolvimento com auto-reload
npm run dev
```

---

## 🗄️ Migração do Banco de Dados

Para aplicar a estrutura do importador no seu Supabase:

1. Abra o arquivo [001_importer_schema.sql](file:///home/awerkori/.Projects/project-nox-importer/migrations/001_importer_schema.sql).
2. Execute o conteúdo no SQL Editor do Supabase com privilégios administrativos.
3. O schema criará:
   - `importer_sources` (com o adapter `nexus` já ativado por padrão)
   - `importer_work_mappings`
   - `importer_chapter_mappings`
   - `importer_queue`
   - `importer_checkpoints`
   - Stored functions: `importer_acquire_job`, `importer_renew_lease`, `importer_release_job`
   - Políticas estritas de RLS e grants para `service_role`.

---

## ☁️ Hospedagem na DIScloud

O projeto inclui o arquivo [discloud.config](file:///home/awerkori/.Projects/project-nox-importer/discloud.config) pré-configurado:

```ini
ID=projectnoximporter
TYPE=bot
MAIN=dist/index.js
NAME=Project Nox Importer
RAM=512
AUTORESTART=true
VERSION=latest
APT=tools
```

### Passos para deploy:

1. Crie o arquivo `.env` preenchido a partir do `.env.example`.
2. Execute `npm run build` para gerar a pasta `dist/`.
3. Compacte os arquivos para a DIScloud (incluindo `dist`, `package.json`, `package-lock.json`, `discloud.config` e `.env`).
4. Envie o zip através do painel ou bot da DIScloud.
