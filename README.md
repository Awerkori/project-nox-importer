# Project Nox · Importer

Serviço autônomo 24/7 responsável por rastrear, extrair metadados, baixar, validar, deduplicar e armazenar imagens de mangás, alimentando continuamente o catálogo do [**Project Nox · Plataforma Web de Mangá**](https://github.com/Awerkori/project-nox-manga) sem necessidade de intervenção manual.

---

## 🎯 Para quem é este repositório?

Este repositório é voltado para **engenheiros de backend, infraestrutura e mantenedores** do ecossistema Project Nox. Trata-se de um serviço daemon de segundo plano, desacoplado do front-end público e de painéis administrativos de equipe.

---

## 🏛️ Arquitetura e Separação de Responsabilidades

| Camada | Tecnologia / Serviço | Responsabilidade |
| :--- | :--- | :--- |
| **Execução 24/7** | DIScloud (Node.js daemon) | Ciclo autônomo contínuo, polling, rate limiting e controle de leases |
| **Banco & Fila** | Supabase (PostgreSQL) | Fila atômica com `FOR UPDATE SKIP LOCKED`, mapeamentos e RLS restrito |
| **Storage de Mídia** | `StorageProvider` (Telegram / Mock) | Armazenamento seguro de blobs e páginas de mídia sem custo |
| **Consumo & Leitura**| [Project Nox · Site](https://github.com/Awerkori/project-nox-manga) | Leitor web e plataforma de entrega ao usuário final |
| **Adapters** | Modulares / TypeScript | Conectores para fontes de catálogo e normalização de dados |

---

## 🔒 Mecanismos de Proteção e Resiliência

1. **Fila com Lease e Heartbeat Atômico:**
   - Tarefas adquiridas via stored function `importer_acquire_job` (`FOR UPDATE SKIP LOCKED`).
   - Controle por `locked_by`, `locked_at` e `lease_expires_at` com renovação contínua via `importer_renew_lease`.
   - Recuperação automática de tarefas órfãs caso ocorra restart ou crash do processo da aplicação.

2. **Deduplicação Conservadora:**
   - Prevenção contra merges destrutivos baseados apenas em aproximação fonética ou similaridade de títulos.
   - Hierarquia estrita: chave composta `source + source_work_id` → mapeamento verificado → criação segura.
   - Conflitos e ambiguidades de catálogo são sinalizados com `sync_status = 'AMBIGUOUS'` para revisão pontual.

3. **Validação Binária e Integridade de Páginas:**
   - Inspeção de cabeçalhos binários (`inspectImage`) para formatos PNG, JPEG e WebP.
   - Geração de hash SHA-256 por página para evitar envios repetidos e economizar tráfego.
   - **Garantia de Entrega Completa:** O capítulo só é marcado como publicado (`published_at`) quando a totalidade das páginas esperadas for rigorosamente validada (`validPages == expectedPages`).

4. **Isolamento de Segurança (RLS):**
   - Políticas de Row Level Security em todas as tabelas operacionais `importer_*`.
   - Permissões restritas unicamente à role administrativa (`service_role`).

---

## 💻 Instalação e Execução Local

### Pré-requisitos
- **Node.js** 22 ou posterior
- Banco PostgreSQL / Supabase com as migrações aplicadas

### Passos
```bash
# Instalar dependências
npm install

# Compilar TypeScript
npm run build

# Executar testes automatizados
npm test

# Iniciar em modo de desenvolvimento
npm run dev
```

---

## 🗄️ Estrutura de Banco de Dados

Para inicializar a estrutura do importador em uma instância Supabase:

1. Acesse o arquivo de migração [`migrations/001_importer_schema.sql`](migrations/001_importer_schema.sql).
2. Execute o script no SQL Editor com privilégios de administrador.
3. O schema provisiona as tabelas de fila, fontes, mapeamentos, funções atômicas e políticas de segurança.

---

## ☁️ Hospedagem na DIScloud

O repositório inclui a configuração pronta em [`discloud.config`](discloud.config):

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

### Deploy Seguro
O deploy em produção pode ser disparado manualmente através do GitHub Actions (`Deploy DIScloud`) a partir da branch `main`, exigindo o environment `discloud-production` e a secret protegida `DISCLOUD_TOKEN`.

---

## 🔗 Projetos Relacionados

- 🌐 [**Project Nox · Plataforma Web de Mangá**](https://github.com/Awerkori/project-nox-manga) — Leitor e catálogo consumido pelo público.
- 📦 [**Project Nox · Extensões de Mangá**](https://github.com/Awerkori/manga-extensoes) — Distribuição de fontes para Mihon / TachiyomiSY.
- 💻 [**Project Nox · Código-fonte das Extensões de Mangá**](https://github.com/Awerkori/manga-fonte-extensoes) — Código em Kotlin das extensões.
- 🎫 [**Project Nox · Central de Pedidos**](https://github.com/Awerkori/project-nox-requests) — Canal oficial de solicitações e chamados.
- 📊 [**Project Nox · Tracker**](https://awerkori.github.io/project-nox-tracker/) — Dashboard público de status.

---

## ⚖️ Licença e Responsabilidade

O **Project Nox** desenvolve e disponibiliza ferramentas para estudo e interoperabilidade técnica. O serviço de importação atua exclusivamente de forma programática sem comercialização de conteúdo de terceiros.
