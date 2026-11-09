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
| **Banco Principal & Fila** | YugabyteDB / YSQL | Banco distribuído principal, persistência de catálogo, leases atômicos e integridade |
| **Storage de Mídia** | Telegram (`StorageProvider`) | Armazenamento de mídia e entrega de blobs de páginas |
| **Consumo & Leitura**| [Project Nox · Site](https://github.com/Awerkori/project-nox-manga) | Leitor web e plataforma de entrega ao usuário final |
| **Legado / Transição** | Supabase (PostgreSQL) | Filas legadas e esquemas transitórios mantidos para compatibilidade |
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
- Cluster **YugabyteDB / YSQL** (ou instância PostgreSQL compatível com certificados configurados)
- Variáveis de ambiente configuradas (`.env` com `YUGABYTE_HOST`, `YUGABYTE_PASSWORD`, etc.)

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

O motor opera primariamente sobre **YugabyteDB / YSQL**, garantindo resiliência distribuída, leases concorrentes seguros e integridade referencial:

1. As configurações de conexão YSQL utilizam certificados SSL e credenciais gerenciadas (`YUGABYTE_HOST`, `YUGABYTE_PORT`, `YUGABYTE_USER`, `YUGABYTE_PASSWORD`, `YUGABYTE_DATABASE`).
2. O arquivo [`migrations/001_importer_schema.sql`](migrations/001_importer_schema.sql) documenta a estrutura histórica/legada de tabelas de fila, fontes e mapeamentos.

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
- 📦 [**Project Nox · Extensões de Mangá**](https://github.com/Awerkori/manga-extensoes) — Distribuição de fontes para Mihon e forks.
- 💻 [**Project Nox · Código-fonte das Extensões de Mangá**](https://github.com/Awerkori/manga-fonte-extensoes) — Código em Kotlin das extensões.
- 🎫 [**Project Nox · Central de Pedidos**](https://github.com/Awerkori/project-nox-requests) — Canal oficial de solicitações e chamados.
- 📊 [**Project Nox · Tracker**](https://awerkori.github.io/project-nox-tracker/) — Dashboard público de status.

---

## 🌟 Créditos e Reconhecimento

- **Project Nox**: Engenharia do motor de ingestão, algoritmos de deduplicação e infraestrutura autônoma.
- Equipe de infraestrutura e administradores de banco de dados.

---

## ⚖️ Licença e Responsabilidade

Este projeto é software livre licenciado sob os termos da **GNU Affero General Public License versão 3.0** (`AGPL-3.0-only`). Consulte o arquivo [LICENSE](LICENSE) para obter o texto integral inalterado da licença e o arquivo [NOTICE](NOTICE) para informações detalhadas de autoria, dependências e notas de governança.

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
O Project Nox adota oficialmente a licença **GNU AGPL versão 3.0**. Nos termos da Seção 13 da AGPLv3, qualquer versão modificada deste software operada em servidores ou instâncias acessíveis remotamente através de uma rede de computadores deve oferecer a todos os usuários remotos a oportunidade de obter o Código-Fonte Correspondente (Corresponding Source) da aplicação em execução, sem custos e através de meio padrão de distribuição de software.

### 🛡️ Proteção de Identidade e Marca (AGPLv3 Seção 7(e))
A concessão da licença AGPLv3 refere-se estritamente aos direitos autorais do software. Os nomes "Project Nox" e "Awerkori", bem como logotipos associados, identificam a procedência e origem do projeto. A licença **não concede** autorização para uso de nomes, marcas ou logotipos para insinuar homologação, endosso ou representação oficial sem autorização prévia por escrito.

### ⚖️ Isenção de Responsabilidade
O **Project Nox** desenvolve e disponibiliza ferramentas exclusivamente para estudo, interoperabilidade técnica e pesquisa de catalogação. O serviço atua estritamente de forma programática como indexador técnico, sem armazenar centralmente nem comercializar conteúdos protegidos de terceiros.
