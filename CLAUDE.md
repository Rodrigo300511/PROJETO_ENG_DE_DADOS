# PNAD ETL Pipeline — Contexto do Projeto

## O que este projeto faz

Pipeline ETL completo para indicadores do mercado de trabalho brasileiro da **PNAD Contínua** (Pesquisa Nacional por Amostra de Domicílios Contínua) do IBGE. Extrai dados via API REST do IBGE, normaliza e persiste em três camadas (Bronze → Silver → Gold), e expõe os dados via UI web, MCP server e SQL.

## Arquitetura

### Medalion Architecture

```
IBGE API (JSON aninhado)
    ↓
[Bronze] MongoDB Atlas — snapshot imutável do JSON bruto
    ↓
[Silver] MongoDB Atlas — documentos normalizados, tipados, indexados
    ↓
[Gold]   SQLite local  — star schema relacional para SQL analytics
```

### Estrutura de Diretórios

```
pnad_etl/
├── pipeline.py          # Orquestrador Prefect (entry point ETL)
├── app.py               # Interface Streamlit (UI + chat IA)
├── pnad_gold.db         # SQLite Gold layer
├── requirements.txt
├── .env / .env.example
└── src/
    ├── extract/
    │   ├── base_extractor.py    # ABC: interface para extractors
    │   └── ibge_extractor.py    # Client HTTP da API IBGE c/ retry
    ├── transform/
    │   └── pnad_transformer.py  # Normalização e geração de chave única
    ├── load/
    │   ├── base_loader.py       # ABC: interface para loaders
    │   ├── mongo_loader.py      # Upsert bulk no MongoDB (Bronze + Silver)
    │   └── sqlite_loader.py     # INSERT OR REPLACE no SQLite (Gold)
    └── mcp/
        └── mcp_server.py        # FastMCP server com ferramentas para IA
```

## Stack Tecnológica

| Componente | Tecnologia |
|---|---|
| Linguagem | Python 3.13 |
| HTTP / Resiliência | `requests` + `tenacity` (exponential backoff) |
| NoSQL | MongoDB Atlas (`pymongo`) — Bronze e Silver |
| SQL | SQLite3 nativo — Gold |
| Orquestração | Prefect 3 |
| UI | Streamlit |
| IA / LLM | FastMCP + OpenAI SDK |
| Config | `python-dotenv` |

## Fluxo de Dados Detalhado

### Extração (`IBGEExtractor`)
- Chama a API IBGE Agregados v3 (tabela 4093 — PNAD Contínua)
- Parâmetros configuráveis via `.env`: `IBGE_AGREGADO`, `IBGE_PERIODOS`, `IBGE_VARIAVEIS`, `IBGE_LOCALIDADES`, `IBGE_CLASSIFICACAO`
- Retry automático com backoff exponencial em falhas HTTP

### Transformação (`PNADTransformer`)
- Desnormaliza JSON aninhado em documentos planos
- Converte período: `"202301"` → `"2023T1"` (trimestre sem zero à esquerda — formato canônico)
- Trata valores nulos: strings `"..."`, `"-"`, `"X"`, `""`, `"NA"`, `"nd"` → `valor=None, valor_disponivel=False`
- Gera `chave_unica` = MD5(`periodo_raw_uf_variavel_sexo`) — chave determinística para idempotência

### Carga (Loaders)
- **Bronze** — insert append-only no MongoDB; preserva JSON original intacto
- **Silver** — bulk upsert no MongoDB filtrando por `chave_unica`; cria índices em `(uf, periodo)`, `(variavel_codigo, periodo)`, `(sexo, uf, periodo)`
- **Gold** — `INSERT OR REPLACE` no SQLite; filtra apenas `valor_disponivel == True`

### Schema Gold (SQLite)

```sql
dim_variavel (variavel_codigo PK, variavel, unidade)
dim_uf       (uf_codigo PK, uf, nivel_territorial)
fato_pnad    (chave_unica PK, variavel_codigo FK, uf_codigo FK,
              periodo, sexo_codigo, sexo, valor REAL, unidade, data_carga)
```

**Estado atual do banco (`pnad_gold.db`):** `dim_variavel` = 3 linhas · `dim_uf` = 28 linhas · `fato_pnad` = 11.160 linhas

O `load_documents` do `SQLiteLoader` popula as três tabelas em ordem — dimensões primeiro (para satisfazer FKs), fato depois.

## Interfaces de Acesso

### 1. CLI via Prefect
```bash
python pipeline.py
```
Executa o fluxo completo com logging estruturado.

### 2. Streamlit UI (`app.py`)
```bash
streamlit run app.py
```
- **Aba "Pipeline ETL"** — trigger manual do ETL com status em tempo real
- **Aba "AI Analyst"** — chat com GPT-4o-mini + ferramentas MCP para consultas em linguagem natural

### 3. MCP Server (`src/mcp/mcp_server.py`)
Ferramentas expostas para agentes IA:
- `consultar_pnad(variavel, uf, sexo, periodo)` — query parametrizada
- `listar_periodos()` — trimestres disponíveis
- `listar_ufs()` — estados com dados
- `top_desocupacao(periodo, n)` — ranking de estados por desemprego
- `comparar_sexo(variavel, uf, periodo)` — gap de gênero

## Variáveis de Ambiente (`.env`)

| Variável | Descrição |
|---|---|
| `DB_USER`, `DB_PASSWORD` | Credenciais MongoDB Atlas |
| `MONGO_URI` | URI de conexão (ou construída a partir de user/password) |
| `MONGO_DB_NAME` | Nome do banco (padrão: `ibge_pnad`) |
| `MONGO_BRONZE_COLLECTION` | Coleção Bronze |
| `MONGO_SILVER_COLLECTION` | Coleção Silver |
| `SQLITE_DB_PATH` | Caminho do SQLite (padrão: `pnad_gold.db`) |
| `IBGE_BASE_URL` | URL base da API IBGE |
| `IBGE_AGREGADO` | Código da tabela PNAD (4093) |
| `IBGE_PERIODOS`, `IBGE_VARIAVEIS`, `IBGE_LOCALIDADES`, `IBGE_CLASSIFICACAO` | Parâmetros de extração |

## Convenções de Dados

### Formato de período
- **Canônico**: `"2023T1"`, `"2023T2"`, `"2023T3"`, `"2023T4"` (trimestre sem zero à esquerda)
- **Raw IBGE** (armazenado em `periodo_raw`): `"202301"`, `"202302"`, `"202303"`, `"202304"`
- Dados carregados antes da correção do transformer podem ter `"2023T01"` no campo `periodo` — o MCP server aceita ambos os formatos via regex `T0?{tri}`

### Campos Silver / Gold
| Campo | Exemplo de valor |
|---|---|
| `periodo` | `"2023T1"` |
| `periodo_raw` | `"202301"` |
| `uf` | `"Pernambuco"`, `"São Paulo"`, `"Brasil"` |
| `nivel_territorial` | `"Unidade da Federação"` (estados), `"Brasil"` (nacional) |
| `sexo` | `"Total"`, `"Homens"`, `"Mulheres"` |
| `variavel` | `"Taxa de desocupação, na semana de referência..."` |
| `variavel_codigo` | `"4090"`, `"4096"`, `"12466"` |

### MCP Server — tolerância de formatos
O `mcp_server.py` normaliza qualquer formato de entrada antes de consultar o MongoDB Silver:
- `"2023T1"` ou `"2023T01"` → regex `^2023T0?1$`
- `"202301"` (raw) → OR no campo `periodo_raw`
- `"2023"` (apenas o ano) → prefixo `^2023T`

## Decisões de Design

- **Idempotência**: `chave_unica` MD5 (baseada em `periodo_raw`) garante que reexecutar o pipeline não duplica dados
- **Resiliência**: `tenacity` com backoff exponencial nas chamadas HTTP
- **Abstração**: `BaseExtractor` e `BaseLoader` permitem adicionar novas fontes/destinos sem alterar a orquestração
- **Separação de camadas**: cada módulo em `src/` tem uma única responsabilidade
- **Batchs**: bulk_write em lotes de 500 documentos para eficiência no MongoDB
- **Ordem de carga Gold**: dimensões (`dim_variavel`, `dim_uf`) sempre gravadas antes da `fato_pnad` para respeitar constraints FK

## Como Executar

```bash
# 1. Instalar dependências
pip install -r requirements.txt

# 2. Configurar variáveis de ambiente
cp .env.example .env
# editar .env com credenciais MongoDB e parâmetros IBGE

# 3. Rodar pipeline ETL
python pipeline.py

# 4. (Opcional) Interface web
streamlit run app.py

# 5. (Opcional) MCP Server
python src/mcp/mcp_server.py
```
