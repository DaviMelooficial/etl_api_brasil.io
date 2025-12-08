# ELT API Brasil.io - Gastos Diretos do Governo Federal

Pipeline ELT com arquitetura Medallion (Raw → Bronze → Silver → Gold) para analise de gastos governamentais, orquestrado pelo Apache Airflow em Docker.

## Caracteristicas

- ELT Pattern: carregamento seguido de transformacao
- Arquitetura Medallion: 4 camadas (Raw → Bronze → Silver → Gold)
- Docker + Airflow: orquestracao em containers
- DuckDB: banco analitico persistente na camada Gold
- Limite de 500 mil registros
- Processamento paralelo com particionamento Hive
- 4 KPIs na camada Gold (tabelas SQL)
- Validacao automatica em todas as etapas

## Estrutura do Projeto

```
etl_api_brasil.io/
│
├── CONFIGURACAO
│   ├── .env                          # API Key Brasil.io
│   ├── pyproject.toml                # Dependencias (uv)
│   ├── docker-compose.yml            # Airflow (4 services)
│   └── start.bat                     # Iniciar Airflow (Windows)
│
├── DAG AIRFLOW
│   └── dags/
│       └── elt_gastos_diretos_dag.py # 4 tarefas: extract → bronze → silver → gold
│
├── SERVICES (Logica de Negocio)
│   ├── services/
│   │   ├── request.py                # Extracao API → Raw (checkpoint/resume)
│   │   ├── silver_transformer.py     # Bronze → Silver (limpeza DuckDB)
│   │   ├── gold_aggregator.py        # Silver → Gold (KPIs DuckDB)
│   │   └── gold_query.py             # Interface SQL para consultar Gold
│
├── DATASET (Arquitetura Medallion)
│   └── dataset/
│       ├── raw/                      # JSONs comprimidos (.json.gz) + checkpoint
│       ├── bronze/                   # Parquet particionado (68 particoes)
│       │   └── ano_mes=YYYY_MM/
│       ├── silver/                   # Parquet limpo e validado
│       │   └── ano_mes=YYYY_MM/
│       └── gold/                     # DuckDB + Parquet (KPIs)
│           ├── analytics.duckdb      # Banco DuckDB persistente (29 MB)
│           ├── kpi_gastos_orgao_mes.parquet
│           ├── kpi_top_favorecidos.parquet
│           ├── kpi_top_10_orgaos.parquet
│           └── kpi_evolucao_temporal.parquet
│
└── INTERFACE
    ├── main.py                       # Menu interativo (6 opcoes)
    ├── executar_gold.py              # Helper: regenerar Gold
    └── testar_duckdb_gold.py         # Validacao completa DuckDB
```

## Instalacao e Execucao

### Passo 1: Setup Inicial

```bash
# Clonar repositório
git clone <repo>
cd etl_api_brasil.io

# Instalar dependências (Python 3.13 + uv)
uv sync

# Configurar API Key
echo API_KEY=sua_chave_brasil_io > .env
```

### Passo 2: Executar via Airflow (Recomendado)

```powershell
# Iniciar Airflow com Docker (Windows)
.\start.bat

# Ou manualmente:
docker-compose up -d

# Acessar interface: http://localhost:8080
# Login: admin / admin
```

Passos no Airflow:
1. Localizar DAG elt_gastos_diretos
2. Ativar toggle
3. Clicar em Trigger DAG
4. Aguardar conclusao (5-10 min para 500k registros)

```powershell
# Parar Airflow
docker-compose down
```

### Passo 3: Executar via Menu (Desenvolvimento)

```bash
uv run main.py

# Menu disponivel:
# 1. Ingestao Streaming (API → Raw → Bronze)
# 2. Processar Bronze → Silver
# 3. Visualizar dados Silver
# 4. Processar Silver → Gold
# 5. Visualizar KPIs Gold (DuckDB)
# 6. Sair
```

## Arquitetura do Pipeline

### DAG Airflow: 4 Tarefas Sequenciais

```
extract_to_bronze → bronze_to_silver → silver_to_gold → validate_gold
```

#### Task 1: extract_to_bronze
- Arquivo: services/request.py
- Funcao: ingestao_gastos_diretos()
- Input: API Brasil.io
- Output: 
  - dataset/raw/*.json.gz (JSONs comprimidos)
  - dataset/bronze/ano_mes=YYYY_MM/*.parquet (68 particoes)
- Caracteristicas:
  - Checkpoint/resume automatico
  - Limite exato de 500k registros
  - Particionamento Hive por ano_mes
  - Skip se dados ja existem

#### Task 2: bronze_to_silver
- Arquivo: services/silver_transformer.py
- Funcao: executar_pipeline()
- Engine: DuckDB (SQL + union_by_name)
- Transformacoes:
  - Remocao de duplicatas
  - Conversao de tipos (string → float, date)
  - Padronizacao (UPPER, TRIM, strip)
  - Limpeza de nulos (NAN → NULL)
  - Filtros (valor > 0, anos validos)
  - Validacao de qualidade (< 5% nulos)
- Output: dataset/silver/ano_mes=YYYY_MM/*.parquet

#### Task 3: silver_to_gold
- Arquivo: services/gold_aggregator.py
- Funcao: processar_gold()
- Engine: DuckDB persistente
- Output:
  - dataset/gold/analytics.duckdb (Banco SQL)
  - dataset/gold/*.parquet (Backup)
- Tabelas DuckDB:
  - gold_gastos_orgao_mes (218 registros)
  - gold_top_favorecidos (20 registros)
  - gold_top_10_orgaos (10 registros)
  - gold_evolucao_temporal (68 registros)

#### Task 4: validate_gold
- Validacoes:
  - Banco DuckDB existe e acessivel
  - 4 tabelas criadas
  - Registros > 0 em cada tabela
  - Queries SQL funcionam

## Detalhamento dos Arquivos

### dags/elt_gastos_diretos_dag.py (198 linhas)
Orquestrador principal do Airflow

```python
# DAG com 4 tarefas:
extract_to_bronze >> bronze_to_silver >> silver_to_gold >> validate_gold

# Configuração:
# - Schedule: None (manual)
# - Retries: 1
# - Owner: davi_melo
```

### services/request.py (120 linhas)
Extracao da API Brasil.io

```python
def ingestao_gastos_diretos(num_pages=250, limite_registros=500000):
    """
    Extrai dados da API e salva em:
    1. Raw: JSONs comprimidos (.json.gz)
    2. Bronze: Parquet particionado (ano_mes=YYYY_MM)
    
    Caracteristicas:
    - Checkpoint automatico (resume de falhas)
    - Limite exato de registros (500k)
    - Streaming (memoria constante)
    - Particionamento Hive
    """
```

### services/silver_transformer.py (150 linhas)
Pipeline Bronze → Silver (DuckDB)

```python
def executar_pipeline():
    """
    Transformacoes com DuckDB SQL:
    
    1. Leitura: read_parquet com union_by_name
    2. Limpeza: nulos, duplicatas, tipos
    3. Validacao: qualidade de dados
    4. Output: Parquet particionado
    
    Retorna: dict com estatisticas
    """
```

### services/gold_aggregator.py (255 linhas)
Agregacoes Silver → Gold (DuckDB)

```python
def processar_gold():
    """
    Cria 4 tabelas SQL no DuckDB:
    
    CREATE OR REPLACE TABLE gold_gastos_orgao_mes AS
    SELECT orgao, ano, mes, SUM(valor) as valor_total, ...
    
    CREATE OR REPLACE TABLE gold_top_favorecidos AS
    SELECT favorecido, SUM(valor) as valor_total, ...
    
    CREATE OR REPLACE TABLE gold_top_10_orgaos AS
    SELECT orgao, SUM(valor) as valor_total, ...
    
    CREATE OR REPLACE TABLE gold_evolucao_temporal AS
    SELECT ano, mes, SUM(valor) as valor_total, ...
    
    Output:
    - analytics.duckdb (banco persistente)
    - *.parquet (backup)
    """
```

### services/gold_query.py (250 linhas)
Interface SQL para consultar Gold

```python
# Funções principais:
conectar_gold()                  # Conexão read-only ao DuckDB
listar_tabelas()                 # Lista tabelas disponíveis
kpi_gastos_orgao_mes(top_n=10)   # Top N gastos por órgão/mês
kpi_top_favorecidos(top_n=10)    # Top N favorecidos
kpi_top_orgaos()                 # Top 10 órgãos
kpi_evolucao_temporal()          # Série temporal
executar_sql_customizado(query)  # SQL livre

# Uso:
from services.gold_query import *
conn = conectar_gold()
result = kpi_top_orgaos(conn)
```

### main.py (214 linhas)
Menu interativo CLI

```python
# 6 opcoes:
1. Ingerir dados da API (500k)
2. Processar Bronze → Silver
3. Visualizar dados Silver
4. Processar Silver → Gold
5. Visualizar KPIs Gold (DuckDB)
6. Sair
```

### executar_gold.py (15 linhas)
Helper para regenerar Gold

```python
from services.gold_aggregator import processar_gold
resultado = processar_gold()
# Mostra estatisticas: registros processados, KPIs gerados
```

### testar_duckdb_gold.py (100 linhas)
Validacao completa do DuckDB Gold

```python
def testar_duckdb_gold():
    """
    Testa:
    - Banco existe?
    - Conexao funciona?
    - 4 tabelas presentes?
    - Queries retornam dados?
    - Contagem de registros correta?
    """
```

## KPIs da Camada Gold

### KPI 1: Gastos por Orgao e Mes
Tabela: gold_gastos_orgao_mes (218 registros)

```sql
SELECT orgao, ano, mes, valor_total, total_transacoes
FROM gold_gastos_orgao_mes
ORDER BY valor_total DESC
LIMIT 10
```

Resultado esperado:
```
MINISTERIO DA EDUCACAO | 2017-12 | R$ 6.950.541.035,56 | 297.522 trans.
MINIST.DOS TRANSP.     | 2017-12 | R$ 1.358.168.698,46 | 9.979 trans.
...
```

### KPI 2: Top Favorecidos
Tabela: gold_top_favorecidos (20 registros)

```sql
SELECT favorecido, valor_total, total_transacoes, ticket_medio
FROM gold_top_favorecidos
ORDER BY valor_total DESC
```

### KPI 3: Top 10 Orgaos
Tabela: gold_top_10_orgaos (10 registros)

```sql
SELECT orgao, valor_total, total_transacoes, valor_medio
FROM gold_top_10_orgaos
```

### KPI 4: Evolucao Temporal
Tabela: gold_evolucao_temporal (68 registros - 68 meses)

```sql
SELECT ano, mes, valor_total, total_transacoes, 
       total_orgaos, total_favorecidos
FROM gold_evolucao_temporal
ORDER BY ano, mes
```

## Tecnologias

| Ferramenta | Uso | Versão |
|------------|-----|--------|
| **Python** | Linguagem base | 3.13 |
| **uv** | Gerenciador de pacotes | 0.5+ |
| **Apache Airflow** | Orquestração | 2.8.1 |
| **Docker** | Containerização | 24.0+ |
| **PostgreSQL** | Metadata Airflow | 13 |
| **DuckDB** | Processamento analítico | 1.1.3 |
| **Pandas** | Manipulação de dados | 2.2+ |
| **PyArrow** | Parquet I/O | 18.0+ |
| **Requests** | HTTP Client | 2.32+ |

## Volume de Dados

| Camada | Formato | Tamanho | Registros | Partições |
|--------|---------|---------|-----------|-----------|
| **Raw** | JSON.gz | ~80 MB | 500.000 | 250 arquivos |
| **Bronze** | Parquet | ~45 MB | 500.000 | 68 (ano_mes) |
| **Silver** | Parquet | ~40 MB | ~495.000 | 68 (ano_mes) |
| **Gold** | DuckDB | 29 MB | 316 | 4 tabelas |
| **Gold** | Parquet | ~16 KB | 316 | 4 arquivos |

Total: ~150 MB

## Validacoes Implementadas

### Bronze
- Particionamento correto (68 particoes ano_mes)
- Schema consistente (union_by_name)
- Sem dados corrompidos

### Silver
- Duplicatas removidas
- Nulos criticos < 5%
- Valores numericos > 0
- Anos validos (2000-2025)
- Meses validos (1-12)
- Tipos corretos (float, date)

### Gold
- Banco DuckDB criado
- 4 tabelas materializadas
- Registros > 0
- Queries SQL funcionais
- Parquet de backup gerado

## Requisitos Academicos Atendidos

| Requisito | Status | Evidencia |
|-----------|--------|-----------|
| Pipeline ELT | Sim | Load → Transform (Bronze → Silver → Gold) |
| Arquitetura Medallion | Sim | Raw → Bronze → Silver → Gold |
| 500k registros exatos | Sim | Limite em request.py |
| Orquestracao Airflow | Sim | DAG com 4 tarefas |
| Camada Gold DuckDB | Sim | analytics.duckdb (29 MB, 4 tabelas) |
| KPIs analiticos | Sim | 4 tabelas SQL prontas |
| Validacao de qualidade | Sim | Todas as camadas |
| Particionamento | Sim | Hive (ano_mes) |
| Checkpoint/Resume | Sim | dataset/raw/checkpoint.txt |

## Como Consultar os Dados

### Via Python

```python
from services.gold_query import *

# Conectar
conn = conectar_gold()

# Listar tabelas
tabelas = listar_tabelas(conn)  # ['gold_gastos_orgao_mes', ...]

# KPI 1: Top 10 gastos
result = kpi_gastos_orgao_mes(conn, top_n=10)
for row in result:
    print(f"{row[0]} | {row[3]:,.2f}")

# KPI 2: Top favorecidos
result = kpi_top_favorecidos(conn, top_n=5)

# SQL customizado
result = conn.execute("""
    SELECT orgao, SUM(valor_total) as total
    FROM gold_gastos_orgao_mes
    WHERE ano = 2017
    GROUP BY orgao
    ORDER BY total DESC
    LIMIT 5
""").fetchall()

conn.close()
```

### Via DuckDB CLI

```bash
# Instalar DuckDB CLI
# Windows: https://duckdb.org/docs/installation/

# Abrir banco
duckdb dataset/gold/analytics.duckdb

# Consultar
SELECT * FROM gold_top_10_orgaos;
SELECT COUNT(*) FROM gold_gastos_orgao_mes;
SHOW TABLES;
.quit
```

## Troubleshooting

### Erro: Table already exists
Solucao: Implementado CREATE OR REPLACE TABLE em todas as tabelas Gold. Reprocessamento ilimitado.

### Docker nao inicia no Windows
Solucao: Instale Docker Desktop e habilite WSL 2.

### API Key invalida
Solucao: Obtenha key em https://brasil.io/auth/tokens/ e adicione no .env

### DuckDB: Database is locked
Solucao: Feche todas as conexoes antes de reprocessar. Use gold_query.py com read_only=True.

## Estrutura Final do Projeto

```text
etl_api_brasil.io/           [Projeto completo]
├── .env                     [API Key Brasil.io]
├── .gitignore              [Ignora: .venv, logs, __pycache__]
├── pyproject.toml          [Dependências uv]
├── uv.lock                 [Lock file]
├── docker-compose.yml      [Airflow stack]
├── start.bat               [Launcher Windows]
├── main.py                 [Menu interativo]
├── executar_gold.py        [Helper Gold]
├── testar_duckdb_gold.py   [Validador DuckDB]
│
├── dags/
│   └── elt_gastos_diretos_dag.py  [DAG Airflow - 4 tasks]
│
├── services/
│   ├── request.py                  [Extração API]
│   ├── silver_transformer.py       [Pipeline Bronze→Silver]
│   ├── gold_aggregator.py          [Pipeline Silver→Gold]
│   └── gold_query.py               [Interface SQL]
│
└── dataset/
    ├── raw/                        [JSON.gz + checkpoint]
    ├── bronze/ano_mes=*/           [Parquet - 68 partições]
    ├── silver/ano_mes=*/           [Parquet - 68 partições]
    └── gold/
        ├── analytics.duckdb        [Banco SQL - 29 MB]
        └── *.parquet               [Backup - 4 arquivos]
```

## Resumo Final

Apos execucao completa, o projeto gera:

Raw: 250 arquivos JSON.gz (~80 MB)  
Bronze: 68 particoes Parquet ano_mes (~45 MB)  
Silver: 68 particoes Parquet limpas (~40 MB)  
Gold: 1 banco DuckDB + 4 Parquet (~29 MB)

Total processado: 500.000 registros exatos  
KPIs disponiveis: 4 tabelas SQL prontas para BI  
Tempo execucao: ~5-10 minutos (depende do hardware)

---

Desenvolvido por Davi Melo | Projeto Academico - Engenharia de Dados | Dezembro 2025
