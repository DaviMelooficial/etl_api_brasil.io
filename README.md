# Sistema de Ingestão de Gastos Diretos

Sistema otimizado para extração, processamento e análise de dados de gastos diretos do Brasil.io com arquitetura de Data Lake.

## Características

- **Ingestão streaming** com processamento em tempo real
- **JSONs comprimidos** (gzip) para economia de espaço
- **Particionamento** automático por ano_mes
- **Checkpoint/Resume** para recuperação de falhas
- **Análise com DuckDB** para performance

## Estrutura do Data Lake

```text
dataset/
├── raw/          # JSONs comprimidos (.gz)
├── bronze/       # Dados particionados (parquet)
│   ├── ano_mes=2023_01/
│   ├── ano_mes=2023_02/
│   └── ...
└── silver/       # Dados consolidados
```

## Instalação

```bash
# Clonar e instalar dependências
git clone <repo>
cd Atividade_1_engenharia
uv sync
```

## Configuração

Criar arquivo `.env`:

```env
API_KEY=sua_chave_brasil_io
```

## Execução

```bash
# Ativar ambiente
uv run main.py

# Ou manualmente
.venv\Scripts\Activate.ps1
python main.py
```

## Menu Principal

1. **Ingestão Streaming** - Extrai dados da API
2. **Visualizar Bronze** - Estatísticas das partições
3. **Listar Raw** - Arquivos JSON comprimidos
4. **Listar Partições** - Estrutura bronze
5. **Consolidar Silver** - Dados unificados
6. **Limpar Raw** - Remove JSONs antigos

## Análise de Dados

```python
# Exemplo com DuckDB
import duckdb
conn = duckdb.connect()

# Query todas as partições
df = conn.execute("""
    SELECT ano, mes, COUNT(*) as registros
    FROM 'dataset/bronze/ano_mes=*/dados_*.parquet'
    GROUP BY ano, mes
    ORDER BY ano, mes
""").fetchdf()
```

## Tecnologias

- **Python 3.13+**
- **Pandas** - Manipulação de dados
- **DuckDB** - Análise SQL
- **PyArrow** - Engine parquet
- **Requests** - API calls
- **Gzip** - Compressão

## Performance

- ~1M registros processados
- 68 partições geradas
- Queries em ~20ms
- Compressão 70-80%