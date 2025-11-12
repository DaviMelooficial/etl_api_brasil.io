# ETL API Brasil.io - Gastos Diretos

Pipeline completo de ETL com arquitetura Medallion (Bronze → Silver → Gold) para dados de gastos diretos do governo federal.

## Características

- **Ingestão streaming** com checkpoint/resume
- **Pipeline Bronze → Silver** com validação de qualidade
- **Particionamento** automático por ano_mes
- **Transformações** e limpeza de dados
- **Análise exploratória** automatizada
- **Relatórios** detalhados de qualidade

## Estrutura do Data Lake

```text
dataset/
├── raw/          # JSONs comprimidos (.gz)
├── bronze/       # Dados brutos particionados (parquet)
│   └── ano_mes=YYYY_MM/dados_YYYY_MM.parquet
├── silver/       # Dados limpos e transformados (parquet)
│   └── ano_mes=YYYY_MM/dados_silver.parquet
└── gold/         # Dados agregados (futuro)
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
2. **Listar Arquivos Raw** - JSONs comprimidos
3. **Listar Partições Bronze** - Estrutura de dados
4. **Limpar Raw** - Remove JSONs antigos
5. **Processar Bronze → Silver** - Pipeline de transformação
6. **Visualizar Dados Silver** - Estatísticas e amostra dos dados
7. **Sair** - Encerra o sistema

## Pipeline Silver

### Transformações Aplicadas
- ✅ Remoção de duplicatas completas
- ✅ Conversão de tipos (String → Float, Date)
- ✅ Padronização de textos (uppercase, trim)
- ✅ Limpeza de valores nulos ('NAN', 'NONE', '')
- ✅ Remoção de registros com valores <= 0
- ✅ Criação de coluna ano_mes padronizada

### Validações de Qualidade
- ✅ Colunas críticas sem nulos excessivos (< 5%)
- ✅ Valores numéricos positivos
- ✅ Anos no intervalo válido (2000-2025)
- ✅ Meses válidos (1-12)
- ✅ Estatísticas de nulos por coluna
- ✅ Identificação de valores inválidos

### Arquitetura do Módulo
- `ler_dados_bronze()` - Lê todos os parquets particionados
- `transformar_dados()` - Aplica limpeza e transformações
- `validar_qualidade()` - Executa validações de integridade
- `salvar_silver()` - Salva dados particionados na silver
- `executar_pipeline()` - Orquestra o processo completo

## Uso Rápido

```bash
# Executar menu interativo
python main.py

```

## Tecnologias

- **Python 3.13** | **Pandas** | **PyArrow**
- **Requests** | **Gzip** | **Logging**
