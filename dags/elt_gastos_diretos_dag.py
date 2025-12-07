"""
DAG do Airflow para Pipeline ELT de Gastos Diretos
Arquitetura: Bronze -> Silver -> Gold

Autor: Davi Melo
Data: Dezembro 2025
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.request import ingestão_gastos_diretos, request_num_pages
from services.silver_transformer import executar_pipeline
from services.gold_aggregator import processar_gold

# Configurações padrão da DAG
default_args = {
    'owner': 'davi_melo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
dag = DAG(
    'elt_gastos_diretos',
    default_args=default_args,
    description='Pipeline ELT - Gastos Diretos (pula extração se dados existirem)',
    schedule_interval=None,  # Execução manual
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=['elt', 'gastos_diretos', 'medallion', 'bronze', 'silver', 'gold'],
)


# ============================================================================
# FUNÇÕES DAS TAREFAS
# ============================================================================

def extract_to_bronze(**context):
    """
    Tarefa 1: Extração de dados da API para camada Bronze
    Pula a extração se os dados já existirem
    """
    print("=" * 60)
    print("INICIANDO EXTRACAO: API -> BRONZE")
    print("=" * 60)
    
    try:
        # Verificar se já existem dados na Bronze
        bronze_path = Path("dataset/bronze")
        if bronze_path.exists():
            particoes = list(bronze_path.glob("ano_mes=*/"))
            if particoes:
                print(f"\nDados ja existem na Bronze!")
                print(f"   Particoes encontradas: {len(particoes)}")
                print(f"   Pulando extracao da API...")
                print(f"\nPara forcar nova extracao, delete a pasta dataset/bronze/")
                return
        
        # Se não existir, fazer a extração
        print("\nDados nao encontrados. Iniciando extracao da API...")
        num_pages = request_num_pages()
        print(f"Total de paginas disponiveis: {num_pages}")
        
        # Ingerir dados com limite de 500.000 registros
        ingestão_gastos_diretos(num_pages, limite_registros=500000)
        
        print("\nExtracao para Bronze concluida com sucesso!")
        
    except Exception as e:
        print(f"\nErro na extracao: {e}")
        raise


def transform_bronze_to_silver(**context):
    """
    Tarefa 2: Transformacao Bronze -> Silver
    """
    print("=" * 60)
    print("INICIANDO TRANSFORMACAO: BRONZE -> SILVER")
    print("=" * 60)
    
    try:
        # Executar pipeline de transformação
        df_silver, validacao = executar_pipeline()
        
        # Publicar métricas no XCom para próximas tarefas
        context['task_instance'].xcom_push(
            key='silver_stats',
            value={
                'total_registros': validacao['total_registros'],
                'status': validacao['status']
            }
        )
        
        print("\nTransformacao para Silver concluida com sucesso!")
        
    except Exception as e:
        print(f"\nErro na transformacao: {e}")
        raise


def transform_silver_to_gold(**context):
    """
    Tarefa 3: Agregacao Silver -> Gold (KPIs)
    """
    print("=" * 60)
    print("INICIANDO AGREGACAO: SILVER -> GOLD")
    print("=" * 60)
    
    try:
        # Processar KPIs
        resultado = processar_gold()
        
        # Publicar metricas no XCom
        context['task_instance'].xcom_push(
            key='gold_stats',
            value=resultado
        )
        
        print("\nAgregacao para Gold concluida com sucesso!")
        
    except Exception as e:
        print(f"\nErro na agregacao: {e}")
        raise


def validate_gold(**context):
    """
    Tarefa 4 (Opcional): Validação da camada Gold
    """
    print("=" * 60)
    print("VALIDANDO CAMADA GOLD")
    print("=" * 60)
    
    import duckdb
    from pathlib import Path
    
    try:
        gold_path = Path("dataset/gold")
        
        if not gold_path.exists():
            raise FileNotFoundError("Camada Gold nao encontrada!")
        
        # Conectar DuckDB
        db_path = gold_path / "analytics.duckdb"
        if not db_path.exists():
            raise FileNotFoundError(f"Banco DuckDB nao encontrado: {db_path}")
        
        conn = duckdb.connect(str(db_path), read_only=True)
        
        # Validar tabelas DuckDB
        tabelas = [
            'gold_gastos_orgao_mes',
            'gold_top_favorecidos',
            'gold_top_10_orgaos',
            'gold_evolucao_temporal'
        ]
        
        validacoes = {}
        
        for tabela in tabelas:
            try:
                result = conn.execute(f"SELECT COUNT(*) FROM {tabela}").fetchone()
                registros = result[0]
                
                status = 'OK' if registros > 0 else 'VAZIO'
                validacoes[tabela] = {'status': status, 'registros': registros}
                
                print(f"[OK] {tabela}: {registros:,} registros - {status}")
            except Exception as e:
                print(f"[ERRO] {tabela}: {e}")
                validacoes[tabela] = {'status': 'FALHA', 'registros': 0}
        
        conn.close()
        
        # Publicar validacoes no XCom
        context['task_instance'].xcom_push(
            key='validacoes_gold',
            value=validacoes
        )
        
        print("\nValidacao concluida!")
        
    except Exception as e:
        print(f"\nErro na validacao: {e}")
        raise


# ============================================================================
# DEFINIÇÃO DAS TAREFAS
# ============================================================================

task_extract = PythonOperator(
    task_id='extract_to_bronze',
    python_callable=extract_to_bronze,
    provide_context=True,
    dag=dag,
)

task_bronze_to_silver = PythonOperator(
    task_id='bronze_to_silver',
    python_callable=transform_bronze_to_silver,
    provide_context=True,
    dag=dag,
)

task_silver_to_gold = PythonOperator(
    task_id='silver_to_gold',
    python_callable=transform_silver_to_gold,
    provide_context=True,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='validate_gold',
    python_callable=validate_gold,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# DEPENDÊNCIAS (ORDEM DE EXECUÇÃO)
# ============================================================================

task_extract >> task_bronze_to_silver >> task_silver_to_gold >> task_validate
