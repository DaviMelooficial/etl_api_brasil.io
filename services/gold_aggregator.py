"""
Modulo de agregacao Silver -> Gold
Cria tabela analitica com KPIs de negocio usando DuckDB
"""

import duckdb
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def processar_gold():
    """
    Processa dados da camada Silver e gera tabela analitica Gold com KPIs
    
    KPIs implementados:
    1. Gastos por orgao e mes (serie temporal de gastos por orgao)
    2. Top 20 favorecidos (ranking de maiores recebedores)
    3. Top 10 orgaos (ranking de maiores gastadores)
    4. BONUS: Evolucao temporal agregada (tendencia mensal)
    """
    
    silver_path = Path("dataset/silver")
    gold_path = Path("dataset/gold")
    
    if not silver_path.exists():
        raise FileNotFoundError("Camada Silver nao encontrada. Execute primeiro o processamento Bronze->Silver.")
    
    gold_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("INICIANDO PROCESSAMENTO SILVER -> GOLD")
    logger.info("=" * 60)
    
    # Conectar ao DuckDB PERSISTENTE (arquivo)
    db_path = gold_path / "analytics.duckdb"
    conn = duckdb.connect(str(db_path))
    logger.info(f"Conectado ao DuckDB: {db_path}")
    
    # Ler todos os dados da Silver usando DuckDB
    logger.info("Lendo dados da camada Silver com DuckDB...")
    silver_pattern = str(silver_path / "**/*.parquet")
    
    # Usar CREATE OR REPLACE para permitir reprocessamento
    conn.execute(f"""
        CREATE OR REPLACE TABLE silver AS 
        SELECT * FROM read_parquet('{silver_pattern}', 
            hive_partitioning=true,
            union_by_name=true
        )
    """)
    
    total_registros = conn.execute("SELECT COUNT(*) FROM silver").fetchone()[0]
    logger.info(f"Total de registros carregados: {total_registros:,}")
    
    # =========================================================================
    # KPI 1: Gastos Totais por Órgão e Mês (Tabela de Fatos Principal)
    # =========================================================================
    logger.info("\nKPI 1: Calculando gastos por órgão e mês...")
    
    conn.execute("""
        CREATE OR REPLACE TABLE gold_gastos_orgao_mes AS
        SELECT 
            ano,
            mes,
            nome_orgao_superior as orgao,
            COUNT(*) as total_transacoes,
            SUM(valor) as valor_total,
            AVG(valor) as valor_medio,
            MIN(valor) as valor_minimo,
            MAX(valor) as valor_maximo
        FROM silver
        WHERE nome_orgao_superior IS NOT NULL
        GROUP BY ano, mes, nome_orgao_superior
        ORDER BY ano DESC, mes DESC, valor_total DESC
    """)
    
    # Salvar KPI 1
    conn.execute(f"""
        COPY gold_gastos_orgao_mes 
        TO '{gold_path / "kpi_gastos_orgao_mes.parquet"}' 
        (FORMAT PARQUET)
    """)
    
    kpi1_registros = conn.execute("SELECT COUNT(*) FROM gold_gastos_orgao_mes").fetchone()[0]
    logger.info(f"  -> KPI 1 salvo: {kpi1_registros:,} registros")
    
    # =========================================================================
    # KPI 2: Gastos por Favorecido (Top 20)
    # =========================================================================
    logger.info("\nKPI 2: Calculando top 20 favorecidos com maiores recebimentos...")
    
    conn.execute("""
        CREATE OR REPLACE TABLE gold_top_favorecidos AS
        SELECT 
            nome_favorecido as favorecido,
            COUNT(*) as total_transacoes,
            SUM(valor) as valor_total,
            AVG(valor) as ticket_medio,
            MIN(valor) as valor_minimo,
            MAX(valor) as valor_maximo,
            COUNT(DISTINCT nome_orgao_superior) as total_orgaos_distintos,
            MIN(ano) as primeiro_ano,
            MAX(ano) as ultimo_ano
        FROM silver
        WHERE nome_favorecido IS NOT NULL 
          AND nome_favorecido != ''
        GROUP BY nome_favorecido
        ORDER BY valor_total DESC
        LIMIT 20
    """)
    
    # Salvar KPI 2
    conn.execute(f"""
        COPY gold_top_favorecidos 
        TO '{gold_path / "kpi_top_favorecidos.parquet"}' 
        (FORMAT PARQUET)
    """)
    
    kpi2_registros = conn.execute("SELECT COUNT(*) FROM gold_top_favorecidos").fetchone()[0]
    logger.info(f"  -> KPI 2 salvo: {kpi2_registros:,} registros (Top favorecidos)")
    
    # =========================================================================
    # KPI 3: Top 10 Órgãos com Maiores Gastos (Ranking)
    # =========================================================================
    logger.info("\nKPI 3: Calculando top 10 órgãos com maiores gastos...")
    
    conn.execute("""
        CREATE OR REPLACE TABLE gold_top_10_orgaos AS
        SELECT 
            nome_orgao_superior as orgao,
            COUNT(*) as total_transacoes,
            SUM(valor) as valor_total,
            AVG(valor) as valor_medio,
            MIN(valor) as valor_minimo,
            MAX(valor) as valor_maximo
        FROM silver
        WHERE nome_orgao_superior IS NOT NULL
        GROUP BY nome_orgao_superior
        ORDER BY valor_total DESC
        LIMIT 10
    """)
    
    # Salvar KPI 3
    conn.execute(f"""
        COPY gold_top_10_orgaos 
        TO '{gold_path / "kpi_top_10_orgaos.parquet"}' 
        (FORMAT PARQUET)
    """)
    
    kpi3_registros = conn.execute("SELECT COUNT(*) FROM gold_top_10_orgaos").fetchone()[0]
    logger.info(f"  -> KPI 3 salvo: {kpi3_registros:,} registros")
    
    # =========================================================================
    # BONUS: Evolução Temporal de Gastos (série temporal)
    # =========================================================================
    logger.info("\nBONUS: Calculando evolução temporal de gastos...")
    
    conn.execute("""
        CREATE OR REPLACE TABLE gold_evolucao_temporal AS
        SELECT 
            ano,
            mes,
            COUNT(*) as total_transacoes,
            SUM(valor) as valor_total,
            AVG(valor) as valor_medio,
            COUNT(DISTINCT nome_orgao_superior) as total_orgaos,
            COUNT(DISTINCT nome_favorecido) as total_favorecidos
        FROM silver
        GROUP BY ano, mes
        ORDER BY ano, mes
    """)
    
    # Salvar BONUS
    conn.execute(f"""
        COPY gold_evolucao_temporal 
        TO '{gold_path / "kpi_evolucao_temporal.parquet"}' 
        (FORMAT PARQUET)
    """)
    
    bonus_registros = conn.execute("SELECT COUNT(*) FROM gold_evolucao_temporal").fetchone()[0]
    logger.info(f"  -> Evolução temporal salva: {bonus_registros:,} registros")
    
    # =========================================================================
    # Resumo Final
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("RESUMO DOS KPIs GERADOS")
    logger.info("=" * 60)
    
    # Mostrar amostra do KPI 1
    logger.info("\nKPI 1 - Top 5 órgãos/mês com maiores gastos:")
    resultado = conn.execute("""
        SELECT orgao, ano, mes, valor_total, total_transacoes 
        FROM gold_gastos_orgao_mes 
        LIMIT 5
    """).fetchall()
    
    for row in resultado:
        logger.info(f"  {row[0][:50]:50} | {row[1]:.0f}-{row[2]:02.0f} | R$ {row[3]:,.2f} | {row[4]:,} transações")
    
    # Mostrar KPI 2
    logger.info("\nKPI 2 - Top 10 favorecidos com maiores recebimentos:")
    resultado = conn.execute("""
        SELECT favorecido, ticket_medio, total_transacoes, valor_total 
        FROM gold_top_favorecidos
        ORDER BY valor_total DESC
        LIMIT 10
    """).fetchall()
    
    for row in resultado:
        logger.info(f"  {row[0][:50]:50} | Ticket: R$ {row[1]:,.2f} | Total: R$ {row[3]:,.2f}")
    
    # Mostrar KPI 3
    logger.info("\nKPI 3 - Top 10 órgãos com maiores gastos:")
    resultado = conn.execute("""
        SELECT orgao, valor_total, total_transacoes 
        FROM gold_top_10_orgaos
    """).fetchall()
    
    for i, row in enumerate(resultado, 1):
        logger.info(f"  {i}. {row[0][:50]:50} | R$ {row[1]:,.2f} | {row[2]:,} transações")
    
    conn.close()
    
    logger.info("\n" + "=" * 60)
    logger.info("PROCESSAMENTO GOLD CONCLUÍDO COM SUCESSO")
    logger.info(f"Arquivos salvos em: {gold_path}")
    logger.info("=" * 60)
    
    return {
        'total_registros_silver': total_registros,
        'kpi1_registros': kpi1_registros,
        'kpi2_registros': kpi2_registros,
        'kpi3_registros': kpi3_registros,
        'bonus_registros': bonus_registros,
        'arquivos_gerados': [
            'kpi_gastos_orgao_mes.parquet',
            'kpi_top_favorecidos.parquet',
            'kpi_top_10_orgaos.parquet',
            'kpi_evolucao_temporal.parquet'
        ]
    }


if __name__ == "__main__":
    try:
        processar_gold()
    except Exception as e:
        logger.error(f"Erro ao processar camada Gold: {e}")
        import traceback
        traceback.print_exc()
