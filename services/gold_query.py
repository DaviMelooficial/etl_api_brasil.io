"""
Modulo para consultar a camada Gold (DuckDB)
Permite acessar as tabelas analiticas sem reprocessar
"""

import duckdb
from pathlib import Path


def conectar_gold():
    """
    Conecta ao banco DuckDB da camada Gold
    Returns:
        Conexao DuckDB
    """
    db_path = Path("dataset/gold/analytics.duckdb")
    
    if not db_path.exists():
        raise FileNotFoundError(
            "Banco de dados Gold nao encontrado. Execute primeiro o processamento Silver->Gold."
        )
    
    return duckdb.connect(str(db_path), read_only=True)


def listar_tabelas(conn=None):
    """Lista todas as tabelas disponíveis no Gold
    Args:
        conn: Conexão DuckDB (opcional, cria uma nova se não fornecida)
    """
    fechar_conn = False
    if conn is None:
        conn = conectar_gold()
        fechar_conn = True
    
    tabelas = conn.execute("SHOW TABLES").fetchall()
    
    if fechar_conn:
        conn.close()
    
    return [t[0] for t in tabelas]


def consultar_kpi(nome_tabela, limite=10, conn=None):
    """
    Consulta uma tabela KPI do Gold
    Args:
        nome_tabela: Nome da tabela (ex: 'gold_gastos_orgao_mes')
        limite: Número máximo de linhas a retornar
        conn: Conexão DuckDB (opcional)
    Returns:
        DataFrame com os resultados
    """
    fechar_conn = False
    if conn is None:
        conn = conectar_gold()
        fechar_conn = True
    
    try:
        df = conn.execute(f"""
            SELECT * FROM {nome_tabela}
            LIMIT {limite}
        """).df()
        
        return df
    finally:
        if fechar_conn:
            conn.close()


def kpi_gastos_orgao_mes(conn=None, top_n=10):
    """Retorna os top N órgãos/mês com maiores gastos
    Args:
        conn: Conexão DuckDB (opcional)
        top_n: Número de registros a retornar (None para todos)
    """
    fechar_conn = False
    if conn is None:
        conn = conectar_gold()
        fechar_conn = True
    
    try:
        query = f"""
            SELECT 
                orgao,
                ano,
                mes,
                valor_total,
                total_transacoes,
                valor_medio
            FROM gold_gastos_orgao_mes
            ORDER BY valor_total DESC
        """
        if top_n:
            query += f" LIMIT {top_n}"
        
        return conn.execute(query).fetchall()
    finally:
        if fechar_conn:
            conn.close()


def kpi_top_favorecidos(conn=None, top_n=10):
    """Retorna os top N favorecidos com maiores recebimentos
    Args:
        conn: Conexão DuckDB (opcional)
        top_n: Número de registros a retornar (None para todos)
    """
    fechar_conn = False
    if conn is None:
        conn = conectar_gold()
        fechar_conn = True
    
    try:
        query = f"""
            SELECT 
                favorecido,
                valor_total,
                total_transacoes,
                ticket_medio,
                primeiro_ano,
                ultimo_ano
            FROM gold_top_favorecidos
            ORDER BY valor_total DESC
        """
        if top_n:
            query += f" LIMIT {top_n}"
        
        return conn.execute(query).fetchall()
    finally:
        if fechar_conn:
            conn.close()


def kpi_top_orgaos(conn=None):
    """Retorna os top 10 órgãos gastadores
    Args:
        conn: Conexão DuckDB (opcional)
    """
    fechar_conn = False
    if conn is None:
        conn = conectar_gold()
        fechar_conn = True
    
    try:
        return conn.execute("""
            SELECT 
                orgao,
                valor_total,
                total_transacoes,
                valor_medio
            FROM gold_top_10_orgaos
            ORDER BY valor_total DESC
        """).fetchall()
    finally:
        if fechar_conn:
            conn.close()


def kpi_evolucao_temporal(conn=None):
    """Retorna a evolução temporal dos gastos
    Args:
        conn: Conexão DuckDB (opcional)
    """
    fechar_conn = False
    if conn is None:
        conn = conectar_gold()
        fechar_conn = True
    
    try:
        return conn.execute("""
            SELECT 
                ano,
                mes,
                valor_total,
                total_transacoes,
                valor_medio,
                total_orgaos,
                total_favorecidos
            FROM gold_evolucao_temporal
            ORDER BY ano, mes
        """).fetchall()
    finally:
        if fechar_conn:
            conn.close()


def executar_sql_customizado(query):
    """
    Executa uma query SQL customizada no Gold
    Args:
        query: Query SQL
    Returns:
        DataFrame com os resultados
    """
    conn = conectar_gold()
    
    try:
        df = conn.execute(query).df()
        return df
    finally:
        conn.close()


def resumo_gold():
    """Mostra um resumo completo de todos os KPIs"""
    print("\n" + "=" * 80)
    print("📊 RESUMO COMPLETO - CAMADA GOLD (DuckDB)")
    print("=" * 80)
    
    conn = conectar_gold()
    
    try:
        # Estatísticas gerais
        print("\n📍 Banco de Dados:")
        print(f"   Localização: dataset/gold/analytics.duckdb")
        
        # Listar tabelas e contagens
        tabelas = conn.execute("SHOW TABLES").fetchall()
        print(f"\n📋 Tabelas Disponíveis: {len(tabelas)}")
        
        for tabela in tabelas:
            nome = tabela[0]
            count = conn.execute(f"SELECT COUNT(*) FROM {nome}").fetchone()[0]
            print(f"   • {nome:30} → {count:,} registros")
        
        print("\n" + "=" * 80)
        
    finally:
        conn.close()


if __name__ == "__main__":
    """Exemplo de uso"""
    
    # Mostrar resumo
    resumo_gold()
    
    # Listar tabelas
    tabelas = listar_tabelas()
    print("\n📋 Tabelas disponíveis:")
    for tabela in tabelas:
        print(f"   • {tabela}")
    
    # Consultar KPIs
    print("\n" + "=" * 80)
    print("🔍 EXEMPLOS DE CONSULTAS")
    print("=" * 80)
    
    conn = conectar_gold()
    
    print("\n[KPI 1] Top 5 Órgãos com Maiores Gastos:")
    result = kpi_top_orgaos(conn)
    for i, row in enumerate(result[:5], 1):
        print(f"  {i}. {row[0][:50]:50} | R$ {row[1]:>15,.2f}")
    
    print("\n[KPI 2] Top 5 Favorecidos:")
    result = kpi_top_favorecidos(conn, top_n=5)
    for i, row in enumerate(result, 1):
        print(f"  {i}. {row[0][:50]:50} | R$ {row[1]:>15,.2f}")
    
    print("\n[KPI 3] Evolução Temporal (últimos 6 meses):")
    result = kpi_evolucao_temporal(conn)
    for row in result[-6:]:
        print(f"  {row[0]:.0f}-{row[1]:02.0f} | R$ {row[2]:>15,.2f} | {row[3]:>7,} trans.")
    
    conn.close()
    print("\n" + "=" * 80)
