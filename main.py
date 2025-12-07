from services.request import ingestão_gastos_diretos, request_num_pages
from services.silver_transformer import executar_pipeline
from services.gold_aggregator import processar_gold
import os
import time

def main():

    while True:
        print("Sistema de Ingestao de Gastos Diretos")
        print("-" * 40)
        print("1. Ingestao Streaming (API -> Bronze)")
        print("2. Processar Bronze -> Silver")
        print("3. Visualizar Dados Silver")
        print("4. Processar Silver -> Gold (KPIs)")
        print("5. Visualizar KPIs Gold")
        print("6. Sair")
        print("-" * 40)

        opcao = input("Escolha uma opcao: ")

        if opcao == "1":
            try:
                num_pages = request_num_pages()
                print(f"Total de paginas encontradas: {num_pages}")
                
                # Ingerir com limite de 500.000 registros
                ingestão_gastos_diretos(num_pages, limite_registros=500000)
            except Exception as e:
                print(f"Erro durante a ingestao: {e}")
            input("\nPressione Enter para continuar...")
            os.system('cls')

        elif opcao == "2":
            print("\n" + "=" * 60)
            print("PROCESSAMENTO BRONZE -> SILVER")
            print("=" * 60)
            try:
                df_silver, validacao = executar_pipeline()
                
                print("\n" + "=" * 60)
                print("RESUMO DO PROCESSAMENTO")
                print("=" * 60)
                print(f"Total de registros processados: {validacao['total_registros']:,}")
                print(f"Status da validacao: {validacao['status']}")
                
                print("\nColunas criticas:")
                for col, stats in validacao['colunas_criticas'].items():
                    print(f"  {col}: {stats['nulos']:,} nulos ({stats['percentual']}%)")
                
                if validacao['valores_invalidos']:
                    print("\nValores invalidos encontrados:")
                    for tipo, qtd in validacao['valores_invalidos'].items():
                        if qtd > 0:
                            print(f"  {tipo}: {qtd:,}")
                
                print("\nDados salvos em: dataset/silver/")
                print("=" * 60)
                
            except Exception as e:
                print(f"\nErro durante o processamento: {e}")
                import traceback
                traceback.print_exc()
            
            input("\nPressione Enter para continuar...")
            os.system('cls')

        elif opcao == "6":
            print("\n" + "=" * 60)
            print("VISUALIZAR DADOS SILVER")
            print("=" * 60)
            try:
                # Verificar se já existe dados silver
                from pathlib import Path
                import pandas as pd
                
                silver_path = Path("dataset/silver")
                
                if not silver_path.exists() or not list(silver_path.glob("**/*.parquet")):
                    print("\nNenhum dado encontrado na camada Silver.")
                    print("Execute primeiro a opcao 2 (Processar Bronze -> Silver)")
                else:
                    print("\nLendo dados da camada Silver...")
                    
                    # Ler dados silver
                    dfs = []
                    for arquivo in silver_path.glob("**/*.parquet"):
                        dfs.append(pd.read_parquet(arquivo))
                    df_silver = pd.concat(dfs, ignore_index=True)
                    
                    print("\n" + "=" * 60)
                    print("INFORMACOES DOS DADOS SILVER")
                    print("=" * 60)
                    print(f"\nTotal de registros: {len(df_silver):,}")
                    print(f"Colunas: {list(df_silver.columns)}")
                    
                    # Estatísticas básicas
                    if 'valor' in df_silver.columns:
                        print(f"\nValor total: R$ {df_silver['valor'].sum():,.2f}")
                        print(f"Valor medio: R$ {df_silver['valor'].mean():,.2f}")
                        print(f"Valor maximo: R$ {df_silver['valor'].max():,.2f}")
                    
                    if 'ano' in df_silver.columns:
                        print(f"\nPeriodo: {df_silver['ano'].min():.0f} - {df_silver['ano'].max():.0f}")
                    
                    print("\nAmostra dos dados:")
                    print(df_silver.head(10))
                    print("=" * 60)
                    
            except Exception as e:
                print(f"\nErro ao visualizar dados: {e}")
                import traceback
                traceback.print_exc()
            
            input("\nPressione Enter para continuar...")
            os.system('cls')

        elif opcao == "4":
            print("\n" + "=" * 60)
            print("PROCESSAMENTO SILVER -> GOLD (KPIs)")
            print("=" * 60)
            try:
                resultado = processar_gold()
                
                print("\n" + "=" * 60)
                print("RESUMO DO PROCESSAMENTO")
                print("=" * 60)
                print(f"Registros processados: {resultado['total_registros_silver']:,}")
                print(f"\nKPIs gerados:")
                for arquivo in resultado['arquivos_gerados']:
                    print(f"  - {arquivo}")
                print("\nDados salvos em: dataset/gold/")
                print("=" * 60)
                
            except Exception as e:
                print(f"\nErro durante o processamento: {e}")
                import traceback
                traceback.print_exc()
            
            input("\nPressione Enter para continuar...")
            os.system('cls')

        elif opcao == "5":
            print("\n" + "=" * 60)
            print("VISUALIZAR KPIs DA CAMADA GOLD")
            print("=" * 60)
            try:
                from services.gold_query import (
                    conectar_gold, 
                    listar_tabelas, 
                    kpi_gastos_orgao_mes, 
                    kpi_top_favorecidos,
                    kpi_top_orgaos,
                    kpi_evolucao_temporal
                )
                
                # Conectar ao DuckDB Gold
                conn = conectar_gold()
                if conn is None:
                    print("\nNenhum banco de dados encontrado na camada Gold.")
                    print("Execute primeiro a opcao 4 (Processar Silver -> Gold)")
                else:
                    # Listar tabelas disponiveis
                    tabelas = listar_tabelas(conn)
                    print(f"\nTabelas disponiveis: {', '.join(tabelas)}")
                    
                    # KPI 1: Gastos por Orgao e Mes
                    print("\n[KPI 1] TOP 10 GASTOS POR ORGAO/MES:")
                    print("-" * 60)
                    result = kpi_gastos_orgao_mes(conn, top_n=10)
                    for row in result:
                        print(f"{row[0][:40]:40} | {row[1]:.0f}-{row[2]:02.0f} | R$ {row[3]:>15,.2f} | {row[4]:>6,} trans.")
                    
                    # KPI 2: Top Favorecidos
                    print("\n[KPI 2] TOP 10 FAVORECIDOS:")
                    print("-" * 60)
                    result = kpi_top_favorecidos(conn, top_n=10)
                    for i, row in enumerate(result, 1):
                        print(f"{i:2}. {row[0][:45]:45} | R$ {row[1]:>15,.2f} | {row[2]:>7,} trans.")
                    
                    # KPI 3: Top 10 Orgaos
                    print("\n[KPI 3] TOP 10 ORGAOS COM MAIORES GASTOS:")
                    print("-" * 60)
                    result = kpi_top_orgaos(conn)
                    for i, row in enumerate(result, 1):
                        print(f"{i:2}. {row[0][:45]:45} | R$ {row[1]:>15,.2f} | {row[2]:>7,} trans.")
                    
                    # KPI 4: Evolucao Temporal
                    print("\n[KPI 4] EVOLUCAO TEMPORAL (ultimos 12 meses):")
                    print("-" * 60)
                    result = kpi_evolucao_temporal(conn)
                    for row in result[-12:]:
                        print(f"{row[0]:.0f}-{row[1]:02.0f} | R$ {row[2]:>15,.2f} | {row[3]:>7,} trans.")
                    
                    conn.close()
                    print("=" * 60)
                    
            except Exception as e:
                print(f"\nErro ao visualizar KPIs: {e}")
                import traceback
                traceback.print_exc()
            
            input("\nPressione Enter para continuar...")
            os.system('cls')

        elif opcao == "6":
            print("Saindo...")
            break
        else:
            print("Opcao invalida. Tente novamente.")
            time.sleep(1)

if __name__ == "__main__":
    main()