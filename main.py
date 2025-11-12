from services.request import ingestão_gastos_diretos, request_num_pages
from services.auxilar import processamento_dados, limpar_dados_raw, listar_arquivos_raw, listar_particoes
from services.silver_transformer import executar_pipeline
import os
import time

def main():

    while True:
        print("Sistema de Ingestao de Gastos Diretos")
        print("-" * 40)
        print("1. Ingestao Streaming")
        print("2. Listar Arquivos Raw")
        print("3. Listar Particoes Bronze")
        print("4. Limpar Arquivos Raw")
        print("5. Processar Bronze -> Silver")
        print("6. Visualizar Dados Silver")
        print("7. Sair")
        print("-" * 40)

        opcao = input("Escolha uma opcao: ")

        if opcao == "1":
            try:
                num_pages = request_num_pages()
                print(f"Total de paginas encontradas: {num_pages}")
                
                ingestão_gastos_diretos(num_pages)
            except Exception as e:
                print(f"Erro durante a ingestao: {e}")
            input("\nPressione Enter para continuar...")
            os.system('cls')

        elif opcao == "2":
            listar_arquivos_raw()
            input("\nPressione Enter para continuar...")
            os.system('cls')

        elif opcao == "3":
            listar_particoes()
            input("\nPressione Enter para continuar...")
            os.system('cls')

        elif opcao == "4":
            limpar_dados_raw()
            input("\nPressione Enter para continuar...")
            os.system('cls')

        elif opcao == "5":
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
                    print("Execute primeiro a opcao 5 (Processar Bronze -> Silver)")
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

        elif opcao == "7":
            print("Saindo...")
            break
        else:
            print("Opcao invalida. Tente novamente.")
            time.sleep(1)

if __name__ == "__main__":
    main()