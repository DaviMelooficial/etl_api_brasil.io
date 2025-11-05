from services.request import ingestão_gastos_diretos, request_num_pages
from services.processador import processamento_dados, limpar_dados_raw, listar_arquivos_raw, listar_particoes
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
        print("5. Sair")
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
            print("Saindo...")
            break
        else:
            print("Opcao invalida. Tente novamente.")
            time.sleep(1)

if __name__ == "__main__":
    main()