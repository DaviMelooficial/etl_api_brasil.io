import pandas as pd
from pathlib import Path
import json
import gzip
import os

def processamento_dados():
    """
    Exibe estatísticas dos dados já particionados na bronze
    """
    bronze_path = Path("dataset/bronze")
    
    if not bronze_path.exists():
        print("Pasta bronze não encontrada. Execute primeiro a ingestão de dados.")
        return
    
    # Buscar todas as partições
    particoes = list(bronze_path.glob("ano_mes=*/"))
    
    if not particoes:
        print("Nenhuma partição encontrada na bronze. Execute primeiro a ingestão.")
        return
    
    print(f"Encontradas {len(particoes)} particoes na bronze:")
    print("=" * 50)
    
    total_registros = 0
    
    for particao in sorted(particoes):
        ano_mes = particao.name.replace("ano_mes=", "")
        arquivos_parquet = list(particao.glob("*.parquet"))
        
        if arquivos_parquet:
            # Carregar dados da partição
            dfs_particao = []
            for arquivo in arquivos_parquet:
                df_part = pd.read_parquet(arquivo)
                dfs_particao.append(df_part)
            
            df_particao = pd.concat(dfs_particao, ignore_index=True) if dfs_particao else pd.DataFrame()
            
            if not df_particao.empty:
                print(f"{ano_mes}: {len(df_particao):,} registros")
                total_registros += len(df_particao)
                
                # Mostrar valor total se existir
                if 'valor' in df_particao.columns:
                    valor_total = df_particao['valor'].sum()
                    print(f"    Valor: R$ {valor_total:,.2f}")
    
    print("=" * 50)
    print(f"Total geral: {total_registros:,} registros")
    
    # Mostrar amostra dos dados se disponível
    if particoes:
        primeira_particao = sorted(particoes)[0]
        arquivos = list(primeira_particao.glob("*.parquet"))
        if arquivos:
            df_sample = pd.read_parquet(arquivos[0])
            print(f"\nColunas disponiveis: {list(df_sample.columns)}")
            print(f"\nAmostra dos dados:")
            print(df_sample.head())

def listar_arquivos_raw():
    """
    Lista todos os arquivos JSON (comprimidos) na pasta raw
    """
    raw_path = Path("dataset/raw")
    
    if not raw_path.exists():
        print("Pasta raw não encontrada.")
        return
    
    # Buscar arquivos .json.gz
    json_files = list(raw_path.glob("gastos_diretos_page_*.json.gz"))
    
    if not json_files:
        print("Nenhum arquivo JSON comprimido encontrado na pasta raw.")
        return
    
    print(f"Arquivos JSON comprimidos encontrados ({len(json_files)}):")
    print("=" * 60)
    
    total_size = 0
    total_records = 0
    
    for arquivo in sorted(json_files):
        try:
            # Ler arquivo comprimido
            with gzip.open(arquivo, 'rt', encoding='utf-8') as f:
                dados = json.load(f)
            
            num_records = len(dados.get('results', []))
            file_size = arquivo.stat().st_size
            total_size += file_size
            total_records += num_records
            
            print(f"{arquivo.name}: {num_records} registros ({file_size/1024:.1f} KB)")
            
        except Exception as e:
            print(f"{arquivo.name}: Erro ao ler arquivo ({e})")
    
    print("=" * 60)
    print(f"Total: {total_records:,} registros em {total_size/1024/1024:.1f} MB")

def limpar_dados_raw():
    """
    Remove arquivos JSON comprimidos da pasta raw
    """
    raw_path = Path("dataset/raw")
    if raw_path.exists():
        json_files = list(raw_path.glob("gastos_diretos_page_*.json.gz"))
        
        if json_files:
            print(f"Encontrados {len(json_files)} arquivos JSON comprimidos para remover...")
            resposta = input("Confirma a remocao? (s/n): ")
            if resposta.lower() == 's':
                for arquivo in json_files:
                    arquivo.unlink()
                print("Arquivos JSON removidos com sucesso!")
            else:
                print("Operacao cancelada.")
        else:
            print("Nenhum arquivo JSON comprimido encontrado na pasta raw.")
    else:
        print("Pasta raw nao existe.")

def listar_particoes():
    """
    Lista todas as partições disponíveis na bronze
    """
    bronze_path = Path("dataset/bronze")
    
    if not bronze_path.exists():
        print("Pasta bronze nao encontrada.")
        return
    
    particoes = list(bronze_path.glob("ano_mes=*/"))
    
    if not particoes:
        print("Nenhuma particao encontrada na bronze.")
        return
    
    print("Particoes disponiveis na bronze:")
    print("=" * 50)
    
    for particao in sorted(particoes):
        ano_mes = particao.name.replace("ano_mes=", "")
        arquivos = list(particao.glob("*.parquet"))
        total_arquivos = len(arquivos)
        
        # Contar registros
        total_registros = 0
        for arquivo in arquivos:
            df = pd.read_parquet(arquivo)
            total_registros += len(df)
        
        print(f"{ano_mes}: {total_registros:,} registros em {total_arquivos} arquivo(s)")