"""
ENDPOINT DE REQUISIÇÃO:
GET /api/v1/dataset/gastos-diretos/

head:
    Accept: application/json
    Content-Type: application/json
"""

import requests
import os
from dotenv import load_dotenv
import pandas as pd
import json
import time
import gzip
from pathlib import Path

load_dotenv()

def request_num_pages():
    url = "https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Token {os.getenv('API_KEY')}"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        dados = response.json()
        num_pages = dados['count']
        return num_pages

def processar_dados_streaming(dados, pagina):
    """Processa dados imediatamente após download e salva particionado"""
    if 'results' not in dados or not dados['results']:
        return
    
    # Normalizar os dados
    df = pd.json_normalize(dados['results'])
    
    # Verificar se tem as colunas necessárias
    if 'mes' not in df.columns or 'ano' not in df.columns:
        print(f"Página {pagina}: Dados sem mes/ano - salvando sem particionamento")
        return
    
    # Criar coluna mes_ano
    df["mes_ano"] = pd.to_datetime(df["ano"].astype(str) + "-" + df["mes"].astype(str), format="%Y-%m")
    df["ano_mes"] = df["ano"].astype(str) + "_" + df["mes"].astype(str).str.zfill(2)
    df["_pagina_origem"] = pagina
    
    # Agrupar por ano_mes e salvar particionado
    grupos = df.groupby('ano_mes')
    
    for ano_mes, grupo in grupos:
        # Criar diretório da partição
        partition_path = Path("dataset/bronze") / f"ano_mes={ano_mes}"
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Arquivo parquet da partição
        arquivo_parquet = partition_path / f"dados_{ano_mes}.parquet"
        
        # Remover coluna de particionamento antes de salvar
        grupo_clean = grupo.drop(columns=['ano_mes'])
        
        # Append se já existir
        if arquivo_parquet.exists():
            df_existente = pd.read_parquet(arquivo_parquet)
            df_combined = pd.concat([df_existente, grupo_clean], ignore_index=True)
            df_combined.to_parquet(arquivo_parquet, index=False)
        else:
            grupo_clean.to_parquet(arquivo_parquet, index=False)
        
        print(f"  -> Particao {ano_mes}: +{len(grupo_clean)} registros")

def salvar_json_comprimido(dados, pagina):
    """Salva JSON comprimido na pasta raw"""
    arquivo_gz = f"dataset/raw/gastos_diretos_page_{pagina}.json.gz"
    
    with gzip.open(arquivo_gz, 'wt', encoding='utf-8') as f:
        json.dump(dados, f, indent=2)
    
def salvar_checkpoint(pagina_atual, arquivo_checkpoint="dataset/raw/checkpoint.txt"):
    """Salva o progresso atual"""
    Path(arquivo_checkpoint).parent.mkdir(parents=True, exist_ok=True)
    with open(arquivo_checkpoint, 'w') as f:
        f.write(str(pagina_atual))

def carregar_checkpoint(arquivo_checkpoint="dataset/raw/checkpoint.txt"):
    """Carrega o progresso salvo"""
    try:
        with open(arquivo_checkpoint, 'r') as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return 2  # Página inicial padrão

def ingestão_gastos_diretos(num_pages):
    """
    Ingere dados da API com processamento streaming e JSONs comprimidos
    Args:
        num_pages: número total de páginas
    """
    checkpoint_path = "dataset/raw/checkpoint.txt"
    
    # Criar diretórios se não existirem
    Path("dataset/raw").mkdir(parents=True, exist_ok=True)
    Path("dataset/bronze").mkdir(parents=True, exist_ok=True)
    
    # Carregar checkpoint
    init = carregar_checkpoint(checkpoint_path)
    print(f"Iniciando da página {init}")
    print("Processamento streaming ativo: dados serão processados conforme chegam\n")
    
    total_processados = 0
    
    for i in range(init, num_pages + 1):
        while True:
            try:
                url = f"https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data?page={i}"
                headers = {
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "Authorization": f"Token {os.getenv('API_KEY')}"
                }
                response = requests.get(url, headers=headers)

                if response.status_code == 200:
                    dados = response.json()
                    
                    # 1. Salvar JSON comprimido na raw
                    arquivo_gz = salvar_json_comprimido(dados, i)
                    print(f"Página {i}: JSON comprimido salvo")
                    
                    # 2. Processar dados imediatamente (streaming)
                    if dados.get('results'):
                        processar_dados_streaming(dados, i)
                        total_processados += len(dados['results'])
                    
                    # 3. Salvar checkpoint
                    salvar_checkpoint(i + 1, checkpoint_path)
                    
                    print(f"Página {i}: Concluída (Total processados: {total_processados:,})\n")
                    
                    time.sleep(2)  # Tempo entre requisições
                    break  # Sai do while e vai para a próxima página
                else:
                    raise Exception(f"Falha na requisição: {response.status_code}")
            except Exception as e:
                print(f"Erro ao processar a página {i}: {e}")
                print("Aguardando 10 segundos antes de tentar novamente...")
                time.sleep(10)
    
    # Remover checkpoint ao finalizar
    if Path(checkpoint_path).exists():
        os.remove(checkpoint_path)
    
    print("=" * 60)
    print("Ingestao concluida com sucesso!")
    print(f"Total de registros processados: {total_processados:,}")
    print(f"JSONs comprimidos salvos em: dataset/raw/")
    print(f"Dados particionados por ano_mes em: dataset/bronze/")
    print("=" * 60)