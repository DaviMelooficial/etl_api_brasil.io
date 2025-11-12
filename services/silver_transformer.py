"""
Módulo de transformação Bronze -> Silver
Realiza limpeza, validação e padronização dos dados
"""

import pandas as pd
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def ler_dados_bronze():
    """
    Lê todos os arquivos parquet da camada bronze
    Returns:
        DataFrame consolidado com todos os dados
    """
    bronze_path = Path("dataset/bronze")
    
    if not bronze_path.exists():
        raise FileNotFoundError("Pasta bronze não encontrada. Execute primeiro a ingestão de dados.")
    
    # Buscar todas as partições
    particoes = list(bronze_path.glob("ano_mes=*/"))
    
    if not particoes:
        raise FileNotFoundError("Nenhuma partição encontrada na bronze.")
    
    logger.info(f"Encontradas {len(particoes)} partições na bronze")
    
    # Ler todos os arquivos parquet
    dfs = []
    for particao in sorted(particoes):
        arquivos = list(particao.glob("*.parquet"))
        for arquivo in arquivos:
            df = pd.read_parquet(arquivo)
            dfs.append(df)
    
    df_bronze = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total de registros lidos: {len(df_bronze):,}")
    
    return df_bronze


def transformar_dados(df):
    """
    Aplica transformações e limpeza nos dados
    Args:
        df: DataFrame com dados da bronze
    Returns:
        DataFrame transformado
    """
    df_silver = df.copy()
    
    logger.info("Iniciando transformações...")
    
    # 1. Remover duplicatas completas
    registros_antes = len(df_silver)
    df_silver = df_silver.drop_duplicates()
    duplicatas_removidas = registros_antes - len(df_silver)
    if duplicatas_removidas > 0:
        logger.info(f"Removidas {duplicatas_removidas:,} duplicatas")
    
    # 2. Converter tipos de dados
    # Colunas numéricas
    colunas_numericas = ['valor', 'ano', 'mes']
    for col in colunas_numericas:
        if col in df_silver.columns:
            df_silver[col] = pd.to_numeric(df_silver[col], errors='coerce')
    
    # Converter data mes_ano
    if 'mes_ano' in df_silver.columns:
        df_silver['mes_ano'] = pd.to_datetime(df_silver['mes_ano'], errors='coerce')
    
    # 3. Padronizar strings (uppercase e trim)
    colunas_texto = ['orgao', 'favorecido', 'uf_favorecido', 'unidade_gestora', 'tipo_favorecido']
    for col in colunas_texto:
        if col in df_silver.columns:
            df_silver[col] = df_silver[col].astype(str).str.strip().str.upper()
            # Substituir 'NAN' e 'NONE' por valores nulos reais
            df_silver[col] = df_silver[col].replace(['NAN', 'NONE', ''], pd.NA)
    
    # 4. Validar e limpar dados
    # Remover registros com valores negativos ou zero
    if 'valor' in df_silver.columns:
        registros_antes = len(df_silver)
        df_silver = df_silver[df_silver['valor'] > 0]
        removidos = registros_antes - len(df_silver)
        if removidos > 0:
            logger.info(f"Removidos {removidos:,} registros com valor <= 0")
    
    # 5. Criar coluna ano_mes padronizada (se não existir)
    if 'ano_mes' not in df_silver.columns and 'ano' in df_silver.columns and 'mes' in df_silver.columns:
        df_silver['ano_mes'] = (
            df_silver['ano'].astype(str) + '_' + 
            df_silver['mes'].astype(str).str.zfill(2)
        )
    
    logger.info("Transformações concluídas")
    
    return df_silver


def validar_qualidade(df):
    """
    Valida a qualidade dos dados transformados
    Args:
        df: DataFrame transformado
    Returns:
        dict com resultados da validação
    """
    logger.info("Iniciando validação de qualidade...")
    
    validacao = {
        'total_registros': len(df),
        'colunas_criticas': {},
        'valores_nulos': {},
        'valores_invalidos': {},
        'status': 'OK'
    }
    
    # Colunas críticas que não devem ter nulos
    colunas_criticas = ['valor', 'ano', 'mes', 'orgao', 'favorecido']
    
    for col in colunas_criticas:
        if col in df.columns:
            nulos = df[col].isna().sum()
            validacao['colunas_criticas'][col] = {
                'nulos': int(nulos),
                'percentual': round(nulos / len(df) * 100, 2)
            }
            
            # Marcar como falha se mais de 5% de nulos em colunas críticas
            if nulos / len(df) > 0.05:
                validacao['status'] = 'ALERTA'
                logger.warning(f"Coluna '{col}' tem {nulos:,} nulos ({validacao['colunas_criticas'][col]['percentual']}%)")
    
    # Verificar valores inválidos
    if 'valor' in df.columns:
        valores_negativos = (df['valor'] <= 0).sum()
        validacao['valores_invalidos']['valor_negativo_ou_zero'] = int(valores_negativos)
    
    if 'ano' in df.columns:
        anos_invalidos = ((df['ano'] < 2000) | (df['ano'] > 2025)).sum()
        validacao['valores_invalidos']['ano_fora_intervalo'] = int(anos_invalidos)
    
    if 'mes' in df.columns:
        meses_invalidos = ((df['mes'] < 1) | (df['mes'] > 12)).sum()
        validacao['valores_invalidos']['mes_invalido'] = int(meses_invalidos)
    
    # Estatísticas gerais de nulos por coluna
    for col in df.columns:
        nulos = df[col].isna().sum()
        if nulos > 0:
            validacao['valores_nulos'][col] = {
                'nulos': int(nulos),
                'percentual': round(nulos / len(df) * 100, 2)
            }
    
    logger.info(f"Validação concluída - Status: {validacao['status']}")
    
    return validacao


def salvar_silver(df):
    """
    Salva dados transformados na camada silver mantendo particionamento
    Args:
        df: DataFrame transformado
    """
    silver_path = Path("dataset/silver")
    silver_path.mkdir(parents=True, exist_ok=True)
    
    logger.info("Salvando dados na camada silver...")
    
    # Verificar se tem coluna de particionamento
    if 'ano_mes' not in df.columns:
        logger.warning("Coluna ano_mes não encontrada. Salvando sem particionamento.")
        df.to_parquet(silver_path / "dados_silver.parquet", index=False)
        return
    
    # Agrupar por ano_mes e salvar particionado
    grupos = df.groupby('ano_mes')
    
    total_particoes = 0
    for ano_mes, grupo in grupos:
        # Criar diretório da partição
        partition_path = silver_path / f"ano_mes={ano_mes}"
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Remover coluna de particionamento antes de salvar
        grupo_clean = grupo.drop(columns=['ano_mes'])
        
        # Arquivo parquet da partição
        arquivo_parquet = partition_path / f"dados_silver.parquet"
        grupo_clean.to_parquet(arquivo_parquet, index=False)
        
        total_particoes += 1
        logger.info(f"  -> Partição {ano_mes}: {len(grupo_clean):,} registros salvos")
    
    logger.info(f"Total de {total_particoes} partições salvas na silver")


def executar_pipeline():
    """
    Executa o pipeline completo Bronze -> Silver
    Returns:
        tuple (DataFrame, validação)
    """
    try:
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE BRONZE -> SILVER")
        logger.info("=" * 60)
        
        # 1. Ler dados da bronze
        df_bronze = ler_dados_bronze()
        
        # 2. Transformar dados
        df_silver = transformar_dados(df_bronze)
        
        # 3. Validar qualidade
        validacao = validar_qualidade(df_silver)
        
        # 4. Salvar na silver
        salvar_silver(df_silver)
        
        logger.info("=" * 60)
        logger.info("PIPELINE CONCLUÍDO COM SUCESSO")
        logger.info("=" * 60)
        
        return df_silver, validacao
        
    except Exception as e:
        logger.error(f"Erro no pipeline: {e}")
        raise