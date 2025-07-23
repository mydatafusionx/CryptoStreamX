# Databricks notebook source
# DBTITLE 1,Importações
import os
import sys
import time
from datetime import datetime

# Importações do PySpark
from pyspark.sql.functions import (
    current_timestamp, 
    lit, 
    col, 
    when, 
    count, 
    countDistinct, 
    min, 
    max, 
    avg
)
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    TimestampType, 
    DoubleType, 
    LongType, 
    MapType
)

# Adiciona o diretório raiz ao path para importar módulos
sys.path.append('/Workspace/Repos/DataFusionX/CryptoStreamX/src')

# Importações personalizadas
from utils.api_client import CoinGeckoClient
from utils.db_utils import DeltaTableManager
from utils.config import config

# COMMAND ----------

# DBTITLE 1,Configuração Inicial
# Inicializa o cliente da API
coingecko = CoinGeckoClient()

# Inicializa o gerenciador de tabelas Delta
db_manager = DeltaTableManager(spark, config.catalog_name, config.bronze_schema)

# Define o nome da tabela
table_name = "coingecko_raw"

# COMMAND ----------

# DBTITLE 1,Função para buscar dados da API
def fetch_coingecko_data():
    """Busca dados de criptomoedas da API CoinGecko."""
    try:
        print("Buscando dados da API CoinGecko...")
        data = coingecko.get_market_data(
            vs_currency='usd',
            order='market_cap_desc',
            per_page=100,
            page=1,
            sparkline=False,
            price_change_percentage='24h,7d'
        )
        print(f"Total de registros recebidos: {len(data)}")
        return data
    except Exception as e:
        print(f"Erro ao buscar dados da API: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Função para processar e salvar os dados
def process_and_save_data(data):
    """
    Processa os dados da API CoinGecko e salva na camada Bronze.
    
    Args:
        data: Lista de dicionários contendo os dados das criptomoedas
        
    Returns:
        dict: Dicionário com métricas da execução e status
        
    Raises:
        ValueError: Se os dados estiverem vazios ou inválidos
        Exception: Para outros erros durante o processamento
    """
    metrics = {
        'start_time': None,
        'end_time': None,
        'duration_seconds': None,
        'records_processed': 0,
        'records_saved': 0,
        'success': False,
        'error': None,
        'columns_processed': 0,
        'missing_columns': []
    }
    
    try:
        metrics['start_time'] = current_timestamp()
        
        # Validação dos dados de entrada
        if not data or not isinstance(data, (list, dict)):
            raise ValueError("Dados de entrada inválidos. Espera-se uma lista de dicionários.")
            
        if isinstance(data, dict):  # Se for um único registro, converte para lista
            data = [data]
            
        if len(data) == 0:
            print("Aviso: Nenhum dado para processar.")
            metrics['end_time'] = current_timestamp()
            metrics['success'] = True
            return metrics
        
        # Converte para DataFrame do Spark
        df = spark.createDataFrame(data)
        metrics['records_processed'] = len(data)
        
        # Valida colunas obrigatórias
        required_columns = ["id", "symbol", "name", "current_price", "market_cap"]
        missing_required = [col for col in required_columns if col not in df.columns]
        
        if missing_required:
            raise ValueError(f"Colunas obrigatórias ausentes: {', '.join(missing_required)}")
        
        # Adiciona metadados de ingestão
        run_id = dbutils.widgets.get("run_id") if dbutils.widgets.get("run_id") else "manual_run_" + str(int(time.time()))
        
        df = (df
              .withColumn("ingestion_timestamp", current_timestamp())
              .withColumn("pipeline_run_id", lit(run_id))
             )
        
        # Define o esquema esperado
        expected_columns = [
            # Identificação
            "id", "symbol", "name", "image", 
            # Preços e capitalização
            "current_price", "market_cap", "market_cap_rank", 
            "fully_diluted_valuation", "total_volume", 
            # Variações de preço
            "high_24h", "low_24h", "price_change_24h", 
            "price_change_percentage_24h", "market_cap_change_24h", 
            "market_cap_change_percentage_24h",
            # Fornecimento
            "circulating_supply", "total_supply", "max_supply",
            # Máximos e mínimos históricos
            "ath", "ath_change_percentage", "ath_date",
            "atl", "atl_change_percentage", "atl_date",
            # Outros
            "roi", "last_updated",
            # Variações percentuais
            "price_change_percentage_24h_in_currency",
            "price_change_percentage_7d_in_currency",
            # Metadados
            "ingestion_timestamp", "pipeline_run_id"
        ]
        
        # Registra colunas ausentes
        metrics['missing_columns'] = [col for col in expected_columns if col not in df.columns]
        
        # Garante que todas as colunas esperadas existam
        for col_name in expected_columns:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(None))
        
        # Ordena as colunas conforme esperado
        df = df.select(expected_columns)
        metrics['columns_processed'] = len(expected_columns)
        
        # Validação de dados
        initial_count = df.count()
        if initial_count == 0:
            raise ValueError("Nenhum registro válido para processar após a validação.")
        
        # Remove duplicados baseado no ID
        df = df.dropDuplicates(["id"])
        metrics['duplicates_removed'] = initial_count - df.count()
        
        # Escreve os dados na tabela Delta
        db_manager.write_dataframe(
            df=df,
            table_name=table_name,
            mode="append",
            merge_schema=True,
            partition_columns=["pipeline_run_id"]
        )
        
        # Atualiza métricas
        metrics['end_time'] = current_timestamp()
        metrics['records_saved'] = df.count()
        metrics['success'] = True
        
        # Registra métricas
        duration = (metrics['end_time'].cast("long") - metrics['start_time'].cast("long")).cast("double") / 1000
        metrics['duration_seconds'] = round(duration, 2)
        
        # Log de sucesso
        print(f"""
        ====== RESUMO DA INGESTÃO ======
        • Tabela: {config.catalog_name}.{config.bronze_schema}.{table_name}
        • Registros processados: {metrics['records_processed']}
        • Registros salvos: {metrics['records_saved']}
        • Colunas processadas: {metrics['columns_processed']}
        • Colunas ausentes: {len(metrics['missing_columns'])}
        • Duração: {metrics['duration_seconds']} segundos
        • Pipeline Run ID: {run_id}
        ================================
        """)
        
        if metrics['missing_columns']:
            print(f"Aviso: Foram encontradas {len(metrics['missing_columns'])} colunas ausentes que foram preenchidas com NULL.")
        
        return metrics
        
    except Exception as e:
        metrics['end_time'] = current_timestamp()
        metrics['success'] = False
        metrics['error'] = str(e)
        
        # Calcula a duração mesmo em caso de erro
        if metrics['start_time'] and metrics['end_time']:
            duration = (metrics['end_time'].cast("long") - metrics['start_time'].cast("long")).cast("double") / 1000
            metrics['duration_seconds'] = round(duration, 2)
        
        # Log de erro detalhado
        error_msg = f"""
        !!! ERRO NO PROCESSAMENTO !!!
        • Mensagem: {str(e)}
        • Registros processados: {metrics.get('records_processed', 0)}
        • Duração: {metrics.get('duration_seconds', 0)} segundos
        • Pipeline Run ID: {run_id if 'run_id' in locals() else 'N/A'}
        """
        print(error_msg)
        
        # Log adicional para debug
        import traceback
        print("\nStack Trace:")
        print(traceback.format_exc())
        
        # Propaga o erro para tratamento externo
        raise

# COMMAND ----------

# DBTITLE 1,Execução do Pipeline
def main():
    """
    Função principal de execução do pipeline de ingestão.
    
    Returns:
        dict: Dicionário com métricas e status da execução
    """
    execution_metrics = {
        'pipeline_start_time': current_timestamp(),
        'pipeline_end_time': None,
        'pipeline_duration_seconds': None,
        'pipeline_status': 'FAILED',
        'api_status': None,
        'processing_status': None,
        'total_records_processed': 0,
        'error': None,
        'run_id': dbutils.widgets.get("run_id") if dbutils.widgets.get("run_id") else f"manual_run_{int(time.time())}"
    }
    
    try:
        print(f"\n{'='*50}")
        print(f"INICIANDO PIPELINE DE INGESTÃO - {execution_metrics['run_id']}")
        print(f"{'='*50}\n")
        
        # 1. Busca dados da API
        print("1. Buscando dados da API CoinGecko...")
        try:
            data = fetch_coingecko_data()
            execution_metrics['api_status'] = 'SUCCESS'
            
            if not data:
                raise ValueError("A API não retornou nenhum dado.")
                
            execution_metrics['total_records_processed'] = len(data)
            print(f"   ✓ Dados recebidos: {len(data)} registros")
            
        except Exception as e:
            execution_metrics['api_status'] = 'FAILED'
            execution_metrics['error'] = f"Erro na API: {str(e)}"
            raise
        
        # 2. Processa e salva os dados
        print("\n2. Processando e salvando dados...")
        try:
            processing_metrics = process_and_save_data(data)
            execution_metrics['processing_status'] = 'SUCCESS'
            execution_metrics.update(processing_metrics)
            
            # Atualiza métricas gerais
            execution_metrics['pipeline_status'] = 'SUCCESS'
            execution_metrics['total_records_processed'] = processing_metrics.get('records_processed', 0)
            
            print("   ✓ Dados processados e salvos com sucesso!")
            
        except Exception as e:
            execution_metrics['processing_status'] = 'FAILED'
            execution_metrics['error'] = f"Erro no processamento: {str(e)}"
            raise
            
    except Exception as e:
        # Registra o erro nas métricas
        if not execution_metrics.get('error'):
            execution_metrics['error'] = f"Erro no pipeline: {str(e)}"
        
        # Log do erro
        print(f"\n{'!'*50}")
        print(f"ERRO NO PIPELINE: {str(e)}")
        print(f"{'!'*50}\n")
        
        # Propaga o erro para o Databricks (opcional)
        if dbutils and hasattr(dbutils.notebook, 'exit'):
            dbutils.notebook.exit({"status": "FAILED", "error": str(e)})
        
        raise
        
    finally:
        # Finaliza as métricas
        execution_metrics['pipeline_end_time'] = current_timestamp()
        duration = (execution_metrics['pipeline_end_time'].cast("long") - 
                   execution_metrics['pipeline_start_time'].cast("long")).cast("double") / 1000
        execution_metrics['pipeline_duration_seconds'] = round(duration, 2)
        
        # Log do resumo da execução
        print(f"\n{'='*50}")
        print("RESUMO DA EXECUÇÃO".center(50))
        print(f"{'='*50}")
        print(f"• Status: {'✅ SUCESSO' if execution_metrics['pipeline_status'] == 'SUCCESS' else '❌ FALHA'}")
        print(f"• Início: {execution_metrics['pipeline_start_time']}")
        print(f"• Término: {execution_metrics['pipeline_end_time']}")
        print(f"• Duração: {execution_metrics['pipeline_duration_seconds']} segundos")
        print(f"• Registros processados: {execution_metrics.get('total_records_processed', 0)}")
        
        if execution_metrics.get('error'):
            print(f"• Erro: {execution_metrics['error']}")
            
        print(f"{'='*50}\n")
        
        # Salva as métricas de execução (opcional)
        try:
            # Aqui você pode adicionar código para salvar as métricas em uma tabela Delta
            # ou enviar para um sistema de monitoramento
            pass
        except Exception as e:
            print(f"Aviso: Não foi possível salvar as métricas de execução: {str(e)}")
        
        # Retorna as métricas para uso em outros notebooks ou orquestradores
        return execution_metrics

# Executa o pipeline
if __name__ == "__main__":
    main()

# COMMAND ----------

# DBTITLE 1,Funções de Monitoramento
def save_execution_metrics(metrics):
    """
    Salva as métricas de execução em uma tabela Delta para monitoramento.
    
    Args:
        metrics (dict): Dicionário contendo as métricas da execução
    """
    try:
        # Define o esquema da tabela de métricas
        metrics_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("pipeline_name", StringType(), True),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), False),
            StructField("duration_seconds", DoubleType(), True),
            StructField("status", StringType(), False),
            StructField("records_processed", LongType(), True),
            StructField("records_saved", LongType(), True),
            StructField("api_status", StringType(), True),
            StructField("processing_status", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])
        
        # Prepara os dados para salvar
        metrics_data = [(
            metrics.get('run_id', 'unknown'),
            'ingest_coingecko',
            metrics.get('pipeline_start_time'),
            metrics.get('pipeline_end_time'),
            metrics.get('pipeline_duration_seconds'),
            metrics.get('pipeline_status', 'UNKNOWN'),
            metrics.get('total_records_processed', 0),
            metrics.get('records_saved', 0),
            metrics.get('api_status'),
            metrics.get('processing_status'),
            metrics.get('error'),
            {
                'missing_columns': str(metrics.get('missing_columns', [])),
                'duplicates_removed': str(metrics.get('duplicates_removed', 0))
            }
        )]
        
        # Cria o DataFrame com as métricas
        metrics_df = spark.createDataFrame(metrics_data, schema=metrics_schema)
        
        # Define o caminho da tabela de métricas
        metrics_table = f"{config.catalog_name}.monitoring.pipeline_executions"
        
        # Verifica se a tabela existe, se não, cria
        if not spark._jsparkSession.catalog().tableExists(metrics_table.split('.')[0], 
                                                         f"{metrics_table.split('.')[1]}.{metrics_table.split('.')[2]}"):
            (metrics_df.write
             .format("delta")
             .mode("overwrite")
             .saveAsTable(metrics_table))
        else:
            # Se a tabela já existe, faz append dos dados
            (metrics_df.write
             .format("delta")
             .mode("append")
             .saveAsTable(metrics_table))
        
        print(f"✓ Métricas salvas em {metrics_table}")
        
    except Exception as e:
        print(f"⚠️ Não foi possível salvar as métricas: {str(e)}")

def get_pipeline_status(run_id):
    """
    Consulta o status de uma execução do pipeline pelo run_id.
    
    Args:
        run_id (str): ID da execução a ser consultada
        
    Returns:
        dict: Dicionário com os dados da execução ou None se não encontrado
    """
    try:
        metrics_table = f"{config.catalog_name}.monitoring.pipeline_executions"
        if spark._jsparkSession.catalog().tableExists(metrics_table.split('.')[0], 
                                                     f"{metrics_table.split('.')[1]}.{metrics_table.split('.')[2]}"):
            df = spark.sql(f"""
                SELECT * FROM {metrics_table}
                WHERE run_id = '{run_id}'
                ORDER BY end_time DESC
                LIMIT 1
            """)
            
            if df.count() > 0:
                return df.collect()[0].asDict()
        return None
    except Exception as e:
        print(f"Erro ao consultar status do pipeline: {str(e)}")
        return None

# COMMAND ----------

# DBTITLE 1,Verificação dos Dados
# Verifica se os dados foram salvos corretamente
try:
    df = spark.table(f"{config.catalog_name}.{config.bronze_schema}.{table_name}")
    print(f"Total de registros na tabela: {df.count():,}")
    
    # Mostra os dados mais recentes
    print("\nÚltimos 5 registros inseridos:")
    display(df.orderBy(col("ingestion_timestamp").desc()).limit(5))
    
    # Estatísticas básicas
    if df.count() > 0:
        print("\nEstatísticas básicas:")
        df_stats = df.select(
            count("*").alias("total_records"),
            countDistinct("id").alias("unique_coins"),
            min("current_price").alias("min_price"),
            max("current_price").alias("max_price"),
            avg("current_price").alias("avg_price"),
            min("market_cap").alias("min_market_cap"),
            max("market_cap").alias("max_market_cap")
        )
        display(df_stats)
    
except Exception as e:
    print(f"Erro ao verificar os dados: {str(e)}")
    
# COMMAND ----------

# DBTITLE 1,Execução do Notebook
# Esta célula executa o pipeline quando o notebook é executado diretamente
if __name__ == "__main__":
    # Executa o pipeline
    metrics = main()
    
    # Salva as métricas de execução
    if metrics:
        save_execution_metrics(metrics)
        
        # Se houver erro, encerra com falha
        if metrics.get('pipeline_status') != 'SUCCESS':
            dbutils.notebook.exit({
                "status": "FAILED",
                "error": metrics.get('error', 'Erro desconhecido'),
                "run_id": metrics.get('run_id')
            })
