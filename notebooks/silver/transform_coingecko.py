# Databricks notebook source
# DBTITLE 1,Importações
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_timestamp, coalesce, current_timestamp, sum as _sum
from pyspark.sql.types import DoubleType, TimestampType, StructType, StructField, StringType, LongType, DecimalType
import sys
import os
import time
from datetime import datetime
import json
import logging
from functools import wraps

# Configura o logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Decorador para retry com backoff
def retry_with_backoff(retries=3, backoff_in_seconds=1):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            retry_count = 0
            while retry_count < retries:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    retry_count += 1
                    if retry_count == retries:
                        logger.error(f"Operação falhou após {retries} tentativas: {str(e)}")
                        raise
                    wait_time = backoff_in_seconds * (2 ** (retry_count - 1))
                    logger.warning(f"Tentativa {retry_count}/{retries} - aguardando {wait_time}s antes de tentar novamente...")
                    time.sleep(wait_time)
        return wrapper
    return decorator

# Configurações
class Config:
    def __init__(self):
        # Nomes dos schemas e tabelas
        self.bronze_schema = 'bronze'
        self.silver_schema = 'silver'
        self.bronze_table = 'coingecko_raw'
        self.silver_table = 'coingecko_enriched'
        
        # Configurações adicionais
        self.silver_table_path = f"{self.silver_schema}.{self.silver_table}"
        self.bronze_table_path = f"{self.bronze_schema}.{self.bronze_table}"

# Inicializa a configuração
config = Config()

# Classe para gerenciar tabelas Delta
class DeltaTableManager:
    def __init__(self, spark, schema_name=None):
        self.spark = spark
        self.schema_name = schema_name
        self.max_retries = 3
        self.retry_delay = 2  # segundos
    
    @retry_with_backoff(retries=3, backoff_in_seconds=2)
    def create_schema_if_not_exists(self):
        """Cria o schema se não existir com tratamento de erros e retry."""
        if not self.schema_name:
            return self
            
        try:
            logger.info(f"Verificando/Criando schema: {self.schema_name}")
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
            logger.info(f"Schema {self.schema_name} verificado/criado com sucesso")
            return self
        except Exception as e:
            logger.error(f"Falha ao criar schema {self.schema_name}: {str(e)}")
            raise
    
    @retry_with_backoff(retries=3, backoff_in_seconds=2)
    def table_exists(self, table_path):
        """Verifica se a tabela existe com tratamento de erros e retry."""
        try:
            logger.debug(f"Verificando existência da tabela: {table_path}")
            df = self.spark.sql(f"SHOW TABLES IN {table_path.split('.')[0]} LIKE '{table_path.split('.')[-1]}'")
            exists = df.count() > 0
            logger.debug(f"Tabela {table_path} existe: {exists}")
            return exists
        except Exception as e:
            logger.error(f"Erro ao verificar existência da tabela {table_path}: {str(e)}")
            return False
    
    @retry_with_backoff(retries=3, backoff_in_seconds=2)
    def create_table_from_dataframe(self, df, table_path, partition_cols=None):
        """Cria uma nova tabela Delta a partir de um DataFrame com tratamento de erros e retry."""
        try:
            logger.info(f"Criando nova tabela {table_path}...")
            writer = df.write.format("delta")
            
            if partition_cols:
                writer = writer.partitionBy(partition_cols)
                
            writer.mode("overwrite").saveAsTable(table_path)
            logger.info(f"Tabela {table_path} criada com sucesso!")
            
            # Otimiza a tabela após criação
            self.optimize_table(table_path)
            return True
        except Exception as e:
            logger.error(f"Falha ao criar tabela {table_path}: {str(e)}")
            raise
    
    @retry_with_backoff(retries=3, backoff_in_seconds=2)
    def optimize_table(self, table_path):
        """Otimiza a tabela Delta com tratamento de erros e retry."""
        try:
            logger.info(f"Otimizando tabela {table_path}...")
            self.spark.sql(f"OPTIMIZE {table_path} ZORDER BY (id)")
            logger.info(f"Tabela {table_path} otimizada com sucesso")
            return True
        except Exception as e:
            logger.error(f"Falha ao otimizar tabela {table_path}: {str(e)}")
            raise

# Inicializa o gerenciador de tabelas Delta com tratamento de erros
try:
    logger.info("Inicializando o gerenciador de tabelas Delta...")
    db_manager = DeltaTableManager(spark=spark, schema_name=config.silver_schema)
    db_manager.create_schema_if_not_exists()
    logger.info(f"Conexão com o banco de dados inicializada: {config.silver_schema}")
    
    # Verifica se as tabelas existem
    bronze_exists = db_manager.table_exists(config.bronze_table_path)
    logger.info(f"Tabela Bronze existe: {bronze_exists}")
    
    logger.info(f"Tabela Bronze configurada: {config.bronze_table_path}")
    logger.info(f"Tabela Silver configurada: {config.silver_table_path}")
    
except Exception as e:
    logger.error(f"Falha crítica ao inicializar o gerenciador de tabelas Delta: {str(e)}")
    logger.exception("Detalhes do erro:")
    raise

# COMMAND ----------

# DBTITLE 1,Função para ler dados da camada Bronze
def read_bronze_data():
    """Lê os dados mais recentes da camada Bronze."""
    try:
        logger.info(f"Lendo dados da tabela {config.bronze_table_path}...")
        
        # Verifica se a tabela bronze existe
        if not db_manager.table_exists(config.bronze_table_path):
            logger.error(f"Tabela {config.bronze_table_path} não encontrada!")
            return None
        
        # Primeiro, verifica as colunas disponíveis na tabela
        table_info = spark.sql(f"DESCRIBE TABLE EXTENDED {config.bronze_table_path}")
        columns = [row.col_name.lower() for row in table_info.select("col_name").collect()]
        logger.info(f"Colunas disponíveis na tabela bronze: {', '.join(columns)}")
        
        # Verifica se a coluna de timestamp existe (pode ter nomes diferentes)
        timestamp_col = None
        for col_name in ['ingestion_timestamp', 'timestamp', 'created_at', 'last_updated']:
            if col_name in columns:
                timestamp_col = col_name
                break
                
        if not timestamp_col:
            logger.warning("Nenhuma coluna de timestamp encontrada. Usando a última execução sem ordenação por tempo.")
            latest_run = spark.sql(f"""
                SELECT DISTINCT pipeline_run_id 
                FROM {config.bronze_table_path}
                LIMIT 1
            """).collect()
        else:
            logger.info(f"Usando a coluna '{timestamp_col}' para ordenação temporal")
            # Primeiro, obtém o timestamp mais recente
            latest_timestamp = spark.sql(f"""
                SELECT MAX(`{timestamp_col}`) as max_timestamp
                FROM {config.bronze_table_path}
            """).collect()[0].max_timestamp
            
            # Depois, busca o pipeline_run_id correspondente
            if latest_timestamp:
                latest_run = spark.sql(f"""
                    SELECT DISTINCT pipeline_run_id 
                    FROM {config.bronze_table_path}
                    WHERE `{timestamp_col}` = "{latest_timestamp}"
                    LIMIT 1
                """).collect()
            else:
                # Fallback se não encontrar timestamp
                latest_run = spark.sql(f"""
                    SELECT DISTINCT pipeline_run_id 
                    FROM {config.bronze_table_path}
                    LIMIT 1
                """).collect()
        
        if not latest_run:
            logger.warning("Nenhum dado encontrado na camada Bronze.")
            return None
            
        run_id = latest_run[0].pipeline_run_id
        logger.info(f"Processando execução: {run_id}")
        
        # Lê os dados da última execução
        df = spark.table(config.bronze_table_path) \
                 .filter(col("pipeline_run_id") == run_id)
        
        logger.info(f"Total de registros a serem processados: {df.count()}")
        logger.info(f"Schema do DataFrame: {df.schema}")
        return df
        
    except Exception as e:
        logger.error(f"Erro ao ler dados da camada Bronze: {str(e)}", exc_info=True)
        return None

# COMMAND ----------

# DBTITLE 1,Função para validar dados
def validate_data(df):
    """
    Aplica validações nos dados da camada Bronze.
    
    Args:
        df: DataFrame contendo os dados a serem validados
        
    Returns:
        DataFrame: DataFrame com os dados validados e transformações aplicadas
        
    Raises:
        ValueError: Se os dados não atenderem aos critérios de validação essenciais
    """
    try:
        logger.info("Iniciando validação dos dados...")
        
        # Verifica se o DataFrame está vazio
        if df is None or df.rdd.isEmpty():
            logger.warning("Nenhum dado para validar. DataFrame vazio ou nulo.")
            return df
            
        # Contagem inicial de registros
        initial_count = df.count()
        logger.info(f"Total de registros para validação: {initial_count}")
        
        # Define regras de validação
        validation_rules = {
            # Colunas obrigatórias (não podem ser nulas)
            "id": {"rule": "is_not_null", "severity": "error"},
            "symbol": {"rule": "is_not_null", "severity": "error"},
            "name": {"rule": "is_not_null", "severity": "error"},
            
            # Colunas numéricas que devem ser positivas ou nulas
            "current_price": {"rule": "is_positive_or_null", "severity": "warning"},
            "market_cap": {"rule": "is_positive_or_null", "severity": "warning"},
            "total_volume": {"rule": "is_positive_or_null", "severity": "warning"},
            "high_24h": {"rule": "is_positive_or_null", "severity": "warning"},
            "low_24h": {"rule": "is_positive_or_null", "severity": "warning"},
            "price_change_24h": {"rule": "is_numeric_or_null", "severity": "warning"},
            "price_change_percentage_24h": {"rule": "is_numeric_or_null", "severity": "warning"},
            
            # Datas que devem estar em um formato válido
            "last_updated": {"rule": "is_valid_timestamp", "severity": "error"},
            "ath_date": {"rule": "is_valid_timestamp_or_null", "severity": "warning"},
            "atl_date": {"rule": "is_valid_timestamp_or_null", "severity": "warning"},
            
            # Validações específicas
            "market_cap_rank": {"rule": "is_positive_integer_or_null", "severity": "warning"}
        }
        
        # Contadores de validação
        validation_metrics = {
            "total_records": initial_count,
            "warnings": 0,
            "errors": 0,
            "invalid_records": 0
        }
        
        # Aplica validações
        for column, config in validation_rules.items():
            rule = config["rule"]
            severity = config["severity"]
            
            if column not in df.columns:
                logger.warning(f"Coluna '{column}' não encontrada no DataFrame. Pulando validação.")
                continue
                
            if rule == "is_not_null":
                invalid_count = df.filter(col(column).isNull()).count()
                if invalid_count > 0:
                    msg = f"{invalid_count} registros com valor nulo na coluna {column}"
                    if severity == "error":
                        logger.error(msg)
                        validation_metrics["errors"] += invalid_count
                    else:
                        logger.warning(msg)
                        validation_metrics["warnings"] += 1
            
            elif rule == "is_positive_or_null":
                invalid_count = df.filter((col(column) < 0) & (col(column).isNotNull())).count()
                if invalid_count > 0:
                    msg = f"{invalid_count} registros com valor negativo na coluna {column}"
                    if severity == "error":
                        logger.error(msg)
                        validation_metrics["errors"] += invalid_count
                    else:
                        logger.warning(msg)
                        validation_metrics["warnings"] += 1
            
            elif rule == "is_numeric_or_null":
                try:
                    # Tenta converter para numérico, se falhar, marca como inválido
                    df = df.withColumn(
                        column,
                        when(col(column).isNotNull(), col(column).cast("double")).otherwise(lit(None))
                    )
                except Exception as e:
                    invalid_count = df.filter(col(column).isNotNull()).count()
                    logger.warning(f"{invalid_count} registros com valor não numérico na coluna {column}")
                    validation_metrics["warnings"] += 1
            
            elif rule == "is_valid_timestamp" or rule == "is_valid_timestamp_or_null":
                try:
                    df = df.withColumn(
                        column,
                        when(
                            col(column).isNotNull(), 
                            to_timestamp(col(column))
                        ).otherwise(lit(None))
                    )
                except Exception as e:
                    invalid_count = df.filter(col(column).isNotNull()).count()
                    msg = f"{invalid_count} registros com formato de data inválido na coluna {column}"
                    if rule == "is_valid_timestamp":
                        logger.error(msg)
                        validation_metrics["errors"] += invalid_count
                    else:
                        logger.warning(msg)
                        validation_metrics["warnings"] += 1
            
            elif rule == "is_positive_integer_or_null":
                invalid_count = df.filter(
                    (col(column).isNotNull()) & 
                    ((col(column) < 0) | (col(column) != col(column).cast("int")))
                ).count()
                if invalid_count > 0:
                    logger.warning(f"{invalid_count} registros com valor não inteiro ou negativo na coluna {column}")
                    validation_metrics["warnings"] += 1
        
        # Log do resumo da validação
        logger.info("\n" + "="*50)
        logger.info("RESUMO DA VALIDAÇÃO".center(50))
        logger.info("="*50)
        logger.info(f"Total de registros: {validation_metrics['total_records']}")
        logger.info(f"Avisos: {validation_metrics['warnings']}")
        logger.info(f"Erros: {validation_metrics['errors']}")
        logger.info("="*50 + "\n")
        
        # Verifica se há erros críticos
        if validation_metrics["errors"] > 0:
            error_msg = f"Validação falhou com {validation_metrics['errors']} erros críticos."
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        return df
        
    except Exception as e:
        logger.error(f"Erro durante a validação dos dados: {str(e)}", exc_info=True)
        raise

# COMMAND ----------

# DBTITLE 1,Função para transformar dados
def transform_data(df):
    """Aplica transformações nos dados."""
    try:
        print("Aplicando transformações...")
        
        # 1. Renomear colunas para nomes mais descritivos
        df = df.withColumnRenamed("id", "coin_id") \
               .withColumnRenamed("current_price", "price_usd") \
               .withColumnRenamed("market_cap", "market_cap_usd") \
               .withColumnRenamed("total_volume", "total_volume_usd")
        
        # 2. Converter tipos de dados
        df = df.withColumn("market_cap_rank", col("market_cap_rank").cast("integer"))
        
        # 3. Calcular campos derivados
        # Market Dominance
        total_market_cap = df.agg(_sum("market_cap_usd").alias("total")).collect()[0]["total"]
        df = df.withColumn(
            "market_dominance",
            (col("market_cap_usd") / lit(total_market_cap)) * 100
        )
        
        # 4. Formatar campos de data/hora
        timestamp_columns = ["ath_date", "atl_date", "last_updated"]
        for col_name in timestamp_columns:
            if col_name in df.columns:
                df = df.withColumn(col_name, to_timestamp(col(col_name)))
        
        # 5. Adicionar metadados
        df = df.withColumn("processing_timestamp", current_timestamp())
        
        # 6. Selecionar e ordenar colunas
        final_columns = [
            "coin_id", "symbol", "name", "price_usd", "market_cap_usd", 
            "market_cap_rank", "market_dominance", "total_volume_usd",
            "high_24h", "low_24h", "price_change_24h", "price_change_percentage_24h",
            "market_cap_change_24h", "market_cap_change_percentage_24h",
            "circulating_supply", "total_supply", "max_supply",
            "ath", "ath_change_percentage", "ath_date",
            "atl", "atl_change_percentage", "atl_date",
            "price_change_percentage_24h_in_currency",
            "price_change_percentage_7d_in_currency",
            "last_updated", "processing_timestamp", "pipeline_run_id"
        ]
        
        # Garante que todas as colunas existam no DataFrame
        for col_name in final_columns:
            if col_name not in df.columns and col_name != "pipeline_run_id":
                df = df.withColumn(col_name, lit(None))
        
        # Seleciona apenas as colunas desejadas
        df = df.select(final_columns)
        
        return df
        
    except Exception as e:
        print(f"Erro durante a transformação dos dados: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Função para salvar dados na camada Silver
def save_to_silver(df):
    """
    Salva os dados processados na camada Silver.
    
    Args:
        df: DataFrame contendo os dados processados
        
    Returns:
        bool: True se os dados foram salvos com sucesso, False caso contrário
    """
    try:
        if df is None or df.rdd.isEmpty():
            logger.warning("Nenhum dado para salvar na camada Silver.")
            return False
            
        logger.info(f"Salvando {df.count()} registros na camada Silver...")
        
        # Garante que o schema silver existe
        db_manager.create_schema_if_not_exists()
        
        # Verifica se a tabela silver já existe
        if db_manager.table_exists(config.silver_table_path):
            # Atualiza a tabela existente
            logger.info(f"Atualizando tabela {config.silver_table_path}...")
            
            # Cria uma view temporária
            df.createOrReplaceTempView("updates")
            
            try:
                # Tenta fazer o merge
                merge_sql = f"""
                MERGE INTO {config.silver_table_path} target
                USING updates source
                ON target.id = source.id AND target.last_updated = source.last_updated
                WHEN NOT MATCHED THEN
                    INSERT *
                """
                spark.sql(merge_sql)
                logger.info("Dados mesclados com sucesso na tabela existente.")
            except Exception as merge_error:
                logger.warning(f"Erro ao fazer merge, tentando sobrescrever a tabela: {str(merge_error)}")
                # Se o merge falhar, tenta sobrescrever a tabela
                db_manager.create_table_from_dataframe(df, config.silver_table_path)
        else:
            # Cria uma nova tabela
            db_manager.create_table_from_dataframe(df, config.silver_table_path)
        
        # Verifica o resultado
        saved_count = spark.table(config.silver_table_path).count()
        logger.info(f"✅ Dados salvos com sucesso em {config.silver_table_path}. Total de registros: {saved_count}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao salvar dados na camada Silver: {str(e)}", exc_info=True)
        raise

# COMMAND ----------

# DBTITLE 1,Execução do Pipeline Silver
def main():
    """
    Função principal de execução do pipeline Silver.
    
    Returns:
        dict: Dicionário com métricas e status da execução
    """
    start_time = time.time()
    metrics = {
        'status': 'SUCCESS',
        'start_time': datetime.now().isoformat(),
        'end_time': None,
        'duration_seconds': None,
        'records_processed': 0,
        'error_message': None,
        'bronze_table': config.bronze_table_path,
        'silver_table': config.silver_table_path
    }
    
    try:
        logger.info("=== Iniciando pipeline Silver ===")
        logger.info(f"Bronze: {config.bronze_table_path}")
        logger.info(f"Silver: {config.silver_table_path}")
        
        # 1. Ler dados da camada Bronze
        logger.info("Etapa 1/3: Lendo dados da camada Bronze...")
        bronze_df = read_bronze_data()
        if bronze_df is None or bronze_df.rdd.isEmpty():
            logger.warning("Nenhum dado para processar na camada Bronze.")
            metrics['status'] = 'SKIPPED'
            return metrics
        
        # 2. Validar dados
        logger.info("Etapa 2/3: Validando dados...")
        validated_df = validate_data(bronze_df)
        if validated_df is None or validated_df.rdd.isEmpty():
            logger.warning("Nenhum dado válido para processar após validação.")
            metrics['status'] = 'SKIPPED'
            return metrics
        
        # 3. Transformar dados
        logger.info("Etapa 3/3: Transformando dados...")
        transformed_df = transform_data(validated_df)
        if transformed_df is None or transformed_df.rdd.isEmpty():
            logger.warning("Nenhum dado para salvar após transformação.")
            metrics['status'] = 'SKIPPED'
            return metrics
        
        # 4. Salvar na camada Silver
        logger.info("Salvando resultados na camada Silver...")
        if save_to_silver(transformed_df):
            metrics['records_processed'] = transformed_df.count()
            logger.info(f"✅ Pipeline executado com sucesso. {metrics['records_processed']} registros processados.")
        else:
            metrics['status'] = 'FAILED'
            metrics['error_message'] = 'Falha ao salvar os dados na camada Silver.'
    
    except Exception as e:
        metrics['status'] = 'FAILED'
        metrics['error_message'] = str(e)
        logger.error(f"❌ Erro durante a execução do pipeline: {str(e)}", exc_info=True)
        raise
    
    finally:
        # Atualiza métricas finais
        metrics['end_time'] = datetime.now().isoformat()
        metrics['duration_seconds'] = round(time.time() - start_time, 2)
        
        # Log das métricas
        logger.info("\n=== Métricas de Execução ===")
        for key, value in metrics.items():
            if key != 'error_message' or (key == 'error_message' and value is not None):
                logger.info(f"{key.upper()}: {value}")
        
        return metrics
        logger.info(f"Duração: {metrics['duration_seconds']:.2f} segundos")
        logger.info(f"Registros processados: {metrics.get('record_count', 0)}")
        if metrics["error"]:
            logger.error(f"Erro: {metrics['error']}")
        logger.info("="*50 + "\n")

# Executa o pipeline
if __name__ == "__main__":
    try:
        metrics = main()
        # Encerra com código de erro se falhar
        if metrics and metrics["status"] == "failed":
            sys.exit(1)
    except Exception as e:
        logger.critical(f"Erro crítico não tratado: {str(e)}", exc_info=True)
        sys.exit(1)

# COMMAND ----------

# DBTITLE 1,Verificação dos Dados
# Verifica se os dados foram salvos corretamente
try:
    df = spark.table(f"{config.catalog_name}.{config.silver_schema}.{silver_table}")
    print(f"Total de registros na tabela Silver: {df.count()}")
    print("\nAmostra dos dados processados:")
    display(df.limit(5))
    
    # Verifica estatísticas básicas
    print("\nEstatísticas básicas:")
    df.select(
        "current_price", "market_cap", "market_dominance", 
        "price_change_percentage_24h"
    ).summary().show()
    
    # Otimiza a tabela para melhor desempenho
    print("\nOtimizando a tabela...")
    spark.sql(f"OPTIMIZE {config.catalog_name}.{config.silver_schema}.{silver_table} ZORDER BY (coin_id, market_cap_rank)")
    
    # Executa VACUUM para limpar arquivos antigos (mantém últimos 7 dias)
    print("Limpando arquivos antigos...")
    spark.sql(f"VACUUM {config.catalog_name}.{config.silver_schema}.{silver_table} RETAIN 168 HOURS")
    
except Exception as e:
    print(f"Erro ao verificar os dados: {str(e)}")
    dbutils.notebook.exit("Data transformation failed")
