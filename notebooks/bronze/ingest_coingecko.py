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

# Configuração de imports e paths
import os
import sys

# Configuração do ambiente para importação do módulo utils
import os
import sys
import shutil
import tempfile
from pathlib import Path

print("=== Configuração do ambiente ===")
print(f"Diretório de trabalho atual: {os.getcwd()}")
print(f"Python version: {sys.version}")

# 1. Cria um diretório temporário
with tempfile.TemporaryDirectory() as temp_dir:
    print(f"\n1. Criando diretório temporário: {temp_dir}")
    
    # 2. Cria a estrutura de diretórios
    src_dir = os.path.join(temp_dir, 'src')
    utils_dir = os.path.join(src_dir, 'utils')
    os.makedirs(utils_dir, exist_ok=True)
    
    print(f"2. Estrutura de diretórios criada em: {src_dir}")
    
    # 3. Lista de arquivos necessários
    required_files = [
        'api_client.py',
        'config.py',
        'db_utils.py'
    ]
    
    # 4. Cria os arquivos necessários
    print("3. Criando arquivos necessários...")
    
    # api_client.py
    api_client_content = [
        '# api_client.py simplificado',
        'import requests',
        '',
        'class CoinGeckoClient:',
        '    def __init__(self):',
        '        self.base_url = "https://api.coingecko.com/api/v3"',
        '        self.session = requests.Session()',
        '        ', 
        '    def get_coin_markets(self, vs_currency="usd", **kwargs):',
        '        """Obtém dados de mercado para criptomoedas."""',
        '        endpoint = "{}/coins/markets".format(self.base_url)',
        '        params = {',
        '            "vs_currency": vs_currency,',
        '            "order": "market_cap_desc",',
        '            "per_page": 100,',
        '            "page": 1,',
        '            "sparkline": False,',
        '        }',
        '        params.update(kwargs)',
        '        ', 
        '        try:',
        '            response = self.session.get(endpoint, params=params)',
        '            response.raise_for_status()',
        '            return response.json()',
        '        except requests.RequestException as e:',
        '            print("Erro ao buscar dados da API: {}".format(str(e)))',
        '            return []',
        '            ', 
        '    def get_market_data(self, vs_currency="usd", **kwargs):',
        '        """Método alternativo para compatibilidade."""',
        '        return self.get_coin_markets(vs_currency, **kwargs)'
    ]
    
    with open(os.path.join(utils_dir, 'api_client.py'), 'w') as f:
        f.write('\n'.join(api_client_content))
    
    # config.py
    config_content = """
# config.py simplificado
config = {
    "catalog_name": "hive_metastore",
    "bronze_schema": "bronze",
    "silver_schema": "silver",
    "gold_schema": "gold",
    "database": {
        "path": "/dbfs/FileStore/tables/crypto_data",
        "format": "delta"
    },
    "api": {
        "retries": 3,
        "timeout": 30
    }
}
"""
    with open(os.path.join(utils_dir, 'config.py'), 'w') as f:
        f.write(config_content.strip())
    
    # db_utils.py
    db_utils_content = [
        'from pyspark.sql import SparkSession',
        'from pyspark.sql.functions import current_timestamp, lit',
        'from datetime import datetime',
        'from typing import Optional, List',
        'from pyspark.sql import DataFrame',
        'from pyspark.sql.types import StructType',
        '',
        'class DeltaTableManager:',
        '    """Helper class for managing Delta tables."""',
        '    ', 
        '    def __init__(self, spark: SparkSession, catalog_name: str, schema_name: str, table_name: Optional[str] = None):',
        '        """Initialize the DeltaTableManager.',
        '        ', 
        '        Args:',
        '            spark: Active SparkSession',
        '            catalog_name: Name of the catalog (e.g., \'hive_metastore\')',
        '            schema_name: Name of the schema (e.g., \'bronze\', \'silver\', \'gold\')',
        '            table_name: Optional default table name to use for operations',
        '        """',
        '        self.spark = spark',
        '        self.catalog_name = catalog_name',
        '        self.schema_name = schema_name',
        '        self.table_name = table_name',
        '        self.full_schema_path = f"{catalog_name}.{schema_name}"',
        '        ', 
        '        # Ensure catalog and schema exist',
        '        self._ensure_catalog_and_schema()',
        '    ', 
        '    def _ensure_catalog_and_schema(self) -> None:',
        '        """Ensure that the catalog and schema exist."""',
        '        try:',
        '            # No need to create catalog for hive_metastore',
        '            if self.catalog_name.lower() != "hive_metastore":',
        '                self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog_name}")',
        '            ', 
        '            # Create schema if not exists',
        '            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.full_schema_path}")',
        '            print(f"✅ Schema verificado/criado: {self.full_schema_path}")',
        '            ', 
        '        except Exception as e:',
        '            print(f"❌ Erro ao verificar/criar schema {self.full_schema_path}: {str(e)}")',
        '            raise',
        '    ', 
        '    def table_exists(self, table_name: str = None) -> bool:',
        '        """Check if a table exists in the current schema."""',
        '        table_to_check = table_name or self.table_name',
        '        if not table_to_check:',
        '            raise ValueError("No table name provided and no default table_name set")',
        '            ',
        '        full_table_name = f"{self.full_schema_path}.{table_to_check}"',
        '        try:',
        '            self.spark.sql(f"DESCRIBE TABLE {full_table_name}")',
        '            return True',
        '        except Exception as e:',
        '            if "Table or view not found" in str(e):',
        '                return False',
        '            raise',
        '    ', 
        '    def write_dataframe(',
        '        self,',
        '        df: DataFrame,',
        '        table_name: str = None,',
        '        mode: str = "append",',
        '        merge_schema: bool = False,',
        '        overwrite_schema: bool = False,',
        '        partition_by: Optional[List[str]] = None',
        '    ) -> None:',
        '        """Write a DataFrame to a Delta table."""',
        '        target_table = table_name or self.table_name',
        '        if not target_table:',
        '            raise ValueError("No table name provided and no default table_name set")',
        '            
        '        full_table_name = f"{self.full_schema_path}.{target_table}"',
        '        print(f"Preparando para escrever na tabela: {full_table_name}")',
        '        print(f"Schema do DataFrame: {df.schema}")',
        '        print(f"Modo de escrita: {mode}")',
        '        print(f"Partição: {partition_by}")',
        '        
        '        # Add metadata columns if they don\'t exist',
        '        if "ingestion_timestamp" not in df.columns:',
        '            df = df.withColumn("ingestion_timestamp", current_timestamp())',
        '        if "pipeline_run_id" not in df.columns:',
        '            df = df.withColumn("pipeline_run_id", lit(f\'run_{int(datetime.now().timestamp())}\'))',
        '        
        '        # Create table if it doesn\'t exist',
        '        if not self.table_exists(target_table):',
        '            print(f"Tabela {full_table_name} não existe. Criando...")',
        '            try:',
        '                # Create table with explicit schema',
        '                create_stmt = f\'CREATE TABLE {full_table_name} (\''
        '                for field in df.schema.fields:',
        '                    create_stmt += f"\\n  {field.name} {field.dataType.simpleString()},"',
        '                create_stmt = create_stmt.rstrip(",") + "\\n)"
        '                
        '                if partition_by:',
        '                    create_stmt += f"\\nPARTITIONED BY ({", ".join(partition_by)})"',
        '                    
        '                create_stmt += "\\nUSING DELTA"
        '                print(f"Executando: {create_stmt}")
        '                self.spark.sql(create_stmt)
        '                print(f"✅ Tabela {full_table_name} criada com sucesso!")
        '            except Exception as e:',
        '                print(f"❌ Erro ao criar tabela {full_table_name}: {str(e)}")
        '                raise',
        '        
        '        # Configure writer',
        '        writer = df.write.format("delta")',
        '        if partition_by:',
        '            writer = writer.partitionBy(partition_by)',
        '        if merge_schema:',
        '            writer = writer.option("mergeSchema", "true")',
        '        if overwrite_schema:',
        '            writer = writer.option("overwriteSchema", "true")',
        '            
        '        try:',
        '            print(f"Escrevendo {df.count()} registros em {full_table_name}...")',
        '            writer.mode(mode).saveAsTable(full_table_name)',
        '            print(f"✅ Dados escritos com sucesso em {full_table_name}")',
        '        except Exception as e:',
        '            error_msg = str(e).lower()',
        '            print(f"❌ Erro ao escrever na tabela {full_table_name}: {error_msg}")
        '            
        '            if "schema mismatch" in error_msg and not merge_schema:',
        '                print("⚠️  Tentando novamente com mergeSchema=True...")',
        '                writer.option("mergeSchema", "true").mode(mode).saveAsTable(full_table_name)',
        '                print("✅ Dados escritos com sucesso usando mergeSchema=True")',
        '            elif "table or view not found" in error_msg:',
        '                print("⚠️  Tabela não encontrada. Tentando recriar a tabela...")',
        '                self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")',
        '                # Try again with a fresh table',
        '                return self.write_dataframe(df, target_table, mode, merge_schema, overwrite_schema, partition_by)',
        '            else:',
        '                raise',
        '    ', 
        '    def read_table(self, table_name: str = None) -> DataFrame:',
        '        """Read data from a Delta table."""',
        '        target_table = table_name or self.table_name',
        '        if not target_table:',
        '            raise ValueError("No table name provided and no default table_name set")',
        '            
        '        full_table_name = f"{self.full_schema_path}.{target_table}"',
        '        return self.spark.read.table(full_table_name)',
        '    ', 
        '    def table_exists(self):',
        '        """Verifica se a tabela existe."""',
        '        from pyspark.sql.utils import AnalysisException',
        '        try:',
        '            self.spark.sql(',
        '                "DESCRIBE TABLE {}".format(self.full_table_name)',
        '            )',
        '            return True',
        '        except AnalysisException:',
        '            return False'
    ]
    with open(os.path.join(utils_dir, 'db_utils.py'), 'w') as f:
        f.write('\n'.join(db_utils_content))
    
    print("4. Arquivos criados com sucesso!")
    
    # 5. Adiciona o diretório src ao PYTHONPATH
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    
    print(f"\n5. Adicionado ao PYTHONPATH: {src_dir}")
    
    # 6. Tenta importar o módulo utils
    print("\n6. Tentando importar o módulo 'utils'...")
    try:
        import utils
        print("✅ Módulo 'utils' importado com sucesso!")
        print(f"Localização: {utils.__file__}")
        
        # 7. Verifica se as classes necessárias estão disponíveis
        print("\n7. Verificando classes necessárias...")
        try:
            from utils.api_client import CoinGeckoClient
            from utils.db_utils import DeltaTableManager
            from utils.config import config
            
            print("✅ Todas as classes necessárias foram importadas com sucesso!")
            
        except ImportError as e:
            print(f"❌ Erro ao importar classes necessárias: {str(e)}")
            raise
            
    except ImportError as e:
        print(f"❌ Não foi possível importar o módulo 'utils': {str(e)}")
        print("\nCaminhos de busca atuais (PYTHONPATH):")
        for i, path in enumerate(sys.path[:10]):
            print(f"{i}. {path}")
        if len(sys.path) > 10:
            print(f"... e mais {len(sys.path) - 10} caminhos")
        raise

# Continua com o resto do código
print("\n=== Ambiente configurado com sucesso! ===\n")

# Importações personalizadas
from utils.api_client import CoinGeckoClient
from utils.db_utils import DeltaTableManager
from utils.config import config

# Verifica se estamos em um ambiente Databricks
IS_DATABRICKS = 'dbutils' in globals()

# Inicialização do Spark
try:
    if not IS_DATABRICKS:
        print("Inicializando Spark localmente...")
        from pyspark.sql import SparkSession
        
        # Configurações para execução local
        spark = (
            SparkSession.builder
            .appName("CryptoStreamX")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0")
            .getOrCreate()
        )
        print("✅ Spark session criada localmente")
    else:
        print("🔵 Usando sessão Spark existente (Databricks)")
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
except Exception as e:
    print(f"⚠️ Aviso: Não foi possível inicializar o Spark: {str(e)}")
    print("Tentando continuar sem Spark...")
    spark = None

# COMMAND ----------

# DBTITLE 1,Configuração Inicial
# Inicializa o cliente da API
try:
    # Tenta carregar a chave da API do ambiente ou de um arquivo de configuração
    api_key = os.environ.get('COINGECKO_API_KEY')
    
    if not api_key and os.path.exists('config.py'):
        try:
            from config import COINGECKO_API_KEY as config_key
            api_key = config_key
        except ImportError:
            pass
    
    if api_key:
        print("🔑 Usando chave de API fornecida")
        coingecko = CoinGeckoClient(api_key=api_key)
    else:
        print("⚠️ Nenhuma chave de API encontrada. Usando modo sem autenticação (limitações podem se aplicar).")
        coingecko = CoinGeckoClient()
        
    # Testa a conexão
    print("Testando conexão com a API CoinGecko...")
    try:
        test_data = coingecko.get_market_data(per_page=1)
        if test_data and isinstance(test_data, list) and len(test_data) > 0:
            print("✅ Conexão com a API CoinGecko bem-sucedida!")
            print(f"  Moeda: {test_data[0].get('id', 'N/A')} - Preço: {test_data[0].get('current_price', 'N/A')} {test_data[0].get('currency', 'USD')}")
        else:
            print("⚠️ A API retornou uma resposta vazia ou inválida.")
    except Exception as e:
        print(f"❌ Erro ao testar a conexão com a API CoinGecko: {str(e)}")
        print("O script continuará, mas algumas funcionalidades podem não estar disponíveis.")
    
except Exception as e:
    print(f"❌ Erro ao inicializar o cliente da API: {str(e)}")
    print("O script continuará, mas algumas funcionalidades podem não estar disponíveis.")
    coingecko = None

def get_config_value(key, default=None):
    """Safely get a value from config, whether it's an object or dict."""
    try:
        # Try to access as object
        return getattr(config, key, default)
    except (AttributeError, TypeError):
        # Fall back to dictionary access
        if isinstance(config, dict):
            return config.get(key, default)
        return default

# Inicializa o gerenciador de tabelas Delta
# Na versão Community, usamos 'hive_metastore' como catálogo
catalog_name = 'hive_metastore'  # Usando hive_metastore para Community Edition
bronze_schema = get_config_value('bronze_schema', 'bronze')
table_name = "coingecko_raw"

print("\n=== Configuração do DeltaTableManager ===")
print(f"Versão do Databricks: {spark.version}")
print(f"Catálogo: {catalog_name}")
print(f"Schema: {bronze_schema}")
print(f"Tabela: {table_name}")

# Cria uma instância do DeltaTableManager
try:
    # Inicializa com todos os parâmetros necessários
    print(f"Inicializando DeltaTableManager com os seguintes parâmetros:")
    print(f"- catalog_name: {catalog_name}")
    print(f"- schema_name: {bronze_schema}")
    print(f"- table_name: {table_name}")
    
    db_manager = DeltaTableManager(
        spark=spark,
        catalog_name=catalog_name,
        schema_name=bronze_schema,
        table_name=table_name  # Adicionando o parâmetro obrigatório
    )
    print("✅ Gerenciador de tabelas Delta inicializado com sucesso!")
    
    # Verifica se o banco de dados/schema existe
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_schema}")
    spark.sql(f"USE {bronze_schema}")
    print(f"✅ Banco de dados/schema '{bronze_schema}' verificado/criado com sucesso!")
    
    # Lista as tabelas existentes para depuração
    print("\n📋 Tabelas existentes no schema:")
    spark.sql(f"SHOW TABLES IN {bronze_schema}").show(truncate=False)
    
    print("✅ Configuração inicial concluída com sucesso!")
    
except Exception as e:
    print(f"❌ Erro ao inicializar DeltaTableManager: {str(e)}")
    raise

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
        run_id = run_id  # Use the run_id from the main function
        
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
            partition_by=["pipeline_run_id"]
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
        ✅ Dados processados com sucesso!
        • Registros processados: {metrics['records_processed']}
        • Registros salvos: {metrics['records_saved']}
        • Tabela: {get_config_value('catalog_name')}.{get_config_value('bronze_schema')}.{table_name}
        • Tempo total: {metrics['duration_seconds']:.2f} segundos
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
    # Verifica se está em ambiente Databricks
    is_databricks = 'dbutils' in globals()
    
    # Gera um run_id baseado no ambiente
    run_id = f"manual_run_{int(time.time())}"  # Default run_id
    
    try:
        if is_databricks:
            # Try to get run_id from widget, create it if it doesn't exist
            try:
                run_id = dbutils.widgets.get("run_id")
            except Exception:
                # Widget doesn't exist, create it with a default value
                dbutils.widgets.text("run_id", run_id, "Run ID")
    except Exception as e:
        print(f"⚠️ Warning: Could not access Databricks widgets: {str(e)}")
        print(f"Using default run_id: {run_id}")
    
    execution_metrics = {
        'pipeline_start_time': current_timestamp(),
        'pipeline_end_time': None,
        'pipeline_duration_seconds': None,
        'pipeline_status': 'FAILED',
        'api_status': None,
        'processing_status': None,
        'total_records_processed': 0,
        'error': None,
        'run_id': run_id
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
        # Convert to timestamp in seconds (as float)
        duration = (execution_metrics['pipeline_end_time'] - execution_metrics['pipeline_start_time']).total_seconds()
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
        
        # Salva as métricas de execução
        try:
            save_execution_metrics(execution_metrics)
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
        metrics_table = f"{get_config_value('catalog_name', 'datafusionx_catalog')}.monitoring.pipeline_executions"
        
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
        metrics_table = f"{get_config_value('catalog_name', 'datafusionx_catalog')}.monitoring.pipeline_executions"
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
    df = spark.table(f"{get_config_value('catalog_name')}.{get_config_value('bronze_schema')}.{table_name}")
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
