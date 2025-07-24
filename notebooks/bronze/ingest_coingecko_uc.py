#!/usr/bin/env python
# Databricks notebook source
# DBTITLE 1,Ingestão de Dados CoinGecko (Unity Catalog)
"""
Script para ingestão de dados da API CoinGecko para Delta Lake usando Unity Catalog.
"""

import os
import sys
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp, lit

# Configurações
CATALOG_NAME = 'main'  # Catálogo padrão no Unity Catalog
SCHEMA_NAME = 'bronze'
TABLE_NAME = 'coingecko_raw'
FULL_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

# Inicialização do Spark
def init_spark():
    """Inicializa e retorna uma sessão do Spark."""
    print("Inicializando Spark...")
    return SparkSession.builder \
        .appName("CryptoStreamX") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

# Cliente da API CoinGecko
class CoinGeckoClient:
    def __init__(self, api_key=None):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({"x-cg-demo-api-key": api_key})
    
    def get_market_data(self, vs_currency='usd', **kwargs):
        """Obtém dados de mercado para criptomoedas."""
        endpoint = f"{self.base_url}/coins/markets"
        params = {
            "vs_currency": vs_currency,
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": False,
            "price_change_percentage": "24h,7d"
        }
        params.update(kwargs)
        
        try:
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Erro na requisição: {str(e)}")
            return []

def create_table_if_not_exists(spark):
    """Cria a tabela se ela não existir."""
    schema = StructType([
        StructField("id", StringType()),
        StructField("symbol", StringType()),
        StructField("name", StringType()),
        StructField("current_price", DoubleType()),
        StructField("market_cap", DoubleType()),
        StructField("total_volume", DoubleType()),
        StructField("price_change_percentage_24h", DoubleType()),
        StructField("last_updated", StringType()),
        StructField("ingestion_timestamp", TimestampType()),
        StructField("pipeline_run_id", StringType())
    ])
    
    print(f"Verificando tabela {FULL_TABLE_NAME}...")
    
    # Verifica se a tabela já existe
    if not spark.catalog.tableExists(FULL_TABLE_NAME):
        print(f"Criando tabela {FULL_TABLE_NAME}...")
        # Cria o schema se não existir
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
        
        # Cria a tabela usando SQL para garantir compatibilidade com Unity Catalog
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
            id STRING,
            symbol STRING,
            name STRING,
            current_price DOUBLE,
            market_cap DOUBLE,
            total_volume DOUBLE,
            price_change_percentage_24h DOUBLE,
            last_updated STRING,
            ingestion_timestamp TIMESTAMP,
            pipeline_run_id STRING
        ) USING DELTA
        """
        spark.sql(create_table_sql)
        print(f"✅ Tabela {FULL_TABLE_NAME} criada com sucesso!")
    else:
        print(f"ℹ️  Tabela {FULL_TABLE_NAME} já existe.")

def main():
    print("=== Iniciando ingestão de dados da CoinGecko ===")
    print(f"Tabela de destino: {FULL_TABLE_NAME}")
    
    try:
        # 1. Inicializa o Spark
        spark = init_spark()
        print(f"✅ Spark inicializado. Versão: {spark.version}")
        
        # 2. Cria a tabela se não existir
        create_table_if_not_exists(spark)
        
        # 3. Busca dados da API
        print("\nBuscando dados da API CoinGecko...")
        client = CoinGeckoClient()
        data = client.get_market_data(per_page=10)  # Apenas 10 registros para testes
        
        if not data:
            raise ValueError("Nenhum dado retornado pela API")
            
        print(f"✅ Dados recebidos: {len(data)} registros")
        
        # 4. Prepara os dados para o DataFrame
        print("Processando dados...")
        run_id = f"run_{int(datetime.now().timestamp())}"
        
        # Extrai apenas os campos que precisamos
        rows = []
        for coin in data:
            rows.append((
                coin.get('id'),
                coin.get('symbol'),
                coin.get('name'),
                coin.get('current_price'),
                coin.get('market_cap'),
                coin.get('total_volume'),
                coin.get('price_change_percentage_24h'),
                coin.get('last_updated'),
                datetime.now(),
                run_id
            ))
        
        # 5. Cria DataFrame com schema explícito
        schema = spark.table(FULL_TABLE_NAME).schema
        df = spark.createDataFrame(rows, schema)
        
        # 6. Salva os dados na tabela Delta
        print(f"\nSalvando dados na tabela {FULL_TABLE_NAME}...")
        df.write.format("delta") \
               .mode("append") \
               .option("mergeSchema", "true") \
               .saveAsTable(FULL_TABLE_NAME)
        
        # 7. Mostra estatísticas
        total_records = spark.table(FULL_TABLE_NAME).count()
        print(f"✅ Dados salvos com sucesso! Total de registros na tabela: {total_records}")
        
        # Mostra uma amostra dos dados
        print("\n📋 Amostra dos dados salvos:")
        spark.table(FULL_TABLE_NAME).show(5, truncate=False)
        
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            print("\nEncerrando sessão do Spark...")
            spark.stop()
            print("✅ Sessão do Spark encerrada.")

if __name__ == "__main__":
    main()
