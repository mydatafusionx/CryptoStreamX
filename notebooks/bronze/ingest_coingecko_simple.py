#!/usr/bin/env python
# Databricks notebook source
# DBTITLE 1,Ingestão de Dados CoinGecko
"""
Script para ingestão de dados da API CoinGecko para Delta Lake.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp, lit
import requests
from datetime import datetime

# Configurações
SCHEMA_NAME = 'bronze'
TABLE_NAME = 'coingecko_raw'

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
            "per_page": 10,  # Apenas 10 registros para teste
            "page": 1,
            "sparkline": False,
            "price_change_percentage": "24h"
        }
        params.update(kwargs)
        
        try:
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Erro na requisição: {str(e)}")
            return []

def create_or_update_table(spark, data, table_name=TABLE_NAME, schema_name=SCHEMA_NAME):
    """Cria ou atualiza a tabela com os dados fornecidos."""
    # Define o schema da tabela
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("name", StringType(), False),
        StructField("current_price", DoubleType(), False),
        StructField("market_cap", DoubleType(), False),
        StructField("total_volume", DoubleType(), False),
        StructField("price_change_percentage_24h", DoubleType(), True),
        StructField("last_updated", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("pipeline_run_id", StringType(), False)
    ])
    
    # Prepara os dados
    run_id = f"run_{int(datetime.utcnow().timestamp())}"
    rows = []
    for coin in data:
        rows.append((
            coin.get('id'),
            coin.get('symbol', '').upper(),
            coin.get('name', ''),
            coin.get('current_price'),
            coin.get('market_cap'),
            coin.get('total_volume'),
            coin.get('price_change_percentage_24h'),
            coin.get('last_updated', ''),
            datetime.utcnow(),
            run_id
        ))
    
    # Cria o DataFrame
    df = spark.createDataFrame(rows, schema)
    
    # Cria o schema se não existir
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    
    # Salva a tabela
    full_table_name = f"{schema_name}.{table_name}"
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true")
       .saveAsTable(full_table_name))
    
    # Otimiza a tabela
    spark.sql(f"OPTIMIZE {full_table_name} ZORDER BY (id)")
    
    # Retorna informações
    count = spark.table(full_table_name).count()
    print(f"\n✅ Dados salvos em '{full_table_name}'")
    print(f"📊 Total de registros: {count:,}")
    print("\n📝 Amostra dos dados:")
    spark.table(full_table_name).show(5, truncate=False)
    
    return df

def main():
    print("=== Iniciando ingestão de dados da CoinGecko ===")
    
    try:
        # Inicializa a sessão Spark
        spark = SparkSession.builder.getOrCreate()
        print(f"✅ Spark inicializado. Versão: {spark.version}")
        
        # Busca dados da API
        print("\nBuscando dados da API CoinGecko...")
        client = CoinGeckoClient()
        data = client.get_market_data()
        
        if not data:
            raise ValueError("Nenhum dado retornado pela API")
        
        print(f"✅ Dados recebidos: {len(data)} registros")
        
        # Processa e salva os dados
        print("\nProcessando e salvando dados...")
        df = create_or_update_table(spark, data)
        
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
