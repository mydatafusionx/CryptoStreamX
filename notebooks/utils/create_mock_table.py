#!/usr/bin/env python
# Databricks notebook source
# DBTITLE 1,Criar Tabela com Dados Mockados
"""
Utilitário para criar uma tabela com dados mockados para testes.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import current_timestamp, lit, expr
import random
from datetime import datetime, timedelta

def create_mock_crypto_data(spark, table_name="crypto_mock", num_records=100, schema_name="bronze"):
    """
    Cria uma tabela com dados mockados de criptomoedas.
    
    Args:
        spark: Sessão do Spark
        table_name: Nome da tabela a ser criada
        num_records: Número de registros a serem gerados
        schema_name: Nome do schema/database
    """
    # Define o schema da tabela
    schema = StructType([
        StructField("crypto_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("name", StringType(), False),
        StructField("price_usd", DoubleType(), False),
        StructField("market_cap", DoubleType(), False),
        StructField("volume_24h", DoubleType(), False),
        StructField("price_change_24h", DoubleType(), False),
        StructField("last_updated", TimestampType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("pipeline_run_id", StringType(), False)
    ])
    
    # Lista de criptomoedas para gerar dados
    cryptos = [
        ("bitcoin", "btc", "Bitcoin"),
        ("ethereum", "eth", "Ethereum"),
        ("cardano", "ada", "Cardano"),
        ("solana", "sol", "Solana"),
        ("ripple", "xrp", "XRP")
    ]
    
    # Gera dados mockados
    data = []
    current_time = datetime.utcnow()
    run_id = f"run_{int(current_time.timestamp())}"
    
    for _ in range(num_records):
        crypto = random.choice(cryptos)
        base_price = random.uniform(10, 50000)
        price_change = random.uniform(-0.1, 0.1)  # Variação de -10% a +10%
        
        record = (
            crypto[0],                                  # crypto_id
            crypto[1].upper(),                          # symbol
            crypto[2],                                  # name
            base_price,                                 # price_usd
            base_price * random.uniform(1e6, 1e9),      # market_cap
            base_price * random.uniform(1e4, 1e7),      # volume_24h
            price_change,                               # price_change_24h
            current_time - timedelta(minutes=random.randint(0, 60)),  # last_updated
            current_time,                               # ingestion_timestamp
            run_id                                      # pipeline_run_id
        )
        data.append(record)
    
    # Cria o DataFrame
    df = spark.createDataFrame(data, schema)
    
    # Cria o schema se não existir
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    
    # Salva a tabela
    full_table_name = f"{schema_name}.{table_name}"
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table_name)
    
    # Otimiza a tabela
    spark.sql(f"OPTIMIZE {full_table_name} ZORDER BY (crypto_id)")
    
    # Retorna informações sobre a tabela criada
    count = spark.table(full_table_name).count()
    print(f"\n✅ Tabela '{full_table_name}' criada com sucesso!")
    print(f"📊 Total de registros: {count:,}")
    print(f"📋 Schema:")
    df.printSchema()
    print("\n📝 Amostra dos dados:")
    df.show(5, truncate=False)
    
    return df

# Executa a função se o script for executado diretamente
if __name__ == "__main__":
    # Cria uma sessão do Spark se não existir
    spark = SparkSession.builder.getOrCreate()
    
    # Cria a tabela mockada
    create_mock_crypto_data(spark)
