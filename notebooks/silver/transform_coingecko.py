# Databricks notebook source
# DBTITLE 1,Transform CoinGecko Data (Silver Layer)
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, coalesce, to_timestamp, from_unixtime
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Read from Bronze Layer
# Define source and target tables
catalog = "datafusionx_catalog"
source_table = f"{catalog}.bronze.coingecko_raw"
target_table = f"{catalog}.silver.coingecko_cleaned"

# Read the latest data from bronze layer
try:
    bronze_df = spark.read.table(source_table)
    
    # Get the latest ingestion timestamp
    latest_ingestion = bronze_df.agg({"ingestion_timestamp": "max"}).collect()[0][0]
    
    # Filter for the latest data
    bronze_df = bronze_df.filter(col("ingestion_timestamp") == latest_ingestion)
    
    print(f"Processing data from {latest_ingestion}")
    print(f"Found {bronze_df.count()} records")
    
except Exception as e:
    print(f"Error reading from bronze layer: {str(e)}")
    dbutils.notebook.exit("Data transformation failed")

# COMMAND ----------

# DBTITLE 1,Data Cleaning and Transformation
# Select and transform columns
try:
    silver_df = bronze_df.select(
        col("id").alias("coin_id"),
        col("symbol"),
        col("name"),
        col("current_price").cast("decimal(38,10)").alias("current_price_usd"),
        col("market_cap").cast("decimal(38,2)").alias("market_cap_usd"),
        col("market_cap_rank").cast("integer").alias("market_rank"),
        col("total_volume").cast("decimal(38,2)").alias("total_volume_24h"),
        col("high_24h").cast("decimal(38,10)").alias("high_24h_usd"),
        col("low_24h").cast("decimal(38,10)").alias("low_24h_usd"),
        col("price_change_24h").cast("decimal(38,10)").alias("price_change_24h_usd"),
        col("price_change_percentage_24h").cast("decimal(10,4)").alias("price_change_percentage_24h"),
        col("market_cap_change_24h").cast("decimal(38,2)").alias("market_cap_change_24h_usd"),
        col("market_cap_change_percentage_24h").cast("decimal(10,4)").alias("market_cap_change_percentage_24h"),
        col("circulating_supply").cast("decimal(38,2)"),
        col("total_supply").cast("decimal(38,2)"),
        col("max_supply").cast("decimal(38,2)"),
        col("ath").cast("decimal(38,10)").alias("all_time_high_usd"),
        col("ath_change_percentage").cast("decimal(10,4)").alias("ath_change_percentage"),
        when(col("ath_date").isNotNull(), to_timestamp(col("ath_date"))).alias("ath_date"),
        col("atl").cast("decimal(38,10)").alias("all_time_low_usd"),
        col("atl_change_percentage").cast("decimal(10,4)").alias("atl_change_percentage"),
        when(col("atl_date").isNotNull(), to_timestamp(col("atl_date"))).alias("atl_date"),
        col("price_change_percentage_24h_in_currency").cast("decimal(10,4)").alias("price_change_percentage_24h_usd"),
        col("price_change_percentage_7d_in_currency").cast("decimal(10,4)").alias("price_change_percentage_7d_usd"),
        col("ingestion_timestamp").alias("bronze_ingestion_time"),
        current_timestamp().alias("silver_ingestion_time")
    )
    
    # Add additional calculated fields
    silver_df = silver_df.withColumn(
        "market_dominance", 
        (col("market_cap_usd") / 
         silver_df.agg({"market_cap_usd": "sum"}).collect()[0][0]) * 100
    )
    
    # Display sample of transformed data
    display(silver_df.limit(5))
    
except Exception as e:
    print(f"Error during transformation: {str(e)}")
    dbutils.notebook.exit("Data transformation failed")

# COMMAND ----------

# DBTITLE 1,Write to Silver Layer
# Create silver schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")

# Write to Delta table with schema evolution
silver_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(target_table)

print(f"Successfully wrote {silver_df.count()} records to {target_table}")

# COMMAND ----------

# DBTITLE 1,Optimize and Vacuum Table
# Optimize the table for better performance
spark.sql(f"OPTIMIZE {target_table} ZORDER BY (coin_id, market_rank)")

# Run vacuum to clean up old files (retain last 7 days)
spark.sql(f"VACUUM {target_table} RETAIN 168 HOURS")

# COMMAND ----------

# DBTITLE 1,Create Silver Table DDL (for reference)
silver_ddl = f"""
CREATE TABLE IF NOT EXISTS {target_table} (
    coin_id STRING,
    symbol STRING,
    name STRING,
    current_price_usd DECIMAL(38,10),
    market_cap_usd DECIMAL(38,2),
    market_rank INT,
    total_volume_24h DECIMAL(38,2),
    high_24h_usd DECIMAL(38,10),
    low_24h_usd DECIMAL(38,10),
    price_change_24h_usd DECIMAL(38,10),
    price_change_percentage_24h DECIMAL(10,4),
    market_cap_change_24h_usd DECIMAL(38,2),
    market_cap_change_percentage_24h DECIMAL(10,4),
    circulating_supply DECIMAL(38,2),
    total_supply DECIMAL(38,2),
    max_supply DECIMAL(38,2),
    all_time_high_usd DECIMAL(38,10),
    ath_change_percentage DECIMAL(10,4),
    ath_date TIMESTAMP,
    all_time_low_usd DECIMAL(38,10),
    atl_change_percentage DECIMAL(10,4),
    atl_date TIMESTAMP,
    price_change_percentage_24h_usd DECIMAL(10,4),
    price_change_percentage_7d_usd DECIMAL(10,4),
    market_dominance DECIMAL(10,4),
    bronze_ingestion_time TIMESTAMP,
    silver_ingestion_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)"""

print("Silver table DDL:")
print(silver_ddl)
