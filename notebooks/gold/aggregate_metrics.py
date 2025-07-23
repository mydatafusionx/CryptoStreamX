# Databricks notebook source
# DBTITLE 1,Create Gold Layer Metrics
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Read from Silver Layer
# Define source and target tables
catalog = "datafusionx_catalog"
source_table = f"{catalog}.silver.coingecko_cleaned"
market_metrics_table = f"{catalog}.gold.crypto_market_metrics"
top_performers_table = f"{catalog}.gold.crypto_top_performers"

# Read the latest data from silver layer
try:
    silver_df = spark.read.table(source_table)
    
    # Get the latest ingestion timestamp
    latest_ingestion = silver_df.agg({"silver_ingestion_time": "max"}).collect()[0][0]
    
    # Filter for the latest data
    silver_df = silver_df.filter(col("silver_ingestion_time") == latest_ingestion)
    
    print(f"Processing data from {latest_ingestion}")
    print(f"Found {silver_df.count()} records")
    
except Exception as e:
    print(f"Error reading from silver layer: {str(e)}")
    dbutils.notebook.exit("Data aggregation failed")

# COMMAND ----------

# DBTITLE 1,Create Market Metrics (Gold Layer 1)
# Create gold schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

try:
    # 1. Market Overview Metrics
    market_metrics = silver_df.agg(
        count("coin_id").alias("total_coins"),
        sum("market_cap_usd").alias("total_market_cap_usd"),
        sum("total_volume_24h").alias("total_volume_24h_usd"),
        avg("price_change_percentage_24h").alias("avg_price_change_24h"),
        sum("market_cap_usd" * "price_change_percentage_24h") / sum("market_cap_usd").alias("market_cap_weighted_24h_return"),
        sum(when(col("price_change_percentage_24h") > 0, 1).otherwise(0)).alias("coins_positive_24h"),
        sum(when(col("price_change_percentage_24h") < 0, 1).otherwise(0)).alias("coins_negative_24h"),
        lit(latest_ingestion).alias("as_of_timestamp")
    )
    
    # Add additional calculated metrics
    market_metrics = market_metrics.withColumn(
        "dominance_btc", 
        silver_df.filter(col("coin_id") == "bitcoin").select("market_dominance").collect()[0][0]
    )
    
    market_metrics = market_metrics.withColumn(
        "dominance_eth", 
        silver_df.filter(col("coin_id") == "ethereum").select("market_dominance").collect()[0][0]
    )
    
    # Display results
    display(market_metrics)
    
    # Write to Delta table
    market_metrics.write.format("delta") \
        .mode("append") \
        .saveAsTable(market_metrics_table)
        
    print(f"Successfully wrote market metrics to {market_metrics_table}")
    
except Exception as e:
    print(f"Error creating market metrics: {str(e)}")
    dbutils.notebook.exit("Market metrics creation failed")

# COMMAND ----------

# DBTITLE 1,Create Top Performers (Gold Layer 2)
try:
    # 2. Top Performers by Various Metrics
    
    # Define window for ranking
    window_spec = Window.orderBy(desc("market_cap_usd"))
    
    # Top 10 by Market Cap
    top_by_market_cap = silver_df \
        .withColumn("rank", rank().over(window_spec)) \
        .filter(col("rank") <= 10) \
        .select(
            "rank", "coin_id", "name", "symbol",
            "market_cap_usd", "current_price_usd",
            "price_change_percentage_24h",
            "market_dominance"
        )
    
    # Top Gainers (24h)
    top_gainers = silver_df \
        .orderBy(desc("price_change_percentage_24h")) \
        .limit(10) \
        .select(
            "coin_id", "name", "symbol",
            "current_price_usd", "price_change_percentage_24h",
            "market_cap_usd"
        )
    
    # Top Volume (24h)
    top_volume = silver_df \
        .orderBy(desc("total_volume_24h")) \
        .limit(10) \
        .select(
            "coin_id", "name", "symbol",
            "total_volume_24h", "market_cap_usd"
        )
    
    # Combine all metrics into a single row per coin
    top_performers = top_by_market_cap.alias("cap") \
        .join(top_gainers.alias("gain"), "coin_id", "left_outer") \
        .join(top_volume.alias("vol"), "coin_id", "left_outer") \
        .select(
            col("cap.coin_id"),
            col("cap.name"),
            col("cap.symbol"),
            col("cap.rank").alias("market_cap_rank"),
            col("cap.market_cap_usd"),
            col("cap.current_price_usd"),
            col("cap.price_change_percentage_24h"),
            col("gain.price_change_percentage_24h").alias("top_gainer_24h"),
            col("vol.total_volume_24h"),
            col("cap.market_dominance"),
            lit(latest_ingestion).alias("as_of_timestamp")
        )
    
    # Display results
    display(top_performers)
    
    # Write to Delta table
    top_performers.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(top_performers_table)
        
    print(f"Successfully wrote top performers to {top_performers_table}")
    
except Exception as e:
    print(f"Error creating top performers: {str(e)}")
    dbutils.notebook.exit("Top performers creation failed")

# COMMAND ----------

# DBTITLE 1,Create Gold Table DDLs (for reference)
gold_ddls = f"""
-- Market Metrics Table
CREATE TABLE IF NOT EXISTS {market_metrics_table} (
    total_coins BIGINT,
    total_market_cap_usd DECIMAL(38,2),
    total_volume_24h_usd DECIMAL(38,2),
    avg_price_change_24h DECIMAL(10,4),
    market_cap_weighted_24h_return DECIMAL(10,4),
    coins_positive_24h BIGINT,
    coins_negative_24h BIGINT,
    dominance_btc DECIMAL(10,4),
    dominance_eth DECIMAL(10,4),
    as_of_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Top Performers Table
CREATE TABLE IF NOT EXISTS {top_performers_table} (
    coin_id STRING,
    name STRING,
    symbol STRING,
    market_cap_rank INT,
    market_cap_usd DECIMAL(38,2),
    current_price_usd DECIMAL(38,10),
    price_change_percentage_24h DECIMAL(10,4),
    top_gainer_24h DECIMAL(10,4),
    total_volume_24h DECIMAL(38,2),
    market_dominance DECIMAL(10,4),
    as_of_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (market_cap_rank)
TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
"""

print("Gold tables DDLs:")
print(gold_ddls)
