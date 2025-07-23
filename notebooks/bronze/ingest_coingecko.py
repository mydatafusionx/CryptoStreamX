# Databricks notebook source
# DBTITLE 1,Ingest CoinGecko Data
# Import necessary libraries
import requests
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

# DBTITLE 1,Define CoinGecko API Parameters
# API endpoint and parameters
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,  # Max allowed by API
    "page": 1,
    "sparkline": False,
    "price_change_percentage": "24h,7d"
}

# COMMAND ----------

# DBTITLE 1,Fetch Data from CoinGecko API
try:
    # Make API request
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise exception for HTTP errors
    data = response.json()
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(data)
    
    # Add ingestion timestamp
    df['ingestion_timestamp'] = datetime.utcnow()
    
    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(df)
    
    # Display sample data
    display(spark_df.limit(5))
    
except Exception as e:
    print(f"Error fetching data from CoinGecko API: {str(e)}")
    dbutils.notebook.exit("Data ingestion failed")

# COMMAND ----------

# DBTITLE 1,Write to Delta Lake (Bronze Layer)
# Define catalog and schema
catalog = "datafusionx_catalog"
schema = "bronze"
table_name = "coingecko_raw"

# Create catalog and schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Write to Delta table
spark_df.write.format("delta").mode("append") \
    .saveAsTable(f"{catalog}.{schema}.{table_name}")

# Confirm write
print(f"Successfully wrote {spark_df.count()} records to {catalog}.{schema}.{table_name}")

# COMMAND ----------

# DBTITLE 1,Register Table in Unity Catalog
# The table is automatically registered in Unity Catalog when using saveAsTable
# This is just for verification
try:
    display(spark.sql(f"SELECT * FROM {catalog}.{schema}.{table_name} LIMIT 5"))
except Exception as e:
    print(f"Error verifying table: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Create Database and Table DDL (for reference)
# This is the DDL that would be used to create the table if it didn't exist
# The actual table is created with saveAsTable above
ddl = f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (
    id STRING,
    symbol STRING,
    name STRING,
    image STRING,
    current_price DOUBLE,
    market_cap DOUBLE,
    market_cap_rank LONG,
    fully_diluted_valuation DOUBLE,
    total_volume DOUBLE,
    high_24h DOUBLE,
    low_24h DOUBLE,
    price_change_24h DOUBLE,
    price_change_percentage_24h DOUBLE,
    market_cap_change_24h DOUBLE,
    market_cap_change_percentage_24h DOUBLE,
    circulating_supply DOUBLE,
    total_supply DOUBLE,
    max_supply DOUBLE,
    ath DOUBLE,
    ath_change_percentage DOUBLE,
    ath_date TIMESTAMP,
    atl DOUBLE,
    atl_change_percentage DOUBLE,
    atl_date TIMESTAMP,
    roi MAP<STRING,STRING>,
    last_updated TIMESTAMP,
    price_change_percentage_24h_in_currency DOUBLE,
    price_change_percentage_7d_in_currency DOUBLE,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)"""

print("Table creation DDL:")
print(ddl)
