# Databricks notebook source
# DBTITLE 1,Pipeline Monitoring and Alerting
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as _max, min as _min, avg, stddev
import json
import requests
from datetime import datetime, timedelta

# COMMAND ----------

# DBTITLE 1,Configuration
# Catalog and schema names
catalog = "datafusionx_catalog"
bronze_table = f"{catalog}.bronze.coingecko_raw"
silver_table = f"{catalog}.silver.coingecko_cleaned"
gold_metrics_table = f"{catalog}.gold.crypto_market_metrics"

alert_thresholds = {
    "bronze_row_count": 10,  # Minimum expected rows in bronze
    "silver_row_count": 10,  # Minimum expected rows in silver
    "gold_metrics_count": 1,  # Minimum expected rows in gold metrics
    "data_freshness_hours": 2,  # Maximum allowed data age in hours
    "null_percentage_threshold": 10.0  # Maximum allowed percentage of NULL values in key columns
}

alerts = []

# COMMAND ----------

# DBTITLE 1,Check Data Freshness
def check_data_freshness():
    """Check when the data was last updated in each layer"""
    try:
        # Check Bronze layer freshness
        bronze_latest = spark.sql(f"""
            SELECT MAX(ingestion_timestamp) as latest_timestamp 
            FROM {bronze_table}
        ").collect()[0][0]
        
        if bronze_latest is None:
            alerts.append({"level": "ERROR", "message": "No data found in Bronze layer"})
        else:
            time_diff = datetime.utcnow() - bronze_latest
            if time_diff > timedelta(hours=alert_thresholds["data_freshness_hours"]):
                alerts.append({
                    "level": "WARNING",
                    "message": f"Bronze data is {time_diff} old (threshold: {alert_thresholds['data_freshness_hours']} hours)"
                })
        
        # Check Silver layer freshness
        silver_latest = spark.sql(f"""
            SELECT MAX(silver_ingestion_time) as latest_timestamp 
            FROM {silver_table}
        ").collect()[0][0]
        
        if silver_latest is None:
            alerts.append({"level": "ERROR", "message": "No data found in Silver layer"})
        else:
            time_diff = datetime.utcnow() - silver_latest
            if time_diff > timedelta(hours=alert_thresholds["data_freshness_hours"]):
                alerts.append({
                    "level": "WARNING",
                    "message": f"Silver data is {time_diff} old (threshold: {alert_thresholds['data_freshness_hours']} hours)"
                })
                
    except Exception as e:
        alerts.append({
            "level": "ERROR",
            "message": f"Error checking data freshness: {str(e)}"
        })

# COMMAND ----------

# DBTITLE 1,Check Data Quality
def check_data_quality():
    """Check data quality metrics"""
    try:
        # Check row counts
        bronze_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {bronze_table}").collect()[0][0]
        if bronze_count < alert_thresholds["bronze_row_count"]:
            alerts.append({
                "level": "WARNING",
                "message": f"Low row count in Bronze layer: {bronze_count} (min expected: {alert_thresholds['bronze_row_count']})"
            })
            
        silver_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {silver_table}").collect()[0][0]
        if silver_count < alert_thresholds["silver_row_count"]:
            alerts.append({
                "level": "WARNING",
                "message": f"Low row count in Silver layer: {silver_count} (min expected: {alert_thresholds['silver_row_count']})"
            })
            
        gold_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {gold_metrics_table}").collect()[0][0]
        if gold_count < alert_thresholds["gold_metrics_count"]:
            alerts.append({
                "level": "WARNING",
                "message": f"Low row count in Gold metrics: {gold_count} (min expected: {alert_thresholds['gold_metrics_count']})"
            })
            
        # Check for NULL values in key columns
        null_checks = [
            ("current_price_usd", silver_table),
            ("market_cap_usd", silver_table),
            ("total_volume_24h", silver_table),
            ("total_market_cap_usd", gold_metrics_table)
        ]
        
        for column, table in null_checks:
            result = spark.sql(f"""
                SELECT 
                    (COUNT(CASE WHEN {column} IS NULL THEN 1 END) * 100.0 / COUNT(*)) as null_percentage
                FROM {table}
            """).collect()[0][0]
            
            if result > alert_thresholds["null_percentage_threshold"]:
                alerts.append({
                    "level": "WARNING",
                    "message": f"High NULL percentage in {table}.{column}: {result:.2f}% (threshold: {alert_thresholds['null_percentage_threshold']}%)"
                })
                
    except Exception as e:
        alerts.append({
            "level": "ERROR",
            "message": f"Error checking data quality: {str(e)}"
        })

# COMMAND ----------

# DBTITLE 1,Check Data Consistency
def check_data_consistency():
    """Check consistency between layers"""
    try:
        # Check if the number of coins in silver matches bronze (with some tolerance for filtering)
        bronze_count = spark.sql(f"SELECT COUNT(DISTINCT id) as cnt FROM {bronze_table}").collect()[0][0]
        silver_count = spark.sql(f"SELECT COUNT(DISTINCT coin_id) as cnt FROM {silver_table}").collect()[0][0]
        
        if bronze_count > 0 and silver_count / bronze_count < 0.9:  # Allow 10% filtering
            alerts.append({
                "level": "WARNING",
                "message": f"Significant data loss between Bronze and Silver layers: {silver_count}/{bronze_count} records"
            })
            
        # Check if market cap in gold matches sum in silver
        silver_market_cap = spark.sql(f"SELECT SUM(market_cap_usd) as total FROM {silver_table}").collect()[0][0]
        gold_market_cap = spark.sql(f"SELECT total_market_cap_usd as total FROM {gold_metrics_table} ORDER BY as_of_timestamp DESC LIMIT 1").collect()[0][0]
        
        if silver_market_cap and gold_market_cap:
            diff_pct = abs((silver_market_cap - gold_market_cap) / silver_market_c * 100)
            if diff_pct > 1.0:  # More than 1% difference
                alerts.append({
                    "level": "WARNING",
                    "message": f"Market cap mismatch between Silver and Gold layers: {diff_pct:.2f}% difference"
                })
                
    except Exception as e:
        alerts.append({
            "level": "ERROR",
            "message": f"Error checking data consistency: {str(e)}"
        })

# COMMAND ----------

# DBTITLE 1,Check Pipeline Performance
def check_pipeline_performance():
    """Check pipeline execution metrics"""
    try:
        # Get job run history (last 7 days)
        job_runs = spark.sql(f"""
            SELECT 
                job_name,
                run_id,
                start_time,
                end_time,
                (unix_timestamp(end_time) - unix_timestamp(start_time)) as duration_seconds
            FROM system.compute.history
            WHERE job_name = 'crypto_data_pipeline'
            AND start_time >= date_sub(current_timestamp(), 7)
            ORDER BY start_time DESC
        """)
        
        # Calculate average duration
        if job_runs.count() > 0:
            avg_duration = job_runs.select(avg("duration_seconds")).collect()[0][0]
            last_run = job_runs.first()
            
            # Check if last run was successful
            if last_run["end_time"] is None:
                alerts.append({
                    "level": "ERROR",
                    "message": "Last pipeline run did not complete successfully"
                })
            else:
                # Check if duration is within expected range
                if last_run["duration_seconds"] > avg_duration * 1.5:  # 50% longer than average
                    alerts.append({
                        "level": "WARNING",
                        "message": f"Pipeline execution time increased: {last_run['duration_seconds']:.0f}s (avg: {avg_duration:.0f}s)"
                    })
                    
    except Exception as e:
        alerts.append({
            "level": "ERROR",
            "message": f"Error checking pipeline performance: {str(e)}"
        })

# COMMAND ----------

# DBTITLE 1,Generate Report
def generate_report():
    """Generate monitoring report"""
    print("=" * 80)
    print("CRYPTO DATA PIPELINE MONITORING REPORT")
    print("=" * 80)
    print(f"Generated at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
    
    # Print alerts if any
    if alerts:
        print("\n" + "=" * 80)
        print("ALERTS")
        print("=" * 80)
        for alert in alerts:
            print(f"[{alert['level']}] {alert['message']}")
    else:
        print("No issues detected. Pipeline is healthy!")
    
    # Print data statistics
    print("\n" + "=" * 80)
    print("DATA STATISTICS")
    print("=" * 80)
    
    try:
        # Bronze stats
        bronze_stats = spark.sql(f"""
            SELECT 
                COUNT(*) as row_count,
                MIN(ingestion_timestamp) as first_record,
                MAX(ingestion_timestamp) as last_record,
                COUNT(DISTINCT id) as unique_coins
            FROM {bronze_table}
        ").collect()[0]
        
        print("\nBRONZE LAYER")
        print(f"- Total Rows: {bronze_stats['row_count']:,}")
        print(f"- Unique Coins: {bronze_stats['unique_coins']}")
        print(f"- First Record: {bronze_stats['first_record']}")
        print(f"- Last Record: {bronze_stats['last_record']}")
        
        # Silver stats
        silver_stats = spark.sql(f"""
            SELECT 
                COUNT(*) as row_count,
                MIN(silver_ingestion_time) as first_record,
                MAX(silver_ingestion_time) as last_record,
                COUNT(DISTINCT coin_id) as unique_coins,
                AVG(current_price_usd) as avg_price,
                SUM(market_cap_usd) as total_market_cap
            FROM {silver_table}
        ").collect()[0]
        
        print("\nSILVER LAYER")
        print(f"- Total Rows: {silver_stats['row_count']:,}")
        print(f"- Unique Coins: {silver_stats['unique_coins']}")
        print(f"- First Record: {silver_stats['first_record']}")
        print(f"- Last Record: {silver_stats['last_record']}")
        print(f"- Average Price: ${silver_stats['avg_price']:,.2f}")
        print(f"- Total Market Cap: ${silver_stats['total_market_cap']:,.0f}")
        
        # Gold stats
        gold_stats = spark.sql(f"""
            SELECT 
                COUNT(*) as row_count,
                MIN(as_of_timestamp) as first_record,
                MAX(as_of_timestamp) as last_record,
                AVG(total_market_cap_usd) as avg_market_cap
            FROM {gold_metrics_table}
        ").collect()[0]
        
        print("\nGOLD LAYER (MARKET METRICS)")
        print(f"- Total Records: {gold_stats['row_count']:,}")
        print(f"- First Record: {gold_stats['first_record']}")
        print(f"- Last Record: {gold_stats['last_record']}")
        print(f"- Average Market Cap: ${gold_stats['avg_market_cap']:,.0f}")
        
    except Exception as e:
        print(f"\nError generating statistics: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Execute Monitoring Checks
# Run all monitoring checks
check_data_freshness()
check_data_quality()
check_data_consistency()
check_pipeline_performance()

# Generate and display the report
generate_report()

# Return non-zero exit code if there are critical alerts
if any(alert["level"] == "ERROR" for alert in alerts):
    dbutils.notebook.exit(1)
else:
    dbutils.notebook.exit(0)
