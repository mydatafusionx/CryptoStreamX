"""Tests for the main pipeline functionality."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, IntegerType
from pyspark.sql import Row
from unittest.mock import patch, MagicMock

# Import the pipeline modules to test
from notebooks.bronze.ingest_coingecko import main as bronze_main
from notebooks.silver.transform_coingecko import main as silver_main
from notebooks.gold.aggregate_metrics import main as gold_main

class TestPipelineIntegration:
    """Integration tests for the complete data pipeline."""
    
    @pytest.fixture
    def setup_tables(self, spark):
        """Set up test tables for the pipeline."""
        # Create test catalog and schemas
        spark.sql("CREATE CATALOG IF NOT EXISTS test_catalog")
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_catalog.bronze")
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_catalog.silver")
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_catalog.gold")
        
        # Define schemas
        bronze_schema = StructType([
            StructField("id", StringType()),
            StructField("symbol", StringType()),
            StructField("name", StringType()),
            StructField("current_price", DoubleType()),
            StructField("market_cap", DoubleType()),
            StructField("market_cap_rank", IntegerType()),
            StructField("total_volume", DoubleType()),
            StructField("high_24h", DoubleType()),
            StructField("low_24h", DoubleType()),
            StructField("price_change_24h", DoubleType()),
            StructField("price_change_percentage_24h", DoubleType()),
            StructField("market_cap_change_24h", DoubleType()),
            StructField("market_cap_change_percentage_24h", DoubleType()),
            StructField("circulating_supply", DoubleType()),
            StructField("total_supply", DoubleType()),
            StructField("max_supply", DoubleType()),
            StructField("ath", DoubleType()),
            StructField("ath_change_percentage", DoubleType()),
            StructField("ath_date", StringType()),
            StructField("atl", DoubleType()),
            StructField("atl_change_percentage", DoubleType()),
            StructField("atl_date", StringType()),
            StructField("last_updated", StringType()),
            StructField("ingestion_timestamp", TimestampType())
        ])
        
        # Create bronze table
        spark.sql("""
            CREATE TABLE IF NOT EXISTS test_catalog.bronze.coingecko_raw (
                id STRING,
                symbol STRING,
                name STRING,
                current_price DOUBLE,
                market_cap DOUBLE,
                market_cap_rank INT,
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
                ath_date STRING,
                atl DOUBLE,
                atl_change_percentage DOUBLE,
                atl_date STRING,
                last_updated STRING,
                ingestion_timestamp TIMESTAMP
            )
            USING DELTA
        """)
        
        yield  # Test runs here
        
        # Clean up
        spark.sql("DROP TABLE IF EXISTS test_catalog.bronze.coingecko_raw")
        spark.sql("DROP TABLE IF EXISTS test_catalog.silver.coingecko_cleaned")
        spark.sql("DROP TABLE IF EXISTS test_catalog.gold.crypto_market_metrics")
        spark.sql("DROP TABLE IF EXISTS test_catalog.gold.crypto_top_performers")
        spark.sql("DROP SCHEMA IF EXISTS test_catalog.bronze")
        spark.sql("DROP SCHEMA IF EXISTS test_catalog.silver")
        spark.sql("DROP SCHEMA IF EXISTS test_catalog.gold")
        spark.sql("DROP CATALOG IF EXISTS test_catalog")
    
    def test_bronze_ingestion(self, spark, setup_tables):
        """Test the bronze layer ingestion process."""
        # Mock the API response
        test_data = [
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "current_price": 50000.0,
                "market_cap": 950000000000,
                "market_cap_rank": 1,
                "total_volume": 30000000000,
                "high_24h": 51000.0,
                "low_24h": 49000.0,
                "price_change_24h": 1000.0,
                "price_change_percentage_24h": 2.04,
                "market_cap_change_24h": 20000000000,
                "market_cap_change_percentage_24h": 2.15,
                "circulating_supply": 19000000.0,
                "total_supply": 21000000.0,
                "max_supply": 21000000.0,
                "ath": 69000.0,
                "ath_change_percentage": -27.54,
                "ath_date": "2021-11-10T14:24:11.849Z",
                "atl": 67.81,
                "atl_change_percentage": 73630.0,
                "atl_date": "2013-07-06T00:00:00.000Z",
                "last_updated": "2023-05-01T12:00:00.000Z"
            }
        ]
        
        with patch('notebooks.bronze.ingest_coingecko.requests.get') as mock_get:
            # Configure the mock to return our test data
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = test_data
            mock_get.return_value = mock_response
            
            # Run the bronze ingestion
            bronze_main()
            
            # Verify the data was written to the bronze table
            result = spark.table("test_catalog.bronze.coingecko_raw")
            assert result.count() == 1
            
            # Verify the data was written correctly
            row = result.first()
            assert row["id"] == "bitcoin"
            assert row["symbol"] == "btc"
            assert row["current_price"] == 50000.0
            assert "ingestion_timestamp" in result.columns
    
    def test_silver_transformation(self, spark, setup_tables):
        """Test the silver layer transformation process."""
        # Insert test data into bronze table
        test_data = [
            (
                "bitcoin", "btc", "Bitcoin", 50000.0, 950000000000, 1, 30000000000, 
                51000.0, 49000.0, 1000.0, 2.04, 20000000000, 2.15, 
                19000000.0, 21000000.0, 21000000.0, 69000.0, -27.54, 
                "2021-11-10T14:24:11.849Z", 67.81, 73630.0, "2013-07-06T00:00:00.000Z", 
                "2023-05-01T12:00:00.000Z", "2023-05-01T12:00:00.000Z"
            )
        ]
        
        # Create a DataFrame with the test data
        schema = spark.table("test_catalog.bronze.coingecko_raw").schema
        df = spark.createDataFrame(test_data, schema)
        df.write.format("delta").mode("append").saveAsTable("test_catalog.bronze.coingecko_raw")
        
        # Run the silver transformation
        with patch('notebooks.silver.transform_coingecko.spark', spark):
            silver_main()
        
        # Verify the data was written to the silver table
        result = spark.table("test_catalog.silver.coingecko_cleaned")
        assert result.count() == 1
        
        # Verify the schema and data transformation
        row = result.first()
        assert row["coin_id"] == "bitcoin"
        assert row["symbol"] == "btc"
        assert row["current_price_usd"] == 50000.0
        assert "silver_ingestion_time" in result.columns
    
    def test_gold_aggregation(self, spark, setup_tables):
        """Test the gold layer aggregation process."""
        # Insert test data into silver table
        silver_data = [
            (
                "bitcoin", "btc", "Bitcoin", 50000.0, 950000000000, 1, 30000000000,
                51000.0, 49000.0, 1000.0, 2.04, 20000000000, 2.15,
                "2023-05-01 12:00:00"
            ),
            (
                "ethereum", "eth", "Ethereum", 3000.0, 360000000000, 2, 25000000000,
                3100.0, 2900.0, 50.0, 1.69, 6000000000, 1.69,
                "2023-05-01 12:00:00"
            )
        ]
        
        # Create silver table if it doesn't exist
        spark.sql("""
            CREATE TABLE IF NOT EXISTS test_catalog.silver.coingecko_cleaned (
                coin_id STRING,
                symbol STRING,
                name STRING,
                current_price_usd DOUBLE,
                market_cap_usd DOUBLE,
                market_rank INT,
                total_volume_24h DOUBLE,
                high_24h_usd DOUBLE,
                low_24h_usd DOUBLE,
                price_change_24h_usd DOUBLE,
                price_change_percentage_24h DOUBLE,
                market_cap_change_24h_usd DOUBLE,
                market_cap_change_percentage_24h DOUBLE,
                silver_ingestion_time STRING
            )
            USING DELTA
        """)
        
        # Insert test data
        silver_schema = spark.table("test_catalog.silver.coingecko_cleaned").schema
        df = spark.createDataFrame(silver_data, silver_schema)
        df.write.format("delta").mode("append").saveAsTable("test_catalog.silver.coingecko_cleaned")
        
        # Run the gold aggregation
        with patch('notebooks.gold.aggregate_metrics.spark', spark):
            gold_main()
        
        # Verify the metrics were calculated correctly
        metrics = spark.table("test_catalog.gold.crypto_market_metrics")
        assert metrics.count() == 1
        
        # Check some aggregated values
        row = metrics.first()
        assert row["total_coins"] == 2
        assert row["total_market_cap_usd"] == 1310000000000.0  # 950B + 360B
        assert row["coins_positive_24h"] == 2
        
        # Verify top performers table
        top_performers = spark.table("test_catalog.gold.crypto_top_performers")
        assert top_performers.count() == 2
        assert [r.coin_id for r in top_performers.orderBy("market_rank").select("coin_id").collect()] == ["bitcoin", "ethereum"]
