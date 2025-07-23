"""Pytest configuration and fixtures for the CryptoStreamX test suite."""
import pytest
from pyspark.sql import SparkSession
import os
import shutil
from pathlib import Path

# Set environment variables for testing
os.environ["DATABRICKS_HOST"] = "http://localhost:8080"
os.environ["DATABRICKS_TOKEN"] = "test_token"

@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    """
    spark = (
        SparkSession.builder
        .appName("CryptoStreamX Tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    # Set log level to WARN to reduce console output during tests
    spark.sparkContext.setLogLevel("WARN")
    
    # Create test database if it doesn't exist
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    
    yield spark
    
    # Clean up after tests
    spark.sql("DROP DATABASE IF EXISTS test_db CASCADE")
    spark.stop()
    
    # Remove any test directories
    test_dirs = ["spark-warehouse", "metastore_db", "derby.log"]
    for d in test_dirs:
        if os.path.exists(d):
            if os.path.isdir(d):
                shutil.rmtree(d)
            else:
                os.remove(d)

@pytest.fixture
def test_data_dir(tmp_path):
    """
    Create a temporary directory for test data.
    """
    test_dir = tmp_path / "test_data"
    test_dir.mkdir()
    return str(test_dir)

@pytest.fixture
def sample_market_data():
    """
    Sample market data for testing.
    """
    return [
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
            "ath": 69000.0,
            "ath_change_percentage": -27.54,
            "ath_date": "2021-11-10T14:24:11.849Z",
            "atl": 67.81,
            "atl_change_percentage": 73630.0,
            "atl_date": "2013-07-06T00:00:00.000Z",
            "last_updated": "2023-05-01T12:00:00.000Z"
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 3000.0,
            "market_cap": 360000000000,
            "market_cap_rank": 2,
            "total_volume": 25000000000,
            "high_24h": 3100.0,
            "low_24h": 2900.0,
            "price_change_24h": 50.0,
            "price_change_percentage_24h": 1.69,
            "market_cap_change_24h": 6000000000,
            "market_cap_change_percentage_24h": 1.69,
            "circulating_supply": 120000000.0,
            "total_supply": 120000000.0,
            "ath": 4865.0,
            "ath_change_percentage": -38.33,
            "ath_date": "2021-11-10T14:24:19.604Z",
            "atl": 0.432979,
            "atl_change_percentage": 692900.0,
            "atl_date": "2015-10-20T00:00:00.000Z",
            "last_updated": "2023-05-01T12:00:00.000Z"
        }
    ]
