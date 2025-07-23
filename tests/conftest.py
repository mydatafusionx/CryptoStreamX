"""Pytest configuration and fixtures for the CryptoStreamX test suite."""
import pytest
from pyspark.sql import SparkSession
import os
import shutil
import sys
import pytest
from unittest.mock import MagicMock, patch

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock Spark and Databricks imports
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()
sys.modules['pyspark.sql.types'] = MagicMock()
sys.modules['pyspark.sql.utils'] = MagicMock()
sys.modules['delta'] = MagicMock()
sys.modules['delta.tables'] = MagicMock()

# Mock Databricks specific modules
sys.modules['pyspark.dbutils'] = MagicMock()
sys.modules['pyspark.dbutils'] = MagicMock()
sys.modules['pyspark.dbutils'] = MagicMock()

# Import the mocked modules
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame

# Mock the Spark session
class MockSparkSession:
    def __init__(self):
        self.sql = MagicMock()
        self.createDataFrame = MagicMock()
        self.stop = MagicMock()
        self.sparkContext = MagicMock()
        self.sparkContext.setLogLevel = MagicMock()
        
        # Mock SQL context
        self._jsparkSession = MagicMock()
        self._jsparkSession.sharedState = MagicMock()
        self._jsparkSession.sharedState.catalog = MagicMock()
        
        # Mock the table method to return a mock DataFrame
        self.table = MagicMock()
        
        # Mock the catalog
        self.catalog = MagicMock()
        
        # Mock the SQL context
        self.sql = MagicMock(return_value=MagicMock())

# Mock the DataFrame class
class MockDataFrame(SparkDataFrame):
    def __init__(self, *args, **kwargs):
        self._jdf = MagicMock()
        self.sql_ctx = MagicMock()
        self._sc = MagicMock()
        self._is_streaming = False
        self.write = MagicMock()
        self.write.mode = MagicMock(return_value=self.write)
        self.write.format = MagicMock(return_value=self.write)
        self.write.option = MagicMock(return_value=self.write)
        self.write.saveAsTable = MagicMock()
        self.count = MagicMock(return_value=1)
        self.collect = MagicMock(return_value=[MagicMock(id="bitcoin")])

# Fixture for the mocked Spark session
@pytest.fixture(scope="session")
def spark():
    """Create a mocked SparkSession for testing."""
    # Mock the SparkSession.builder
    mock_builder = MagicMock()
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.side_effect = lambda *args, **kwargs: mock_builder
    mock_builder.enableHiveSupport.return_value = mock_builder
    
    # Create a mock Spark session
    mock_spark = MockSparkSession()
    mock_builder.getOrCreate.return_value = mock_spark
    
    # Patch the SparkSession.builder
    with patch('pyspark.sql.SparkSession.builder', mock_builder):
        yield mock_spark

# Fixture for test environment setup
@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch, tmp_path):
    """Set up test environment variables and configurations."""
    # Set test paths
    test_data_dir = tmp_path / "test_data"
    test_data_dir.mkdir()
    
    # Set environment variables for testing
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi1234567890abcdef1234567890ab")
    monkeypatch.setenv("DATABRICKS_CONFIG_FILE", "/tmp/databricks_config")
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "10.4.x-scala2.12")
    
    # Set Python environment
    monkeypatch.setenv("PYSPARK_PYTHON", "python3")
    monkeypatch.setenv("PYSPARK_DRIVER_PYTHON", "python3")
    
    # Set test-specific environment variables
    monkeypatch.setenv("CATALOG_NAME", "test_catalog")
    monkeypatch.setenv("BRONZE_SCHEMA", "bronze")
    monkeypatch.setenv("SILVER_SCHEMA", "silver")
    monkeypatch.setenv("GOLD_SCHEMA", "gold")
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
