"""Tests for the main pipeline functionality."""
import os
import sys
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

# Skip if not running in Databricks
pytestmark = pytest.mark.skipif(
    'DATABRICKS_RUNTIME_VERSION' not in os.environ,
    reason="Test only runs in Databricks environment"
)

# Sample test data
SAMPLE_COIN_DATA = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 50000.0,
        "market_cap": 1000000000000,
        "market_cap_rank": 1,
        "total_volume": 50000000000,
        "high_24h": 51000.0,
        "low_24h": 49000.0,
        "price_change_24h": 1000.0,
        "price_change_percentage_24h": 2.0,
        "market_cap_change_24h": 20000000000,
        "market_cap_change_percentage_24h": 2.0,
        "circulating_supply": 18750000.0,
        "total_supply": 21000000.0,
        "max_supply": 21000000.0,
        "ath": 69000.0,
        "ath_change_percentage": -27.54,
        "ath_date": "2021-11-10T14:24:11.849Z",
        "atl": 67.81,
        "atl_change_percentage": 73630.37,
        "atl_date": "2013-07-06T00:00:00.000Z",
        "roi": None,
        "last_updated": "2023-01-01T00:00:00.000Z"
    }
]

@pytest.fixture
def mock_environment(monkeypatch):
    """Set up mocks for the Databricks environment."""
    # Mock the config
    mock_config = MagicMock()
    mock_config.catalog_name = os.getenv("CATALOG_NAME")
    mock_config.bronze_schema = os.getenv("BRONZE_SCHEMA")
    mock_config.silver_schema = os.getenv("SILVER_SCHEMA")
    mock_config.gold_schema = os.getenv("GOLD_SCHEMA")
    
    # Mock the Spark session
    mock_spark = MagicMock()
    
    # Create a mock DataFrame
    mock_df = MagicMock()
    mock_df.count.return_value = len(SAMPLE_COIN_DATA)
    mock_df.collect.return_value = [MagicMock(id=coin["id"]) for coin in SAMPLE_COIN_DATA]
    mock_spark.createDataFrame.return_value = mock_df
    
    # Mock the DeltaTableManager
    mock_delta_manager = MagicMock()
    mock_delta_manager.write_data.return_value = True
    
    # Mock the CoinGecko client
    mock_coingecko = MagicMock()
    mock_coingecko.get_market_data.return_value = SAMPLE_COIN_DATA
    
    # Apply the mocks
    monkeypatch.setattr('notebooks.bronze.ingest_coingecko.DeltaTableManager', 
                      lambda *args, **kwargs: mock_delta_manager)
    monkeypatch.setattr('notebooks.bronze.ingest_coingecko.spark', mock_spark)
    monkeypatch.setattr('notebooks.bronze.ingest_coingecko.coingecko', mock_coingecko)
    monkeypatch.setattr('notebooks.bronze.ingest_coingecko.config', mock_config)
    
    # Mock dbutils if not available
    if 'dbutils' not in globals():
        mock_dbutils = MagicMock()
        mock_widgets = MagicMock()
        mock_widgets.get.return_value = f"test_run_{int(datetime.now().timestamp())}"
        mock_dbutils.widgets = mock_widgets
        monkeypatch.setitem(globals(), 'dbutils', mock_dbutils)
    
    # Mock the current_timestamp function
    mock_timestamp = MagicMock()
    mock_timestamp.cast.return_value = 1000
    monkeypatch.setattr('notebooks.bronze.ingest_coingecko.current_timestamp', 
                      lambda: mock_timestamp)
    
    return {
        'spark': mock_spark,
        'delta_manager': mock_delta_manager,
        'coingecko': mock_coingecko,
        'config': mock_config,
        'test_df': mock_df,
        'dbutils': mock_dbutils if 'dbutils' not in globals() else globals()['dbutils']
    }

def test_bronze_ingestion_process_data(mock_environment):
    """Test the bronze layer data processing."""
    # Import inside test to ensure mocks are in place
    from notebooks.bronze.ingest_coingecko import process_and_save_data
    
    # Run the processing function
    result = process_and_save_data(SAMPLE_COIN_DATA)
    
    # Verify the result structure
    assert isinstance(result, dict)
    assert "pipeline_status" in result
    assert "records_processed" in result
    assert result["records_processed"] == len(SAMPLE_COIN_DATA)
    
    # Verify the mock interactions
    mock_environment['coingecko'].get_market_data.assert_not_called()
    mock_environment['spark'].createDataFrame.assert_called_once()
    
    # Verify the Delta manager was called with the correct parameters
    assert mock_environment['delta_manager'].write_data.call_count == 1
    call_args = mock_environment['delta_manager'].write_data.call_args[1]
    assert call_args['table_name'] == "coingecko_raw"
    assert call_args['mode'] == "append"
    assert call_args['merge_schema'] == True

def test_bronze_ingestion_main(mock_environment):
    """Test the main bronze ingestion function."""
    # Import inside test to ensure mocks are in place
    from notebooks.bronze.ingest_coingecko import main as bronze_main
    
    # Run the main function
    result = bronze_main()
    
    # Verify the result structure
    assert isinstance(result, dict)
    assert "pipeline_status" in result
    assert "records_processed" in result
    
    # Verify the CoinGecko API was called
    mock_environment['coingecko'].get_market_data.assert_called_once()
    
    # Verify the Delta manager was called
    assert mock_environment['delta_manager'].write_data.call_count == 1
