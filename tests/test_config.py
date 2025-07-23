"""Tests for the configuration module."""
import os
import pytest
from src.utils.config import Config, validate_environment

class TestConfig:
    """Test cases for the Config class."""
    
    def test_default_config(self):
        """Test that the default configuration is set correctly."""
        config = Config()
        
        # Check default values
        assert config.databricks_host == "https://community.cloud.databricks.com"
        assert config.catalog_name == "datafusionx_catalog"
        assert config.bronze_schema == "bronze"
        assert config.silver_schema == "silver"
        assert config.gold_schema == "gold"
        
        # Check CoinGecko API defaults
        assert config.coingecko_params["vs_currency"] == "usd"
        assert config.coingecko_params["per_page"] == 100
        
    def test_environment_override(self, monkeypatch):
        """Test that environment variables override defaults."""
        # Set environment variables
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test_token_123")
        
        config = Config()
        
        # Check that environment variables take precedence
        assert config.databricks_host == "https://test.databricks.com"
        assert config.databricks_token == "test_token_123"
    
    def test_get_full_table_name(self):
        """Test the get_full_table_name helper function."""
        from src.utils.config import get_full_table_name
        
        # Test with default catalog
        table_name = get_full_table_name("bronze", "test_table")
        assert table_name == "datafusionx_catalog.bronze.test_table"
        
        # Test with custom catalog
        table_name = get_full_table_name("silver", "test_table", "custom_catalog")
        assert table_name == "custom_catalog.silver.test_table"


class TestEnvironmentValidation:
    """Test cases for environment validation."""
    
    def test_validate_environment_success(self, monkeypatch):
        """Test successful environment validation."""
        # Set required environment variables
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test_token_123")
        
        # Validation should pass
        assert validate_environment() is True
    
    def test_validate_environment_missing_vars(self, monkeypatch, capsys):
        """Test environment validation with missing variables."""
        # Clear all environment variables
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        
        # Validation should fail
        assert validate_environment() is False
        
        # Check error message
        captured = capsys.readouterr()
        assert "Missing required environment variables" in captured.out
        assert "DATABRICKS_HOST" in captured.out
        assert "DATABRICKS_TOKEN" in captured.out
    
    def test_validate_environment_partial_vars(self, monkeypatch, capsys):
        """Test environment validation with some missing variables."""
        # Set only one required variable
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        
        # Validation should fail
        assert validate_environment() is False
        
        # Check error message
        captured = capsys.readouterr()
        assert "Missing required environment variables" in captured.out
        assert "DATABRICKS_TOKEN" in captured.out
        assert "DATABRICKS_HOST" not in captured.out  # This one is set, shouldn't be in error
