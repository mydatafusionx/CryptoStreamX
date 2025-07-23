"""Configuration and utility functions for the CryptoStreamX project."""
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class Config:
    """Global configuration for the project."""
    # Databricks configuration
    databricks_host: str = os.getenv("DATABRICKS_HOST", "https://community.cloud.databricks.com")
    databricks_token: str = os.getenv("DATABRICKS_TOKEN", "")
    
    # API configuration
    coingecko_api_url: str = "https://api.coingecko.com/api/v3/coins/markets"
    coingecko_params: dict = None
    
    # Catalog and schema names
    catalog_name: str = "datafusionx_catalog"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    
    # Table names
    bronze_table: str = "coingecko_raw"
    silver_table: str = "coingecko_cleaned"
    gold_metrics_table: str = "crypto_market_metrics"
    gold_performers_table: str = "crypto_top_performers"
    
    def __post_init__(self):
        """Initialize default parameters after dataclass initialization."""
        self.coingecko_params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": False,
            "price_change_percentage": "24h,7d"
        }

# Global configuration instance
config = Config()

def get_full_table_name(schema: str, table: str) -> str:
    """Get fully qualified table name."""
    return f"{config.catalog_name}.{schema}.{table}"

def validate_environment() -> bool:
    """Validate that all required environment variables are set."""
    required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN"]
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"Error: Missing required environment variables: {', '.join(missing)}")
        print("Please set these variables in your environment or .env file")
        return False
    return True
