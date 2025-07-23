"""Tests for the API client module."""
import json
import pytest
import responses
from unittest.mock import patch, MagicMock
from src.utils.api_client import APIClient, CoinGeckoClient

class TestAPIClient:
    """Test cases for the base APIClient class."""
    
    @pytest.fixture
    def mock_response(self):
        """Create a mock response for testing."""
        return {
            "key1": "value1",
            "key2": ["item1", "item2"],
            "nested": {"a": 1, "b": 2}
        }
    
    @responses.activate
    def test_get_success(self, mock_response):
        """Test successful GET request."""
        # Mock the API response
        test_url = "https://api.example.com/test"
        responses.add(
            responses.GET,
            test_url,
            json=mock_response,
            status=200
        )
        
        # Make the request
        client = APIClient("https://api.example.com")
        response = client.get("test")
        
        # Verify the response
        assert response == mock_response
        assert len(responses.calls) == 1
        assert responses.calls[0].request.url == test_url
    
    @responses.activate
    def test_get_with_params(self):
        """Test GET request with query parameters."""
        test_url = "https://api.example.com/test"
        responses.add(
            responses.GET,
            test_url + "?param1=value1&param2=42",
            json={"status": "success"},
            status=200
        )
        
        client = APIClient("https://api.example.com")
        response = client.get(
            "test",
            params={"param1": "value1", "param2": 42}
        )
        
        assert response == {"status": "success"}
    
    @responses.activate
    def test_get_retry_on_failure(self, caplog):
        """Test that the client retries on failure."""
        test_url = "https://api.example.com/test"
        
        # First request fails with 500, second succeeds
        responses.add(
            responses.GET,
            test_url,
            json={"error": "Internal Server Error"},
            status=500
        )
        responses.add(
            responses.GET,
            test_url,
            json={"status": "success"},
            status=200
        )
        
        client = APIClient("https://api.example.com", max_retries=3, backoff_factor=0.1)
        response = client.get("test")
        
        assert response == {"status": "success"}
        assert len(responses.calls) == 2
        assert "Retrying" in caplog.text


class TestCoinGeckoClient:
    """Test cases for the CoinGeckoClient class."""
    
    @pytest.fixture
    def coingecko_client(self):
        """Create a CoinGecko client for testing."""
        return CoinGeckoClient(api_key="test_api_key")
    
    @responses.activate
    def test_get_market_data(self, coingecko_client):
        """Test getting market data from CoinGecko."""
        # Mock the API response
        test_data = [
            {"id": "bitcoin", "symbol": "btc", "current_price": 50000},
            {"id": "ethereum", "symbol": "eth", "current_price": 3000}
        ]
        
        responses.add(
            responses.GET,
            "https://api.coingecko.com/api/v3/coins/markets",
            json=test_data,
            status=200,
            match=[
                responses.matchers.query_param_matcher({
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": "10",
                    "page": "1",
                    "sparkline": "false"
                })
            ]
        )
        
        # Make the request
        result = coingecko_client.get_market_data(per_page=10)
        
        # Verify the request and response
        assert result == test_data
        assert len(responses.calls) == 1
        assert "x-cg-pro-api-key" in responses.calls[0].request.headers
        assert responses.calls[0].request.headers["x-cg-pro-api-key"] == "test_api_key"
    
    @responses.activate
    def test_get_coin_market_chart(self, coingecko_client):
        """Test getting market chart data for a coin."""
        # Mock the API response
        test_data = {
            "prices": [[1614556800000, 50000], [1614643200000, 52000]],
            "market_caps": [[1614556800000, 950000000000], [1614643200000, 960000000000]],
            "total_volumes": [[1614556800000, 30000000000], [1614643200000, 31000000000]]
        }
        
        responses.add(
            responses.GET,
            "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart",
            json=test_data,
            status=200,
            match=[
                responses.matchers.query_param_matcher({
                    "vs_currency": "usd",
                    "days": "7"
                })
            ]
        )
        
        # Make the request
        result = coingecko_client.get_coin_market_chart("bitcoin", days=7)
        
        # Verify the response
        assert result == test_data
    
    @responses.activate
    def test_api_error_handling(self, coingecko_client, caplog):
        """Test error handling for API requests."""
        # Mock a 429 Too Many Requests response
        responses.add(
            responses.GET,
            "https://api.coingecko.com/api/v3/coins/markets",
            json={"error": "Too many requests"},
            status=429
        )
        
        # The client should raise an exception after retries
        with pytest.raises(Exception) as excinfo:
            coingecko_client.get_market_data()
        
        assert "Too many requests" in str(excinfo.value)
        assert "Retrying" in caplog.text
    
    def test_default_parameters(self):
        """Test that default parameters are set correctly."""
        client = CoinGeckoClient()
        assert client.api_key is None
        assert client.base_url == "https://api.coingecko.com/api/v3"
        
        # Verify default headers don't include API key when not provided
        assert "x-cg-pro-api-key" not in client.session.headers
        
        # Create client with API key
        client_with_key = CoinGeckoClient(api_key="test_key")
        assert client_with_key.session.headers["x-cg-pro-api-key"] == "test_key"
