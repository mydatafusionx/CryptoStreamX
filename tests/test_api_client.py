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
        
    @pytest.fixture
    def api_client(self):
        """Create an APIClient instance for testing."""
        return APIClient("https://api.example.com")
    
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
        
    @responses.activate
    def test_post_request(self, api_client, mock_response):
        """Test POST request with JSON data."""
        test_url = "https://api.example.com/test"
        test_data = {"key": "value"}
        
        responses.add(
            responses.POST,
            test_url,
            json=mock_response,
            status=201
        )
        
        response = api_client.post("test", json=test_data)
        
        assert response == mock_response
        assert len(responses.calls) == 1
        assert responses.calls[0].request.method == "POST"
        assert json.loads(responses.calls[0].request.body) == test_data
    
    @responses.activate
    def test_put_request(self, api_client):
        """Test PUT request with JSON data."""
        test_url = "https://api.example.com/test/1"
        test_data = {"name": "updated"}
        
        responses.add(
            responses.PUT,
            test_url,
            json={"status": "updated"},
            status=200
        )
        
        response = api_client.put("test/1", json=test_data)
        
        assert response == {"status": "updated"}
        assert responses.calls[0].request.method == "PUT"
        
    @responses.activate
    def test_delete_request(self, api_client):
        """Test DELETE request."""
        test_url = "https://api.example.com/test/1"
        
        responses.add(
            responses.DELETE,
            test_url,
            json={"status": "deleted"},
            status=204
        )
        
        response = api_client.delete("test/1")
        
        assert response == {"status": "deleted"}
        assert responses.calls[0].request.method == "DELETE"
        
    @responses.activate
    def test_request_headers(self, api_client):
        """Test that custom headers are included in requests."""
        test_url = "https://api.example.com/test"
        custom_headers = {"X-Custom-Header": "test-value"}
        
        responses.add(
            responses.GET,
            test_url,
            json={"status": "success"},
            status=200
        )
        
        api_client.session.headers.update(custom_headers)
        api_client.get("test")
        
        assert responses.calls[0].request.headers["X-Custom-Header"] == "test-value"
    
    @responses.activate
    def test_http_error_handling(self, api_client):
        """Test handling of HTTP errors."""
        test_url = "https://api.example.com/error"
        
        responses.add(
            responses.GET,
            test_url,
            json={"error": "Not Found"},
            status=404
        )
        
        response = api_client.get("error")
        
        assert response == {"error": "Not Found"}
        assert responses.calls[0].response.status_code == 404
        
    @responses.activate
    def test_request_exception_handling(self, api_client):
        """Test handling of request exceptions."""
        test_url = "https://api.example.com/connection-error"
        
        # Simulate a connection error
        responses.add(
            responses.GET,
            test_url,
            body=requests.exceptions.RequestException("Connection error")
        )
        
        with pytest.raises(requests.exceptions.RequestException):
            api_client.get("connection-error")
    
    def test_initialization_with_trailing_slash(self):
        """Test that trailing slashes are handled correctly in base URL."""
        client = APIClient("https://api.example.com/")
        assert client.base_url == "https://api.example.com"
        
        client = APIClient("https://api.example.com/v1/")
        assert client.base_url == "https://api.example.com/v1"
    
    @responses.activate
    def test_get_with_custom_headers(self, api_client):
        """Test GET request with custom headers."""
        test_url = "https://api.example.com/headers"
        
        def request_callback(request):
            # Return the request headers in the response
            return (200, {}, json.dumps(dict(request.headers)))
            
        responses.add_callback(
            responses.GET,
            test_url,
            callback=request_callback
        )
        
        custom_headers = {"X-Custom-Header": "test"}
        response = api_client.get("headers", headers=custom_headers)
        
        # Verify the custom header was sent
        assert response["X-Custom-Header"] == "test"
        # Verify default headers are still present
        assert response["Accept"] == "application/json"


class TestCoinGeckoClient:
    """Test cases for the CoinGeckoClient class."""
    
    @pytest.fixture
    def coingecko_client(self):
        """Create a CoinGecko client for testing."""
        return CoinGeckoClient(api_key="test_api_key")
        
    @pytest.fixture
    def market_data(self):
        """Sample market data for testing."""
        return [
            {
                "id": "bitcoin",
                "symbol": "btc",
                "name": "Bitcoin",
                "current_price": 50000,
                "market_cap": 950000000000,
                "price_change_percentage_24h": 2.5
            },
            {
                "id": "ethereum",
                "symbol": "eth",
                "name": "Ethereum",
                "current_price": 3000,
                "market_cap": 350000000000,
                "price_change_percentage_24h": -1.2
            }
        ]
    
    @responses.activate
    def test_get_market_data(self, coingecko_client, market_data):
        """Test getting market data from CoinGecko."""
        # Mock the API response
        test_data = market_data
        
        responses.add(
            responses.GET,
            "https://api.coingecko.com/api/v3/coins/markets",
            json=test_data,
            status=200
        )
        
        # Test with default parameters
        result = coingecko_client.get_market_data()
        
        assert len(result) == 2
        assert result[0]["id"] == "bitcoin"
        assert result[1]["symbol"] == "eth"
        
        # Verify API key header was set
        assert responses.calls[0].request.headers["x-cg-pro-api-key"] == "test_api_key"
        
    @responses.activate
    def test_get_coin_market_chart(self, coingecko_client):
        """Test getting historical market data for a coin."""
        test_data = {
            "prices": [[1638316800000, 50000], [1638403200000, 51000]],
            "market_caps": [[1638316800000, 950000000000], [1638403200000, 960000000000]],
            "total_volumes": [[1638316800000, 30000000000], [1638403200000, 31000000000]]
        }
        
        responses.add(
            responses.GET,
            "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart",
            json=test_data,
            status=200
        )
        
        result = coingecko_client.get_coin_market_chart("bitcoin", days=7)
        
        assert "prices" in result
        assert "market_caps" in result
        assert "total_volumes" in result
        assert len(result["prices"]) == 2
        
    @responses.activate
    def test_api_key_usage(self, market_data):
        """Test that API key is used when provided."""
        # Client without API key
        client = CoinGeckoClient()
        
        responses.add(
            responses.GET,
            "https://api.coingecko.com/api/v3/coins/markets",
            json=market_data,
            status=200
        )
        
        client.get_market_data()
        assert "x-cg-pro-api-key" not in responses.calls[0].request.headers
        
        # Client with API key
        client_with_key = CoinGeckoClient(api_key="test_key")
        
        responses.add(
            responses.GET,
            "https://api.coingecko.com/api/v3/coins/markets",
            json=market_data,
            status=200
        )
        
        client_with_key.get_market_data()
        assert responses.calls[1].request.headers["x-cg-pro-api-key"] == "test_key"
    
    @responses.activate
    def test_get_market_data_with_category(self, coingecko_client, market_data):
        """Test getting market data filtered by category."""
        responses.add(
            responses.GET,
            "https://api.coingecko.com/api/v3/coins/markets",
            json=market_data,
            status=200
        )
        
        result = coingecko_client.get_market_data(
            vs_currency='usd',
            category='decentralized-finance-defi',
            per_page=5,
            page=1
        )
        
        assert len(result) == 2
        assert 'decentralized-finance-defi' in responses.calls[0].request.params['category']
    
    @responses.activate
    def test_get_coin_market_chart_with_interval(self, coingecko_client):
        """Test getting historical market data with interval parameter."""
        test_data = {
            "prices": [[1638316800000, 50000], [1638403200000, 51000]],
            "market_caps": [[1638316800000, 950000000000], [1638403200000, 960000000000]],
            "total_volumes": [[1638316800000, 30000000000], [1638403200000, 31000000000]]
        }
        
        responses.add(
            responses.GET,
            "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart",
            json=test_data,
            status=200
        )
        
        result = coingecko_client.get_coin_market_chart(
            coin_id="bitcoin",
            vs_currency='eur',
            days=30,
            interval='daily'
        )
        
        assert 'prices' in result
        assert responses.calls[0].request.params['interval'] == 'daily'
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
