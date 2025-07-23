"""Additional tests for the API client module."""
import pytest
import json
import responses
from unittest.mock import patch, MagicMock
from src.utils.api_client import APIClient, CoinGeckoClient

class TestAPIClientAdditional:
    """Additional test cases for the APIClient class."""
    
    @responses.activate
    def test_post_request(self):
        """Test sending a POST request."""
        # Setup test data
        test_url = "https://api.example.com/test"
        test_data = {"key": "value"}
        expected_response = {"status": "success"}
        
        # Mock the POST response
        responses.add(
            responses.POST,
            test_url,
            json=expected_response,
            status=200
        )
        
        # Initialize client and make request
        client = APIClient("https://api.example.com")
        response = client.post("test", json=test_data)
        
        # Verify request and response
        assert response == expected_response
        assert responses.calls[0].request.url == test_url
        assert json.loads(responses.calls[0].request.body) == test_data
    
    @responses.activate
    def test_put_request(self):
        """Test sending a PUT request."""
        # Setup test data
        test_url = "https://api.example.com/test/1"
        test_data = {"key": "updated_value"}
        expected_response = {"status": "updated"}
        
        # Mock the PUT response
        responses.add(
            responses.PUT,
            test_url,
            json=expected_response,
            status=200
        )
        
        # Initialize client and make request
        client = APIClient("https://api.example.com")
        response = client.put("test/1", json=test_data)
        
        # Verify request and response
        assert response == expected_response
        assert responses.calls[0].request.url == test_url
        assert json.loads(responses.calls[0].request.body) == test_data
    
    @responses.activate
    def test_delete_request(self):
        """Test sending a DELETE request."""
        # Setup test data
        test_url = "https://api.example.com/test/1"
        expected_response = {"status": "deleted"}
        
        # Mock the DELETE response
        responses.add(
            responses.DELETE,
            test_url,
            json=expected_response,
            status=200
        )
        
        # Initialize client and make request
        client = APIClient("https://api.example.com")
        response = client.delete("test/1")
        
        # Verify request and response
        assert response == expected_response
        assert responses.calls[0].request.url == test_url
    
    def test_initialization_with_headers(self):
        """Test initialization with custom headers."""
        custom_headers = {
            "Authorization": "Bearer token123",
            "X-Custom-Header": "value"
        }
        
        client = APIClient(
            base_url="https://api.example.com",
            headers=custom_headers
        )
        
        # Verify default headers are still present
        assert client.session.headers["Accept"] == "application/json"
        assert client.session.headers["Content-Type"] == "application/json"
        # Verify custom headers are added
        assert client.session.headers["Authorization"] == "Bearer token123"
        assert client.session.headers["X-Custom-Header"] == "value"


class TestCoinGeckoClientAdditional:
    """Additional test cases for the CoinGeckoClient class."""
    
    @responses.activate
    def test_get_coin_market_chart_without_interval(self):
        """Test getting historical market data without interval parameter."""
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
        
        client = CoinGeckoClient()
        result = client.get_coin_market_chart(
            coin_id="bitcoin",
            vs_currency='usd',
            days=7
        )
        
        assert result == test_data
        assert 'interval' not in responses.calls[0].request.params
    
    @responses.activate
    def test_get_market_data_with_ids(self):
        """Test getting market data with specific coin IDs."""
        test_data = [
            {"id": "bitcoin", "symbol": "btc", "current_price": 50000},
            {"id": "ethereum", "symbol": "eth", "current_price": 3000}
        ]
        
        responses.add(
            responses.GET,
            "https://api.coingecko.com/api/v3/coins/markets",
            json=test_data,
            status=200
        )
        
        client = CoinGeckoClient()
        result = client.get_market_data(
            vs_currency='usd',
            ids=["bitcoin", "ethereum"]
        )
        
        assert len(result) == 2
        assert responses.calls[0].request.params["ids"] == "bitcoin,ethereum"
    
    @responses.activate
    def test_get_market_data_with_sparkline(self):
        """Test getting market data with sparkline data."""
        test_data = [
            {"id": "bitcoin", "symbol": "btc", "current_price": 50000, "sparkline_in_7d": {"price": [48000, 49000, 50000]}}
        ]
        
        responses.add(
            responses.GET,
            "https://api.coingecko.com/api/v3/coins/markets",
            json=test_data,
            status=200
        )
        
        client = CoinGeckoClient()
        result = client.get_market_data(
            vs_currency='usd',
            sparkline=True
        )
        
        assert len(result) == 1
        assert responses.calls[0].request.params["sparkline"] == "true"
    
    def test_api_key_usage_in_headers(self):
        """Test that API key is properly set in headers."""
        with patch('src.utils.api_client.APIClient.__init__') as mock_init:
            # Setup mock to not actually initialize the real APIClient
            mock_init.return_value = None
            
            # Test with API key
            client = CoinGeckoClient(api_key="test_key")
            # Check that APIClient was initialized with the API key in headers
            mock_init.assert_called_once()
            call_args = mock_init.call_args[1]
            assert call_args['headers']['x-cg-pro-api-key'] == "test_key"
            
            # Reset mock for second test
            mock_init.reset_mock()
            
            # Test without API key
            client = CoinGeckoClient()
            # Check that APIClient was initialized without the API key
            mock_init.assert_called_once()
            call_args = mock_init.call_args[1]
            assert 'x-cg-pro-api-key' not in call_args.get('headers', {})
