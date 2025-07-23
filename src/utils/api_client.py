"""API client for interacting with external services."""
import requests
from typing import Dict, Any, Optional
from datetime import datetime
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class APIClient:
    """Generic API client with retry logic and error handling."""
    
    def __init__(self, base_url: str, max_retries: int = 3, backoff_factor: float = 0.5, headers: Optional[Dict[str, str]] = None):
        """Initialize the API client.
        
        Args:
            base_url: Base URL for the API
            max_retries: Maximum number of retries for failed requests
            backoff_factor: Backoff factor for retries
            headers: Custom headers to include in all requests
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PUT", "DELETE"]
        )
        
        # Mount the retry strategy to http:// and https://
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        default_headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': 'CryptoStreamX/1.0'
        }
        
        # Update with any custom headers provided
        if headers:
            default_headers.update(headers)
            
        self.session.headers.update(default_headers)
    
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs) -> Dict[str, Any]:
        """Send a GET request to the API.
        
        Args:
            endpoint: API endpoint (e.g., '/coins/markets')
            params: Query parameters
            **kwargs: Additional arguments to pass to requests.get()
            
        Returns:
            Dict containing the JSON response
            
        Raises:
            requests.exceptions.RequestException: If the request fails after all retries
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"Sending GET request to {url}")
        
        try:
            response = self.session.get(url, params=params, **kwargs)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
            
    def post(self, endpoint: str, json: Optional[Dict[str, Any]] = None, **kwargs) -> Dict[str, Any]:
        """Send a POST request to the API.
        
        Args:
            endpoint: API endpoint
            json: JSON data to send in the request body
            **kwargs: Additional arguments to pass to requests.post()
            
        Returns:
            Dict containing the JSON response
            
        Raises:
            requests.exceptions.RequestException: If the request fails after all retries
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"Sending POST request to {url}")
        
        try:
            response = self.session.post(url, json=json, **kwargs)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
            
    def put(self, endpoint: str, json: Optional[Dict[str, Any]] = None, **kwargs) -> Dict[str, Any]:
        """Send a PUT request to the API.
        
        Args:
            endpoint: API endpoint
            json: JSON data to send in the request body
            **kwargs: Additional arguments to pass to requests.put()
            
        Returns:
            Dict containing the JSON response
            
        Raises:
            requests.exceptions.RequestException: If the request fails after all retries
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"Sending PUT request to {url}")
        
        try:
            response = self.session.put(url, json=json, **kwargs)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
            
    def delete(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Send a DELETE request to the API.
        
        Args:
            endpoint: API endpoint
            **kwargs: Additional arguments to pass to requests.delete()
            
        Returns:
            Dict containing the JSON response
            
        Raises:
            requests.exceptions.RequestException: If the request fails after all retries
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"Sending DELETE request to {url}")
        
        try:
            response = self.session.delete(url, **kwargs)
            response.raise_for_status()
            
            # Some DELETE endpoints might not return content
            if response.status_code == 204 or not response.content:
                return {"status": "success", "message": "Resource deleted successfully"}
                
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise


class CoinGeckoClient(APIClient):
    """Client for interacting with the CoinGecko API."""
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the CoinGecko client.
        
        Args:
            api_key: Optional API key for authenticated requests
        """
        super().__init__("https://api.coingecko.com/api/v3")
        self.api_key = api_key
        
        if api_key:
            self.session.headers.update({'x-cg-pro-api-key': api_key})
    
    def get_market_data(
        self, 
        vs_currency: str = 'usd', 
        ids: Optional[list] = None,
        order: str = 'market_cap_desc',
        per_page: int = 100,
        page: int = 1,
        sparkline: bool = False,
        price_change_percentage: str = '24h,7d',
        category: Optional[str] = None
    ) -> list:
        """Get market data for cryptocurrencies.
        
        Args:
            vs_currency: The target currency of market data (usd, eur, jpy, etc.)
            ids: List of coin ids to filter by
            order: Sort results by field (market_cap_desc, volume_asc, etc.)
            per_page: Total results per page
            page: Page number
            sparkline: Include sparkline 7 days data
            price_change_percentage: Include price change percentage in specified time period
            category: Filter by coin category
            
        Returns:
            List of market data for cryptocurrencies
        """
        params = {
            'vs_currency': vs_currency,
            'order': order,
            'per_page': per_page,
            'page': page,
            'sparkline': 'true' if sparkline else 'false',
            'price_change_percentage': price_change_percentage,
        }
        
        if ids:
            params['ids'] = ','.join(ids)
            
        if category:
            params['category'] = category
        
        return self.get('coins/markets', params=params)
    
    def get_coin_market_chart(
        self, 
        coin_id: str, 
        vs_currency: str = 'usd', 
        days: int = 7,
        interval: Optional[str] = None
    ) -> dict:
        """Get historical market data for a cryptocurrency.
        
        Args:
            coin_id: The coin id (e.g., 'bitcoin')
            vs_currency: The target currency of market data
            days: Data up to number of days ago (1/7/14/30/90/180/365/max)
            interval: Data interval. Possible value: daily
            
        Returns:
            Dictionary with price, market_caps, and total_volumes
        """
        params = {
            'vs_currency': vs_currency,
            'days': days
        }
        
        if interval:
            params['interval'] = interval
            
        return self.get(f'coins/{coin_id}/market_chart', params=params)


# Example usage
if __name__ == "__main__":
    import json
    
    # Initialize the client
    client = CoinGeckoClient()
    
    # Get market data for top 10 cryptocurrencies
    data = client.get_market_data(per_page=10)
    print(json.dumps(data, indent=2))
