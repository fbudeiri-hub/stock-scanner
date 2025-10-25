"""
Base provider class for all API data sources
"""

import requests
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import time

class BaseProvider(ABC):
    """Abstract base class for all stock data providers"""
    
    def __init__(self, api_key: str, name: str):
        self.api_key = api_key
        self.name = name
        self.session = requests.Session()
        self.max_retries = 3
        self.retry_delay = 1
        
    @abstractmethod
    def fetch_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Fetch stock data for given symbols"""
        pass
    
    def _make_request(self, url: str, params: Dict = None, timeout: int = 30):
        """Make HTTP request with retry logic"""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait = self.retry_delay ** attempt
                    print(f"[WARNING] {self.name}: Request failed. Retry in {wait}s...")
                    time.sleep(wait)
                else:
                    print(f"[ERROR] {self.name}: Request failed: {e}")
                    return None
        return None
    
    def normalize_data(self, raw_data: Dict) -> List[Dict]:
        """Normalize provider data to standard format"""
        return raw_data
    
    def close(self):
        """Clean up resources"""
        self.session.close()
