"""
Finnhub API provider implementation
"""

from typing import List, Dict, Any
from .base import BaseProvider
import time
from datetime import datetime

class FinnhubProvider(BaseProvider):
    """Finnhub API provider"""
    
    def __init__(self, api_key: str):
        super().__init__(api_key, "Finnhub")
        self.base_url = "https://finnhub.io/api/v1/quote"
        self.rate_limit_delay = 0.02
        
    def fetch_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Fetch data from Finnhub (individual calls per symbol)"""
        if not symbols:
            return {'data': []}
            
        all_data = []
        
        print(f"[INFO] {self.name}: Fetching {len(symbols)} symbols...")
        
        for symbol in symbols:
            params = {'symbol': symbol, 'token': self.api_key}
            
            response = self._make_request(self.base_url, params)
            
            if response and 'c' in response:
                all_data.append({
                    'symbol': symbol,
                    'quote': response
                })
            
            time.sleep(self.rate_limit_delay)
            
        return {'data': all_data, 'provider': 'finnhub'}
    
    def normalize_data(self, raw_data: Dict) -> List[Dict]:
        """Convert Finnhub format to standard format"""
        normalized = []
        today = datetime.now().strftime('%Y-%m-%d')
        
        for item in raw_data.get('data', []):
            quote = item.get('quote', {})
            normalized.append({
                'symbol': item.get('symbol'),
                'date': today,
                'open': quote.get('o'),
                'high': quote.get('h'),
                'low': quote.get('l'),
                'close': quote.get('c'),
                'volume': quote.get('v'),
                'provider': 'finnhub'
            })
        return normalized
