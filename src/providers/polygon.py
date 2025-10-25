"""
Polygon.io API provider with rate limiting
"""

from typing import List, Dict, Any
from .base import BaseProvider
from utils.rate_limiter_advanced import AdvancedRateLimiter

class PolygonProvider(BaseProvider):
    """Polygon with rate limit compliance (500/day)"""
    
    def __init__(self, api_key: str):
        super().__init__(api_key, "Polygon")
        self.base_url = "https://api.polygon.io/v1/open-close"
        self.rate_limiter = AdvancedRateLimiter()
        
    def fetch_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Fetch with rate limiting"""
        if not symbols:
            return {'data': []}
        
        all_data = []
        
        for idx, symbol in enumerate(symbols):
            if not self.rate_limiter.wait_until_ready(self.name):
                print(f"[WARNING] {self.name}: Rate limit exceeded at {idx}/{len(symbols)}")
                break
            
            params = {
                'stockticker': symbol,
                'adjusted': 'true',
                'apiKey': self.api_key
            }
            
            response = self._make_request(self.base_url, params)
            
            if response and 'c' in response:
                all_data.append({'symbol': symbol, 'data': response})
                self.rate_limiter.record_call(self.name)
            
            if (idx + 1) % 10 == 0:
                stats = self.rate_limiter.get_stats(self.name)
                print(f"[INFO] {self.name}: {idx + 1}/{len(symbols)} | {stats}")
        
        return {'data': all_data, 'provider': 'polygon'}
    
    def normalize_data(self, raw_data: Dict) -> List[Dict]:
        """Normalize Polygon data"""
        normalized = []
        for item in raw_data.get('data', []):
            data = item.get('data', {})
            normalized.append({
                'symbol': item.get('symbol'),
                'date': item.get('from', ''),
                'open': data.get('o'),
                'high': data.get('h'),
                'low': data.get('l'),
                'close': data.get('c'),
                'volume': data.get('v'),
                'provider': 'polygon'
            })
        return normalized

