"""
Tiingo API provider with rate limiting
"""

from typing import List, Dict, Any
from .base import BaseProvider
from utils.rate_limiter_advanced import AdvancedRateLimiter

class TiingoProvider(BaseProvider):
    """Tiingo with rate limit compliance (1000/day)"""
    
    def __init__(self, api_key: str):
        super().__init__(api_key, "Tiingo")
        self.base_url = "https://api.tiingo.com/tiingo/daily"
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
            
            url = f"{self.base_url}/{symbol}"
            params = {'token': self.api_key}
            response = self._make_request(url, params)
            
            if response:
                all_data.append({'symbol': symbol, 'data': response})
                self.rate_limiter.record_call(self.name)
            
            if (idx + 1) % 10 == 0:
                stats = self.rate_limiter.get_stats(self.name)
                print(f"[INFO] {self.name}: {idx + 1}/{len(symbols)} | {stats}")
        
        return {'data': all_data, 'provider': 'tiingo'}
    
    def normalize_data(self, raw_data: Dict) -> List[Dict]:
        """Normalize Tiingo data"""
        normalized = []
        for item in raw_data.get('data', []):
            if 'data' in item and len(item['data']) > 0:
                latest = item['data']
                normalized.append({
                    'symbol': item.get('symbol'),
                    'date': latest.get('date', ''),
                    'open': latest.get('open'),
                    'high': latest.get('high'),
                    'low': latest.get('low'),
                    'close': latest.get('close'),
                    'volume': latest.get('volume'),
                    'provider': 'tiingo'
                })
        return normalized

