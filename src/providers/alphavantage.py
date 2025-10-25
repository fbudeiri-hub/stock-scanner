"""
Alpha Vantage API provider with STRICT rate limiting
HARDEST LIMIT: 5 requests per minute
"""

from typing import List, Dict, Any
from .base import BaseProvider
from utils.rate_limiter_advanced import AdvancedRateLimiter

class AlphaVantageProvider(BaseProvider):
    """Alpha Vantage - VERY strict 5/min limit"""
    
    def __init__(self, api_key: str):
        super().__init__(api_key, "Alpha Vantage")
        self.base_url = "https://www.alphavantage.co/query"
        self.rate_limiter = AdvancedRateLimiter()
        
    def fetch_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Fetch with STRICT rate limiting"""
        if not symbols:
            return {'data': []}
        
        all_data = []
        max_to_fetch = min(10, len(symbols))  # Conservative: only 10 per day
        
        print(f"[INFO] {self.name}: STRICT LIMIT - fetching only {max_to_fetch}/{len(symbols)}")
        
        for idx, symbol in enumerate(symbols[:max_to_fetch]):
            if not self.rate_limiter.wait_until_ready(self.name):
                print(f"[WARNING] {self.name}: Rate limit STRICT limit exceeded.")
                break
            
            params = {'function': 'GLOBAL_QUOTE', 'symbol': symbol, 'apikey': self.api_key}
            response = self._make_request(self.base_url, params)
            
            if response and 'Global Quote' in response:
                all_data.append({'symbol': symbol, 'quote': response['Global Quote']})
                self.rate_limiter.record_call(self.name)
            
            if (idx + 1) % 5 == 0:
                stats = self.rate_limiter.get_stats(self.name)
                print(f"[INFO] {self.name}: {idx + 1}/{max_to_fetch} | {stats}")
        
        return {'data': all_data, 'provider': 'alphavantage'}
    
    def normalize_data(self, raw_data: Dict) -> List[Dict]:
        """Normalize Alpha Vantage data"""
        normalized = []
        for item in raw_data.get('data', []):
            quote = item.get('quote', {})
            normalized.append({
                'symbol': item.get('symbol'),
                'date': '',
                'open': quote.get('02. open'),
                'high': quote.get('03. high'),
                'low': quote.get('04. low'),
                'close': quote.get('05. price'),
                'volume': quote.get('06. volume'),
                'provider': 'alphavantage'
            })
        return normalized
