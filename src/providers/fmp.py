"""
FMP API provider with rate limiting
"""

from typing import List, Dict, Any
from .base import BaseProvider
from utils.rate_limiter_advanced import AdvancedRateLimiter
import time

class FMPProvider(BaseProvider):
    """FMP with rate limit compliance (250/day)"""
    
    def __init__(self, api_key: str):
        super().__init__(api_key, "FMP")
        self.base_url = "https://financialmodelingprep.com/api/v3/quote"
        self.batch_size = 50
        self.rate_limiter = AdvancedRateLimiter()
        
    def fetch_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Fetch with rate limiting"""
        if not symbols:
            return {'data': []}
        
        all_data = []
        total_batches = (len(symbols) + self.batch_size - 1) // self.batch_size
        
        for batch_num, i in enumerate(range(0, len(symbols), self.batch_size)):
            if not self.rate_limiter.wait_until_ready(self.name):
                print(f"[WARNING] {self.name}: Rate limit exceeded.")
                break
            
            batch = symbols[i:i + self.batch_size]
            symbol_str = ','.join(batch)
            
            print(f"[INFO] {self.name}: Batch {batch_num + 1}/{total_batches}")
            params = {'symbol': symbol_str, 'apikey': self.api_key}
            response = self._make_request(self.base_url, params)
            
            if response and isinstance(response, list):
                all_data.extend(response)
                self.rate_limiter.record_call(self.name)
            
            if batch_num < total_batches - 1:
                time.sleep(0.5)
        
        return {'data': all_data, 'provider': 'fmp'}
    
    def normalize_data(self, raw_data: Dict) -> List[Dict]:
        """Normalize FMP data"""
        normalized = []
        for item in raw_data.get('data', []):
            normalized.append({
                'symbol': item.get('symbol'),
                'date': item.get('date', ''),
                'open': item.get('open'),
                'high': item.get('dayHigh'),
                'low': item.get('dayLow'),
                'close': item.get('price'),
                'volume': item.get('volume'),
                'provider': 'fmp'
            })
        return normalized
