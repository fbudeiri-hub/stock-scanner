"""
Optimized Marketstack API provider with rate limiting
"""

from typing import List, Dict, Any
from .base import BaseProvider
from utils.rate_limiter_advanced import AdvancedRateLimiter
import time

class MarketstackProvider(BaseProvider):
    """Marketstack with rate limit compliance"""
    
    def __init__(self, api_key: str):
        super().__init__(api_key, "Marketstack")
        self.base_url = "http://api.marketstack.com/v1/eod"
        self.batch_size = 50
        self.rate_limiter = AdvancedRateLimiter()
        
    def fetch_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Fetch with rate limiting"""
        if not symbols:
            return {'data': []}
        
        all_data = []
        total_batches = (len(symbols) + self.batch_size - 1) // self.batch_size
        
        for batch_num, i in enumerate(range(0, len(symbols), self.batch_size)):
            # Check rate limits
            if not self.rate_limiter.wait_until_ready(self.name):
                print(f"[WARNING] {self.name}: Rate limit exceeded. Stopping.")
                break
            
            batch = symbols[i:i + self.batch_size]
            symbol_str = ','.join(batch)
            
            print(f"[INFO] {self.name}: Batch {batch_num + 1}/{total_batches}")
            
            params = {'symbols': symbol_str, 'access_key': self.api_key, 'limit': 1}
            response = self._make_request(self.base_url, params)
            
            if response and 'data' in response:
                all_data.extend(response['data'])
                self.rate_limiter.record_call(self.name)
                
            if batch_num < total_batches - 1:
                time.sleep(0.5)
        
        return {'data': all_data, 'provider': 'marketstack'}
    
    def normalize_data(self, raw_data: Dict) -> List[Dict]:
        """Normalize data"""
        normalized = []
        for item in raw_data.get('data', []):
            normalized.append({
                'symbol': item.get('symbol'),
                'date': item.get('date'),
                'open': item.get('open'),
                'high': item.get('high'),
                'low': item.get('low'),
                'close': item.get('close'),
                'volume': item.get('volume'),
                'provider': 'marketstack'
            })
        return normalized
