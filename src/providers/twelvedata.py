"""
Twelve Data API provider implementation
"""

from typing import List, Dict, Any
from .base import BaseProvider
import time

class TwelvedataProvider(BaseProvider):
    """Twelve Data API provider"""
    
    def __init__(self, api_key: str):
        super().__init__(api_key, "Twelve Data")
        self.base_url = "https://api.twelvedata.com/quote"
        self.batch_size = 120
        self.rate_limit_delay = 0.1
        
    def fetch_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Fetch data from Twelve Data"""
        if not symbols:
            return {'data': []}
            
        all_data = []
        
        # Process in batches
        for i in range(0, len(symbols), self.batch_size):
            batch = symbols[i:i + self.batch_size]
            symbol_str = ','.join(batch)
            
            params = {
                'symbol': symbol_str,
                'apikey': self.api_key,
                'format': 'json'
            }
            
            print(f"[INFO] {self.name}: Fetching {len(batch)} symbols...")
            
            response = self._make_request(self.base_url, params)
            
            if response:
                if 'data' in response:
                    all_data.extend(response['data'])
                else:
                    all_data.append(response)
            
            time.sleep(self.rate_limit_delay)
            
        return {'data': all_data, 'provider': 'twelvedata'}
    
    def normalize_data(self, raw_data: Dict) -> List[Dict]:
        """Convert Twelve Data format to standard format"""
        normalized = []
        
        for item in raw_data.get('data', []):
            if isinstance(item, dict):
                normalized.append({
                    'symbol': item.get('symbol'),
                    'date': item.get('datetime', ''),
                    'open': item.get('open'),
                    'high': item.get('high'),
                    'low': item.get('low'),
                    'close': item.get('close'),
                    'volume': item.get('volume'),
                    'provider': 'twelvedata'
                })
        return normalized
