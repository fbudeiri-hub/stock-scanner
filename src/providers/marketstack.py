"""
Marketstack API provider implementation
"""

from typing import List, Dict, Any
from .base import BaseProvider
import time

class MarketstackProvider(BaseProvider):
    """Marketstack API provider"""
    
    def __init__(self, api_key: str):
        super().__init__(api_key, "Marketstack")
        self.base_url = "http://api.marketstack.com/v1/eod"
        self.batch_size = 100
        
    def fetch_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Fetch data from Marketstack"""
        if not symbols:
            return {'data': []}
            
        all_data = []
        
        # Process in batches
        for i in range(0, len(symbols), self.batch_size):
            batch = symbols[i:i + self.batch_size]
            symbol_str = ','.join(batch)
            
            params = {
                'symbols': symbol_str,
                'access_key': self.api_key,
                'limit': 1
            }
            
            print(f"[INFO] {self.name}: Fetching {len(batch)} symbols...")
            
            response = self._make_request(self.base_url, params)
            
            if response and 'data' in response:
                all_data.extend(response['data'])
                time.sleep(0.5)
            else:
                print(f"[WARNING] {self.name}: No data returned")
                
        return {'data': all_data, 'provider': 'marketstack'}
    
    def normalize_data(self, raw_data: Dict) -> List[Dict]:
        """Convert Marketstack format to standard format"""
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
