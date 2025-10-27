import requests
import pandas as pd
import logging
import time
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QuoteFetcher:
    def __init__(self):
        self.finnhub_key = os.getenv('FINNHUB_KEY', '')
        self.marketstack_key = os.getenv('MARKETSTACK_KEY', '')
        self.finnhub_url = "https://finnhub.io/api/v1/stock/candle"
        self.marketstack_url = "http://api.marketstack.com/v1/eod"
        self.session = requests.Session()
        self.rate_limit_wait = 0.2  # 200ms between calls
        
    def fetch_finnhub(self, symbol: str, days: int = 30):
        """Fetch from Finnhub with rate limiting"""
        try:
            time.sleep(self.rate_limit_wait)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            params = {
                'symbol': symbol,
                'resolution': 'D',
                'from': int(start_date.timestamp()),
                'to': int(end_date.timestamp()),
                'token': self.finnhub_key
            }
            
            response = self.session.get(self.finnhub_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('s') == 'ok' and len(data.get('c', [])) >= 2:
                    df = pd.DataFrame({
                        'open': data['o'],
                        'high': data['h'],
                        'low': data['l'],
                        'close': data['c'],
                        'volume': data['v']
                    })
                    logger.info(f"✅ Finnhub {symbol}: {len(df)} days")
                    return df
            return None
        except Exception as e:
            logger.debug(f"Finnhub failed for {symbol}: {str(e)}")
            return None
    
    def fetch_marketstack(self, symbol: str, days: int = 30):
        """Fetch from Marketstack as fallback"""
        try:
            time.sleep(self.rate_limit_wait)
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            params = {
                'symbols': symbol,
                'date_from': start_date,
                'date_to': end_date,
                'access_key': self.marketstack_key,
                'limit': 100
            }
            
            response = self.session.get(self.marketstack_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data') and len(data['data']) >= 2:
                    df = pd.DataFrame([{
                        'open': d['open'],
                        'high': d['high'],
                        'low': d['low'],
                        'close': d['close'],
                        'volume': d['volume']
                    } for d in data['data']])
                    logger.info(f"✅ Marketstack {symbol}: {len(df)} days")
                    return df
            return None
        except Exception as e:
            logger.debug(f"Marketstack failed for {symbol}: {str(e)}")
            return None
    
    def fetch_with_fallback(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """Try Finnhub first, fallback to Marketstack"""
        df = self.fetch_finnhub(symbol, days)
        if df is not None:
            return df
        df = self.fetch_marketstack(symbol, days)
        if df is not None:
            return df
        logger.warning(f"⚠ Both providers failed for {symbol}")
        return None
    
    def fetch_batch(self, symbols: list, days: int = 30, workers: int = 6) -> dict:
        """Parallel batch fetching with rate limiting"""
        results = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self.fetch_with_fallback, sym, days): sym for sym in symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    df = future.result()
                    if df is not None:
                        results[symbol] = df
                except Exception as e:
                    logger.error(f"Error fetching {symbol}: {e}")
        
        logger.info(f"Batch complete: {len(results)}/{len(symbols)} symbols")
        return results
