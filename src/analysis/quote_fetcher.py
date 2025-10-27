import requests
import yfinance as yf
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
        self.twelvedata_key = os.getenv('TWELVEDATA_KEY', '')
        self.rate_limit_wait = 0.1
        
    def fetch_finnhub(self, symbol: str, days: int = 30):
        try:
            time.sleep(self.rate_limit_wait)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            resp = requests.get(
                "https://finnhub.io/api/v1/stock/candle",
                params={
                    'symbol': symbol,
                    'resolution': 'D',
                    'from': int(start_date.timestamp()),
                    'to': int(end_date.timestamp()),
                    'token': self.finnhub_key
                },
                timeout=5
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get('s') == 'ok' and len(data.get('c', [])) >= 2:
                    df = pd.DataFrame({
                        'open': data['o'],
                        'high': data['h'],
                        'low': data['l'],
                        'close': data['c'],
                        'volume': data['v']
                    })
                    logger.info(f"Finnhub {symbol}: {len(df)} bars")
                    return df
            return None
        except:
            return None

    def fetch_yfinance(self, symbol: str, days: int = 30):
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            df = yf.download(symbol, start=start_date, end=end_date, progress=False)
            if df is not None and len(df) >= 2 and 'Close' in df.columns:
                df.columns = [c.lower() for c in df.columns]
                logger.info(f"YFinance {symbol}: {len(df)} bars")
                return df
            return None
        except:
            return None
    
    def fetch_with_fallback(self, symbol: str, days: int = 30):
        """Try providers: Finnhub -> YFinance"""
        df = self.fetch_finnhub(symbol, days)
        if df is not None:
            return df
        
        df = self.fetch_yfinance(symbol, days)
        if df is not None:
            return df
        
        logger.warning(f"All providers failed: {symbol}")
        return None

    def fetch_batch(self, symbols: list, days: int = 30, workers: int = 3):
        """Parallel batch fetching with rate limiting"""
        results = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self.fetch_with_fallback, sym, days): sym for sym in symbols}
            for i, future in enumerate(as_completed(futures)):
                symbol = futures[future]
                try:
                    df = future.result()
                    if df is not None:
                        results[symbol] = df
                except:
                    pass
                if (i + 1) % 100 == 0:
                    logger.info(f"Progress: {i+1}/{len(symbols)} ({len(results)} success)")
        
        logger.info(f"Batch complete: {len(results)}/{len(symbols)} stocks")
        return results
