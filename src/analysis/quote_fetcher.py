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
    """Fetch stock data from WORKING providers only"""
    
    def __init__(self):
        self.polygon_key = os.getenv('POLYGON_KEY', '')
        self.twelve_data_key = os.getenv('TWELVE_DATA_KEY', '')
        self.rate_limit_wait = 0.05

    def fetch_yfinance(self, symbol: str, days: int = 30):
        """PRIMARY: Free, reliable, no authentication issues"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Download data
            df = yf.download(symbol, start=start_date, end=end_date, progress=False)
            
            if df is None or len(df) < 2:
                return None
            
            # CRITICAL FIX: Handle MultiIndex columns from yfinance
            if isinstance(df.columns, pd.MultiIndex):
                # If MultiIndex, flatten it
                df.columns = [col.lower() if isinstance(col, tuple) else col.lower() for col in df.columns]
            else:
                # If single index, just lowercase
                df.columns = [col.lower() for col in df.columns]
            
            # Reset index to make date a column
            df = df.reset_index()
            df.rename(columns={'date': 'date', 'index': 'date'}, inplace=True)
            
            # Ensure required columns
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_cols):
                logger.debug(f"yfinance {symbol}: Missing required columns. Got: {list(df.columns)}")
                return None
            
            # Keep only required columns + date
            df = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df['symbol'] = symbol
            
            logger.info(f"✓ {symbol}: yfinance ({len(df)} bars)")
            return df
            
        except Exception as e:
            logger.debug(f"yfinance {symbol} failed: {e}")
            return None

    def fetch_polygon(self, symbol: str, days: int = 30):
        """BACKUP 1: Working API, 200 bars limit"""
        if not self.polygon_key:
            return None
            
        try:
            time.sleep(self.rate_limit_wait)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            resp = requests.get(
                f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}",
                params={'apiKey': self.polygon_key},
                timeout=5
            )
            
            if resp.status_code != 200:
                return None
            
            data = resp.json()
            
            if data.get('status') != 'OK' or not data.get('results'):
                return None
            
            results = data['results']
            df = pd.DataFrame({
                'date': [datetime.fromtimestamp(r['t']/1000) for r in results],
                'open': [r.get('o') for r in results],
                'high': [r.get('h') for r in results],
                'low': [r.get('l') for r in results],
                'close': [r.get('c') for r in results],
                'volume': [r.get('v') for r in results]
            })
            
            df['symbol'] = symbol
            df = df.dropna(subset=['close'])
            
            if len(df) < 2:
                return None
            
            logger.info(f"✓ {symbol}: polygon ({len(df)} bars)")
            return df
            
        except Exception as e:
            logger.debug(f"polygon {symbol} failed: {e}")
            return None

    def fetch_twelvedata(self, symbol: str, days: int = 30):
        """BACKUP 2: Working API, returns 30 bars"""
        if not self.twelve_data_key:
            return None
            
        try:
            time.sleep(self.rate_limit_wait)
            
            resp = requests.get(
                "https://api.twelvedata.com/time_series",
                params={
                    'symbol': symbol,
                    'interval': '1day',
                    'outputsize': 30,
                    'apikey': self.twelve_data_key
                },
                timeout=5
            )
            
            if resp.status_code != 200:
                return None
            
            data = resp.json()
            
            if data.get('status') != 'ok' or not data.get('values'):
                return None
            
            values = data['values']
            df = pd.DataFrame({
                'date': [datetime.strptime(v['datetime'], '%Y-%m-%d') for v in values],
                'open': [float(v.get('open', 0)) for v in values],
                'high': [float(v.get('high', 0)) for v in values],
                'low': [float(v.get('low', 0)) for v in values],
                'close': [float(v.get('close', 0)) for v in values],
                'volume': [float(v.get('volume', 0)) for v in values]
            })
            
            df['symbol'] = symbol
            df = df.dropna(subset=['close'])
            df = df[df['close'] > 0]  # Remove invalid rows
            
            if len(df) < 2:
                return None
            
            logger.info(f"✓ {symbol}: 12data ({len(df)} bars)")
            return df
            
        except Exception as e:
            logger.debug(f"12data {symbol} failed: {e}")
            return None

    def fetch_with_fallback(self, symbol: str, days: int = 30):
        """Try providers in order: yfinance → Polygon → 12Data"""
        
        # PRIMARY
        df = self.fetch_yfinance(symbol, days)
        if df is not None and len(df) >= 2:
            return df
        
        # BACKUP 1
        df = self.fetch_polygon(symbol, days)
        if df is not None and len(df) >= 2:
            return df
        
        # BACKUP 2
        df = self.fetch_twelvedata(symbol, days)
        if df is not None and len(df) >= 2:
            return df
        
        logger.warning(f"✗ {symbol}: ALL providers failed")
        return None

    def fetch_batch(self, symbols: list, days: int = 30, workers: int = 6):
        """Fetch data for multiple symbols in parallel"""
        results = {}
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self.fetch_with_fallback, sym, days): sym for sym in symbols}
            
            for i, future in enumerate(as_completed(futures)):
                try:
                    df = future.result()
                    if df is not None and len(df) >= 2:
                        symbol = df['symbol'].iloc
                        results[symbol] = df
                except Exception as e:
                    logger.debug(f"Batch fetch error: {e}")
                
                if (i + 1) % 100 == 0:
                    logger.info(f"Progress: {i+1}/{len(symbols)} ({len(results)} success)")
        
        logger.info(f"✅ Batch: {len(results)}/{len(symbols)} stocks fetched")
        return results
