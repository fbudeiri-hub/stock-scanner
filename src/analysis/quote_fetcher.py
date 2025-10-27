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
        # FIXED: Changed to match GitHub Secrets naming
        self.finnhub_key = os.getenv('FINNHUB_KEY', '')
        self.twelvedata_key = os.getenv('TWELVE_DATA_KEY', '')  # FIXED: WITH UNDERSCORE
        self.polygon_key = os.getenv('POLYGON_KEY', '')
        self.fmp_key = os.getenv('FMP_KEY', '')
        self.tiingo_key = os.getenv('TIINGO_KEY', '')
        self.alpha_vantage_key = os.getenv('ALPHA_VANTAGE_KEY', '')
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
        except Exception as e:
            logger.debug(f"Finnhub {symbol} failed: {e}")
            return None

    def fetch_polygon(self, symbol: str, days: int = 30):
        try:
            time.sleep(self.rate_limit_wait)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            resp = requests.get(
                f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}",
                params={'apiKey': self.polygon_key},
                timeout=5
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get('status') == 'OK' and len(data.get('results', [])) >= 2:
                    results = data['results']
                    df = pd.DataFrame({
                        'open': [r.get('o') for r in results],
                        'high': [r.get('h') for r in results],
                        'low': [r.get('l') for r in results],
                        'close': [r.get('c') for r in results],
                        'volume': [r.get('v') for r in results]
                    })
                    logger.info(f"Polygon {symbol}: {len(df)} bars")
                    return df
            return None
        except Exception as e:
            logger.debug(f"Polygon {symbol} failed: {e}")
            return None

    def fetch_fmp(self, symbol: str, days: int = 30):
        try:
            time.sleep(self.rate_limit_wait)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            resp = requests.get(
                f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}",
                params={
                    'from': start_date.strftime('%Y-%m-%d'),
                    'to': end_date.strftime('%Y-%m-%d'),
                    'apikey': self.fmp_key
                },
                timeout=5
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get('historical') and len(data['historical']) >= 2:
                    historical = data['historical']
                    df = pd.DataFrame({
                        'open': [h.get('open') for h in historical],
                        'high': [h.get('high') for h in historical],
                        'low': [h.get('low') for h in historical],
                        'close': [h.get('close') for h in historical],
                        'volume': [h.get('volume') for h in historical]
                    })
                    logger.info(f"FMP {symbol}: {len(df)} bars")
                    return df
            return None
        except Exception as e:
            logger.debug(f"FMP {symbol} failed: {e}")
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
        except Exception as e:
            logger.debug(f"YFinance {symbol} failed: {e}")
            return None
    
    def fetch_with_fallback(self, symbol: str, days: int = 30):
        """Try multiple providers in order of preference"""
        # Try Finnhub first (most reliable)
        df = self.fetch_finnhub(symbol, days)
        if df is not None:
            return df
        
        # Try Polygon
        df = self.fetch_polygon(symbol, days)
        if df is not None:
            return df
        
        # Try FMP
        df = self.fetch_fmp(symbol, days)
        if df is not None:
            return df
        
        # Fallback to yfinance (free, no key needed)
        df = self.fetch_yfinance(symbol, days)
        if df is not None:
            return df
        
        logger.warning(f"All providers failed: {symbol}")
        return None

    def fetch_batch(self, symbols: list, days: int = 30, workers: int = 3):
        """Fetch data for multiple symbols in parallel"""
        results = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self.fetch_with_fallback, sym, days): sym for sym in symbols}
            for i, future in enumerate(as_completed(futures)):
                symbol = futures[future]
                try:
                    df = future.result()
                    if df is not None:
                        results[symbol] = df
                except Exception as e:
                    logger.debug(f"Batch fetch failed for {symbol}: {e}")
                    pass
                if (i + 1) % 100 == 0:
                    logger.info(f"Progress: {i+1}/{len(symbols)} ({len(results)} success)")
        
        logger.info(f"Batch complete: {len(results)}/{len(symbols)} stocks fetched")
        return results
