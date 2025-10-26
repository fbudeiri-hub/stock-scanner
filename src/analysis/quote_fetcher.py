import pandas as pd
import yfinance as yf
from polygon import RESTClient
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime, timedelta
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QuoteFetcher:
    """
    Fetches historical OHLCV data for stocks.
    Primary: yfinance (unlimited, no rate limits)
    Backup: Polygon API (5 calls/min with your key)
    """
    
    def __init__(self, polygon_key: Optional[str] = None):
        """
        Initialize the fetcher.
        
        Args:
            polygon_key: 5we5CV0h67o5hv3xvBeXYCdrTWY4wC2Zy (optional, for fallback)
        """
        self.polygon_key = polygon_key
        self.polygon_client = RESTClient(polygon_key) if polygon_key else None
        self.rate_limit_delay = 0.2  # 200ms for Polygon (5/min)
        self.last_request_time = 0
    
    def _respect_rate_limit(self):
        """Respect Polygon's rate limits (5 calls/min = 1 call per 12 seconds)"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def fetch_yfinance(self, symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
        """
        Fetch historical data from yfinance (PRIMARY SOURCE).
        
        Args:
            symbol: Stock ticker (e.g., 'AAPL')
            days: Number of days of history (default 30)
            
        Returns:
            DataFrame with columns: [timestamp, open, high, low, close, volume, symbol]
            or None if fetch fails
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            df = yf.download(symbol, start=start_date, end=end_date, progress=False)
            
            if df.empty:
                logger.warning(f"No data from yfinance for {symbol}")
                return None
            
            # Reset index to make Date a column
            df = df.reset_index()
            df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            df['symbol'] = symbol
            
            logger.info(f"SUCCESS: Fetched {len(df)} days for {symbol} (yfinance)")
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'symbol']]
            
        except Exception as e:
            logger.warning(f"yfinance failed for {symbol}: {str(e)}")
            return None
    
    def fetch_polygon(self, symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
        """
        Fetch historical data from Polygon API (FALLBACK SOURCE).
        
        Args:
            symbol: Stock ticker
            days: Number of days of history
            
        Returns:
            DataFrame or None if fetch fails
        """
        if not self.polygon_client:
            logger.warning("Polygon key not configured, skipping Polygon fetch")
            return None
        
        try:
            self._respect_rate_limit()
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            aggs = self.polygon_client.get_aggs(
                ticker=symbol,
                multiplier=1,
                timespan="day",
                from_=start_date.strftime('%Y-%m-%d'),
                to=end_date.strftime('%Y-%m-%d'),
                limit=250
            )
            
            if not aggs:
                logger.warning(f"No data from Polygon for {symbol}")
                return None
            
            data = {
                'timestamp': [datetime.fromtimestamp(agg.timestamp / 1000) for agg in aggs],
                'open': [agg.o for agg in aggs],
                'high': [agg.h for agg in aggs],
                'low': [agg.l for agg in aggs],
                'close': [agg.c for agg in aggs],
                'volume': [agg.v for agg in aggs],
                'symbol': symbol
            }
            
            df = pd.DataFrame(data)
            logger.info(f"SUCCESS: Fetched {len(df)} days for {symbol} (Polygon)")
            return df
            
        except Exception as e:
            logger.error(f"Polygon failed for {symbol}: {str(e)}")
            return None
    
    def fetch_with_fallback(self, symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
        """
        Fetch data with fallback: tries yfinance first, then Polygon.
        
        Args:
            symbol: Stock ticker
            days: Number of days of history
            
        Returns:
            DataFrame with historical data, or None
        """
        # Try yfinance first (PRIMARY - no limits)
        df = self.fetch_yfinance(symbol, days)
        if df is not None:
            return df
        
        # Fall back to Polygon if yfinance fails
        logger.info(f"yfinance failed for {symbol}, trying Polygon as fallback...")
        df = self.fetch_polygon(symbol, days)
        if df is not None:
            return df
        
        logger.error(f"Both yfinance and Polygon failed for {symbol}")
        return None
    
    def fetch_batch(self, symbols: List[str], days: int = 30, use_fallback: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for multiple stocks.
        
        Args:
            symbols: List of stock tickers
            days: Number of days of history
            use_fallback: Use fallback mechanism (recommended)
            
        Returns:
            Dictionary mapping symbol -> DataFrame
        """
        results = {}
        total = len(symbols)
        
        for i, symbol in enumerate(symbols, 1):
            print(f"[{i}/{total}] Fetching {symbol}...")
            
            if use_fallback:
                df = self.fetch_with_fallback(symbol, days)
            else:
                df = self.fetch_yfinance(symbol, days)
            
            if df is not None:
                results[symbol] = df
        
        print(f"\nSuccessfully fetched {len(results)}/{total} stocks")
        return results
    
    def combine_batches(self, data_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Combine individual stock DataFrames into one.
        
        Args:
            data_dict: Dictionary from fetch_batch()
            
        Returns:
            Combined DataFrame with all stocks
        """
        if not data_dict:
            return pd.DataFrame()
        
        dfs = list(data_dict.values())
        combined = pd.concat(dfs, ignore_index=True)
        
        # Sort by symbol and date
        combined = combined.sort_values(['symbol', 'timestamp']).reset_index(drop=True)
        
        return combined
    
    def save_to_csv(self, df: pd.DataFrame, filepath: str):
        """Save historical data to CSV."""
        df.to_csv(filepath, index=False)
        logger.info(f"Saved historical data to {filepath}")


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    import os
    
    # Initialize fetcher with optional Polygon key
    polygon_key = os.getenv('POLYGON_KEY')
    fetcher = QuoteFetcher(polygon_key=polygon_key)
    
    # Fetch for 5 stocks
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    data_dict = fetcher.fetch_batch(symbols, days=30)
    
    # Combine and save
    combined_df = fetcher.combine_batches(data_dict)
    fetcher.save_to_csv(combined_df, 'data/historical_30d.csv')
    
    print("\nSample data:")
    print(combined_df.head())
