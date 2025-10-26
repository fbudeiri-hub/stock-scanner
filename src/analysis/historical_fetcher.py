import pandas as pd
import requests
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HistoricalDataFetcher:
    """
    Fetches 30 days of historical OHLCV data for stocks using Finnhub API.
    Handles rate limiting, errors, and caching.
    """
    
    def __init__(self, finnhub_key: str, marketstack_key: str = None):
        """
        Initialize the fetcher with API keys.
        
        Args:
            finnhub_key: Your Finnhub API key
            marketstack_key: Your Marketstack API key (backup)
        """
        self.finnhub_key = finnhub_key
        self.marketstack_key = marketstack_key
        self.finnhub_url = "https://finnhub.io/api/v1/quote"
        self.finnhub_candles_url = "https://finnhub.io/api/v1/stock/candle"
        self.rate_limit_delay = 0.1  # 100ms delay between requests
        self.last_request_time = 0
        
    def _respect_rate_limit(self):
        """Ensure we don't exceed rate limits."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def fetch_historical_candles(self, symbol: str, days: int = 30) -> Optional[pd.DataFrame]:
        """
        Fetch historical daily candles for a stock.
        
        Args:
            symbol: Stock ticker (e.g., 'AAPL')
            days: Number of days of history to fetch (default 30)
            
        Returns:
            DataFrame with columns: [timestamp, open, high, low, close, volume]
            or None if fetch fails
        """
        try:
            self._respect_rate_limit()
            
            # Calculate date range
            to_date = datetime.now()
            from_date = to_date - timedelta(days=days)
            
            # Format dates for API (YYYY-MM-DD)
            from_timestamp = int(from_date.timestamp())
            to_timestamp = int(to_date.timestamp())
            
            # Fetch from Finnhub
            params = {
                'symbol': symbol,
                'resolution': 'D',  # Daily
                'from': from_timestamp,
                'to': to_timestamp,
                'token': self.finnhub_key
            }
            
            response = requests.get(self.finnhub_candles_url, params=params, timeout=5)
            response.raise_for_status()
            
            data = response.json()
            
            # Check if we got valid data
            if 'c' not in data or data['s'] == 'no_data':
                logger.warning(f"No historical data for {symbol}")
                return None
            
            # Transform to DataFrame
            df = pd.DataFrame({
                'timestamp': pd.to_datetime(data['t'], unit='s'),
                'open': data['o'],
                'high': data['h'],
                'low': data['l'],
                'close': data['c'],
                'volume': data['v']
            })
            
            df['symbol'] = symbol
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            logger.info(f"✓ Fetched {len(df)} candles for {symbol}")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {symbol}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {symbol}: {str(e)}")
            return None
    
    def fetch_batch(self, symbols: List[str], days: int = 30) -> Dict[str, pd.DataFrame]:
        """
        Fetch historical data for multiple stocks.
        
        Args:
            symbols: List of stock tickers
            days: Number of days of history to fetch
            
        Returns:
            Dictionary mapping symbol -> DataFrame
        """
        results = {}
        total = len(symbols)
        
        for i, symbol in enumerate(symbols, 1):
            print(f"[{i}/{total}] Fetching {symbol}...", end='\r')
            df = self.fetch_historical_candles(symbol, days)
            if df is not None:
                results[symbol] = df
        
        print(f"\n✓ Successfully fetched {len(results)}/{total} stocks")
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
        logger.info(f"✓ Saved historical data to {filepath}")
    
    def get_latest_price_for_symbol(self, df: pd.DataFrame, symbol: str) -> Optional[float]:
        """Get the latest close price for a symbol from the DataFrame."""
        symbol_data = df[df['symbol'] == symbol]
        if symbol_data.empty:
            return None
        return symbol_data.iloc[-1]['close']
    
    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        Validate the quality of fetched data.
        
        Returns:
            Dictionary with quality metrics
        """
        return {
            'total_records': len(df),
            'unique_symbols': df['symbol'].nunique(),
            'date_range': f"{df['timestamp'].min()} to {df['timestamp'].max()}",
            'missing_values': df.isnull().sum().to_dict(),
            'avg_records_per_symbol': len(df) / df['symbol'].nunique() if df['symbol'].nunique() > 0 else 0
        }


# ============================================================================
# USAGE EXAMPLE - How to use this module
# ============================================================================

if __name__ == "__main__":
    # Initialize fetcher with your API keys
    FINNHUB_KEY = "YOUR_FINNHUB_KEY_HERE"  # Replace with your actual key
    
    fetcher = HistoricalDataFetcher(FINNHUB_KEY)
    
    # Example 1: Fetch for a single stock
    aapl_data = fetcher.fetch_historical_candles('AAPL', days=30)
    if aapl_data is not None:
        print(aapl_data.head())
    
    # Example 2: Fetch for multiple stocks
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    historical_dict = fetcher.fetch_batch(symbols, days=30)
    
    # Example 3: Combine and save
    combined_df = fetcher.combine_batches(historical_dict)
    fetcher.save_to_csv(combined_df, 'data/historical_30d.csv')
    
    # Example 4: Check data quality
    quality = fetcher.validate_data_quality(combined_df)
    print("Data Quality Report:")
    for key, value in quality.items():
        print(f"  {key}: {value}")
