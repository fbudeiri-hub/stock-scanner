import yfinance as yf
import pandas as pd
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QuoteFetcher:
    """Fetch historical OHLCV data with error handling and fallback"""

    def fetch_with_fallback(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """Fetch data with validation and fallback handling"""
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Try yfinance
            logger.info(f"Fetching {symbol} from yfinance...")
            df = yf.download(
                symbol, 
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                progress=False
            )
            
            # VALIDATION CHECKS
            if df.empty:
                raise ValueError(f"No data returned for {symbol}")
            
            if len(df) < 2:
                raise ValueError(f"Insufficient data ({len(df)} rows) for {symbol}")
            
            # Check for required columns
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing columns {missing_cols} for {symbol}")
            
            # Standardize column names to lowercase
            df.columns = [col.lower() for col in df.columns]
            
            # Check for data quality
            if df['close'].isna().all():
                raise ValueError(f"All Close prices are NaN for {symbol}")
            
            # Remove rows with NaN Close prices
            df = df.dropna(subset=['close'])
            
            if len(df) < 2:
                raise ValueError(f"Insufficient valid data after cleaning for {symbol}")
            
            logger.info(f"✅ {symbol}: {len(df)} days fetched")
            return df
            
        except Exception as e:
            logger.warning(f"❌ yfinance failed for {symbol}: {str(e)}")
            logger.warning(f"Skipping {symbol} - no valid fallback available")
            return None

    def fetch_batch(self, symbols: list, days: int = 30) -> dict:
        """Fetch multiple symbols"""
        results = {}
        for symbol in symbols:
            try:
                df = self.fetch_with_fallback(symbol, days)
                if df is not None:
                    results[symbol] = df
            except Exception as e:
                logger.error(f"Batch fetch error for {symbol}: {e}")
        return results
