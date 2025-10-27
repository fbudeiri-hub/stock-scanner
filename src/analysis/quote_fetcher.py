import yfinance as yf
import pandas as pd
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QuoteFetcher:
    def fetch_with_fallback(self, symbol: str, days: int = 30) -> pd.DataFrame:
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            df = yf.download(symbol, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), progress=False)
            
            # Handle tuple return from yfinance (error case)
            if isinstance(df, tuple):
                raise ValueError(f"yfinance returned tuple for {symbol}")
            
            if df is None or df.empty or len(df) < 2:
                raise ValueError(f"Insufficient data for {symbol}")
            
            if 'Close' not in df.columns:
                raise ValueError(f"Missing Close column for {symbol}")
            
            df.columns = [col.lower() for col in df.columns]
            df = df.dropna(subset=['close'])
            
            if len(df) < 2:
                raise ValueError(f"Insufficient valid data for {symbol}")
            
            logger.info(f"✅ {symbol}: {len(df)} days")
            return df
            
        except Exception as e:
            logger.warning(f"⚠ {symbol}: {str(e)}")
            return None

    def fetch_batch(self, symbols: list, days: int = 30) -> dict:
        results = {}
        for symbol in symbols:
            df = self.fetch_with_fallback(symbol, days)
            if df is not None:
                results[symbol] = df
        return results
