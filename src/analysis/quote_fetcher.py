import yfinance as yf
import pandas as pd
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QuoteFetcher:
    def fetch_with_fallback(self, symbol: str, days: int = 30):
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            df = yf.download(symbol, start=start_date, end=end_date, progress=False)
            
            if isinstance(df, pd.DataFrame) and len(df) >= 2 and 'Close' in df.columns:
                df.columns = [c.lower() for c in df.columns]
                return df
            return None
        except:
            return None

    def fetch_batch(self, symbols: list, days: int = 30):
        results = {}
        for symbol in symbols:
            df = self.fetch_with_fallback(symbol, days)
            if df is not None:
                results[symbol] = df
        return results
