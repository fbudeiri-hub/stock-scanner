import yfinance as yf
import pandas as pd
import logging
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QuoteFetcher:
    """ULTRA-SIMPLE: Just yfinance, properly validated"""
    
    def __init__(self):
        self.rate_limit_wait = 0.01

    def fetch_yfinance(self, symbol: str, days: int = 30):
        """Ultra-simple yfinance fetch"""
        try:
            # Calculate dates
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Download
            df = yf.download(
                symbol,
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                progress=False
            )
            
            # Validate: must have data
            if df is None or df.empty or len(df) < 2:
                return None
            
            # CRITICAL: Reset index to make Date a column
            df = df.reset_index()
            
            # Normalize column names to lowercase
            df.columns = [str(col).lower() for col in df.columns]
            
            # Select only required columns (handle both 'date' and 'datetime')
            date_col = 'date' if 'date' in df.columns else ('datetime' if 'datetime' in df.columns else df.columns)
            
            # Ensure we have OHLCV columns
            required = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required):
                logger.debug(f"{symbol}: Missing columns. Got: {list(df.columns)}")
                return None
            
            # Create clean DataFrame
            result = pd.DataFrame({
                'date': df[date_col],
                'open': df['open'],
                'high': df['high'],
                'low': df['low'],
                'close': df['close'],
                'volume': df['volume']
            })
            
            result['symbol'] = symbol
            
            # Final validation
            if len(result) < 2:
                return None
            
            logger.info(f"✓ {symbol}: {len(result)} bars")
            return result
            
        except Exception as e:
            logger.debug(f"{symbol} failed: {str(e)[:50]}")
            return None

    def fetch_with_fallback(self, symbol: str, days: int = 30):
        """Single provider only - keep it simple"""
        return self.fetch_yfinance(symbol, days)

    def fetch_batch(self, symbols: list, days: int = 30, workers: int = 6):
        """Fetch multiple symbols in parallel"""
        results = {}
        success_count = 0
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self.fetch_with_fallback, sym, days): sym 
                for sym in symbols
            }
            
            for i, future in enumerate(as_completed(futures), 1):
                try:
                    df = future.result()
                    if df is not None and len(df) >= 2:
                        symbol = df['symbol'].iloc
                        results[symbol] = df
                        success_count += 1
                except Exception as e:
                    logger.debug(f"Batch error: {str(e)[:30]}")
                
                # Progress every 100 stocks
                if i % 100 == 0:
                    logger.info(f"Progress: {i}/{len(symbols)} ({success_count} success)")
        
        logger.info(f"✅ COMPLETE: {len(results)}/{len(symbols)} stocks ({100*len(results)/len(symbols):.1f}% success)")
        return results
