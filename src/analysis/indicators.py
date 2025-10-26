import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TechnicalIndicators:
    """
    Calculates technical indicators for stock price data.
    Includes: RSI, MACD, Bollinger Bands
    """
    
    @staticmethod
    def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
        """
        Calculate Relative Strength Index (RSI).
        
        RSI = 100 - (100 / (1 + RS))
        where RS = Average Gain / Average Loss
        
        Args:
            prices: Series of close prices
            period: Number of periods (default 14)
            
        Returns:
            Series with RSI values (0-100)
            - RSI > 70: Overbought (potential sell)
            - RSI < 30: Oversold (potential buy)
        """
        try:
            delta = prices.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
            
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            return rsi.fillna(50)  # Fill NaN with neutral value
        except Exception as e:
            logger.error(f"Error calculating RSI: {str(e)}")
            return pd.Series( * len(prices))  # Return neutral RSI if error
    
    @staticmethod
    def calculate_macd(prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Calculate MACD (Moving Average Convergence Divergence).
        
        MACD = EMA(12) - EMA(26)
        Signal Line = EMA(9) of MACD
        Histogram = MACD - Signal Line
        
        Args:
            prices: Series of close prices
            fast: Fast EMA period (default 12)
            slow: Slow EMA period (default 26)
            signal: Signal line period (default 9)
            
        Returns:
            Tuple of (MACD, Signal Line, Histogram)
            - Positive histogram: Bullish
            - Negative histogram: Bearish
            - MACD crosses signal line: Trading signals
        """
        try:
            ema_fast = prices.ewm(span=fast, adjust=False).mean()
            ema_slow = prices.ewm(span=slow, adjust=False).mean()
            
            macd_line = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=signal, adjust=False).mean()
            histogram = macd_line - signal_line
            
            return macd_line.fillna(0), signal_line.fillna(0), histogram.fillna(0)
        except Exception as e:
            logger.error(f"Error calculating MACD: {str(e)}")
            zero_series = pd.Series( * len(prices))
            return zero_series, zero_series, zero_series
    
    @staticmethod
    def calculate_bollinger_bands(prices: pd.Series, period: int = 20, std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Calculate Bollinger Bands.
        
        Middle Band = SMA(20)
        Upper Band = Middle Band + (2 * StdDev)
        Lower Band = Middle Band - (2 * StdDev)
        
        Args:
            prices: Series of close prices
            period: Period for moving average (default 20)
            std_dev: Number of standard deviations (default 2.0)
            
        Returns:
            Tuple of (Upper Band, Middle Band, Lower Band)
            - Price touches upper band: Potentially overbought
            - Price touches lower band: Potentially oversold
        """
        try:
            middle_band = prices.rolling(window=period).mean()
            std = prices.rolling(window=period).std()
            
            upper_band = middle_band + (std * std_dev)
            lower_band = middle_band - (std * std_dev)
            
            return upper_band.fillna(prices), middle_band.fillna(prices), lower_band.fillna(prices)
        except Exception as e:
            logger.error(f"Error calculating Bollinger Bands: {str(e)}")
            return prices, prices, prices  # Return price if error
    
    @staticmethod
    def add_indicators_to_dataframe(df: pd.DataFrame, price_col: str = 'close') -> pd.DataFrame:
        """
        Add all technical indicators to a DataFrame.
        
        Args:
            df: DataFrame with price data (must be sorted by date for each symbol)
            price_col: Column name containing prices (default 'close')
            
        Returns:
            DataFrame with new columns:
            - rsi_14
            - macd_12_26_9
            - macd_signal_9
            - macd_histogram
            - bollinger_upper_20
            - bollinger_middle_20
            - bollinger_lower_20
        """
        df = df.copy()
        
        try:
            # Group by symbol if multiple symbols in dataframe
            if 'symbol' in df.columns:
                for symbol in df['symbol'].unique():
                    mask = df['symbol'] == symbol
                    symbol_data = df[mask].sort_values('timestamp')
                    
                    # Calculate indicators
                    rsi = TechnicalIndicators.calculate_rsi(symbol_data[price_col])
                    macd, signal, histogram = TechnicalIndicators.calculate_macd(symbol_data[price_col])
                    upper, middle, lower = TechnicalIndicators.calculate_bollinger_bands(symbol_data[price_col])
                    
                    # Add to dataframe
                    df.loc[mask, 'rsi_14'] = rsi.values
                    df.loc[mask, 'macd_12_26_9'] = macd.values
                    df.loc[mask, 'macd_signal_9'] = signal.values
                    df.loc[mask, 'macd_histogram'] = histogram.values
                    df.loc[mask, 'bollinger_upper_20'] = upper.values
                    df.loc[mask, 'bollinger_middle_20'] = middle.values
                    df.loc[mask, 'bollinger_lower_20'] = lower.values
            else:
                # Single symbol
                rsi = TechnicalIndicators.calculate_rsi(df[price_col])
                macd, signal, histogram = TechnicalIndicators.calculate_macd(df[price_col])
                upper, middle, lower = TechnicalIndicators.calculate_bollinger_bands(df[price_col])
                
                df['rsi_14'] = rsi.values
                df['macd_12_26_9'] = macd.values
                df['macd_signal_9'] = signal.values
                df['macd_histogram'] = histogram.values
                df['bollinger_upper_20'] = upper.values
                df['bollinger_middle_20'] = middle.values
                df['bollinger_lower_20'] = lower.values
            
            logger.info("✓ Successfully added all technical indicators")
            return df
            
        except Exception as e:
            logger.error(f"Error adding indicators to dataframe: {str(e)}")
            return df
    
    @staticmethod
    def get_latest_indicators(df: pd.DataFrame, symbol: str) -> Dict[str, float]:
        """
        Get the latest indicator values for a specific symbol.
        
        Args:
            df: DataFrame with indicators
            symbol: Stock symbol
            
        Returns:
            Dictionary with latest indicator values
        """
        try:
            symbol_data = df[df['symbol'] == symbol].sort_values('timestamp')
            if symbol_data.empty:
                return {}
            
            latest = symbol_data.iloc[-1]
            
            return {
                'symbol': symbol,
                'date': str(latest.get('timestamp', 'N/A')),
                'close': float(latest.get('close', 0)),
                'rsi_14': float(latest.get('rsi_14', 50)),
                'macd': float(latest.get('macd_12_26_9', 0)),
                'macd_signal': float(latest.get('macd_signal_9', 0)),
                'macd_histogram': float(latest.get('macd_histogram', 0)),
                'bb_upper': float(latest.get('bollinger_upper_20', 0)),
                'bb_middle': float(latest.get('bollinger_middle_20', 0)),
                'bb_lower': float(latest.get('bollinger_lower_20', 0))
            }
        except Exception as e:
            logger.error(f"Error getting latest indicators for {symbol}: {str(e)}")
            return {}


# ============================================================================
# USAGE EXAMPLE - How to use this module
# ============================================================================

if __name__ == "__main__":
    from src.analysis.historical_fetcher import HistoricalDataFetcher
    import os
    
    # Initialize fetcher
    FINNHUB_KEY = os.getenv('FINNHUB_KEY', 'your_key_here')
    fetcher = HistoricalDataFetcher(FINNHUB_KEY)
    
    # Fetch historical data for a few stocks
    symbols = ['AAPL', 'MSFT', 'GOOGL']
    historical_dict = fetcher.fetch_batch(symbols, days=30)
    
    # Combine into one DataFrame
    combined_df = fetcher.combine_batches(historical_dict)
    
    # Add technical indicators
    df_with_indicators = TechnicalIndicators.add_indicators_to_dataframe(combined_df)
    
    # Show results
    print("\nDataFrame with Indicators:")
    print(df_with_indicators[['symbol', 'timestamp', 'close', 'rsi_14', 'macd_12_26_9', 'bollinger_upper_20']].tail(10))
    
    # Get latest indicators for AAPL
    print("\nLatest Indicators for AAPL:")
    latest = TechnicalIndicators.get_latest_indicators(df_with_indicators, 'AAPL')
    for key, value in latest.items():
        print(f"  {key}: {value}")
