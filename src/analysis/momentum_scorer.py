import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MomentumScorer:
    """
    Combines all technical indicators into a single 0-100 momentum score.
    
    Scoring logic:
    - RSI (30%): Overbought/Oversold momentum
    - MACD (30%): Trend direction and strength
    - Bollinger Bands (20%): Volatility and breakout potential
    - Price Velocity (20%): Price movement strength
    """
    
    @staticmethod
    def score_rsi(rsi: float) -> float:
        """
        Score RSI on 0-100 scale.
        
        Logic:
        - RSI < 30: Oversold (strong buy signal) = 80-100
        - RSI 30-50: Weak (neutral) = 40-60
        - RSI 50-70: Strong (bullish) = 60-80
        - RSI > 70: Overbought (sell signal) = 0-40
        
        Args:
            rsi: RSI value (0-100)
            
        Returns:
            Score 0-100
        """
        if rsi < 30:
            # Oversold - strong buy signal
            return min(100, 80 + (30 - rsi) / 3)  # 80-100
        elif rsi < 50:
            # Weak uptrend
            return 40 + (rsi - 30) / 2  # 40-50
        elif rsi < 70:
            # Strong uptrend
            return 60 + (rsi - 50) / 2  # 60-70
        else:
            # Overbought - sell signal
            return max(0, 40 - (rsi - 70) / 3)  # 0-40
    
    @staticmethod
    def score_macd(macd: float, signal: float, histogram: float) -> float:
        """
        Score MACD on 0-100 scale.
        
        Logic:
        - MACD > Signal: Bullish (positive histogram)
        - MACD < Signal: Bearish (negative histogram)
        - Histogram strength indicates momentum
        
        Args:
            macd: MACD line value
            signal: Signal line value
            histogram: MACD histogram (MACD - Signal)
            
        Returns:
            Score 0-100
        """
        try:
            # Positive histogram = bullish
            if histogram > 0:
                # Bullish: scale 50-100 based on histogram magnitude
                # Clamp histogram to reasonable range
                clamped_hist = min(abs(histogram), 2.0)
                score = 50 + (clamped_hist / 2.0) * 50
                return min(100, score)
            else:
                # Bearish: scale 0-50 based on histogram magnitude
                clamped_hist = max(abs(histogram), 0)
                score = 50 - (clamped_hist / 2.0) * 50
                return max(0, score)
        except:
            return 50  # Neutral if error
    
    @staticmethod
    def score_bollinger_bands(close: float, bb_upper: float, bb_middle: float, bb_lower: float) -> float:
        """
        Score Bollinger Bands position on 0-100 scale.
        
        Logic:
        - Close at lower band: Oversold (80-100)
        - Close at middle band: Neutral (50)
        - Close at upper band: Overbought (0-20)
        
        Args:
            close: Current close price
            bb_upper: Upper Bollinger Band
            bb_middle: Middle Bollinger Band (SMA)
            bb_lower: Lower Bollinger Band
            
        Returns:
            Score 0-100
        """
        try:
            if bb_upper == bb_lower:
                return 50  # Avoid division by zero
            
            # Normalize price position between bands (0-1)
            band_width = bb_upper - bb_lower
            position = (close - bb_lower) / band_width if band_width > 0 else 0.5
            
            # Clamp position to 0-1
            position = max(0, min(1, position))
            
            # Convert to 0-100 score
            # Low position (near lower band) = high score (oversold)
            # High position (near upper band) = low score (overbought)
            score = 100 - (position * 100)
            
            return score
        except:
            return 50
    
    @staticmethod
    def score_price_velocity(df_symbol: pd.DataFrame, days: int = 5) -> float:
        """
        Score price momentum based on recent price movement.
        
        Logic:
        - Calculate % change over last N days
        - Positive % change = bullish (50-100)
        - Negative % change = bearish (0-50)
        
        Args:
            df_symbol: DataFrame for single stock (sorted by date)
            days: Number of days for velocity calculation
            
        Returns:
            Score 0-100
        """
        try:
            if len(df_symbol) < days + 1:
                return 50  # Not enough data
            
            # Get prices
            current_price = df_symbol.iloc[-1]['close']
            past_price = df_symbol.iloc[-days-1]['close']
            
            if past_price == 0:
                return 50
            
            # Calculate % change
            pct_change = ((current_price - past_price) / past_price) * 100
            
            # Convert to 0-100 score
            # Strong positive change = 80-100
            # Small positive = 50-80
            # Small negative = 20-50
            # Strong negative = 0-20
            
            if pct_change >= 5:
                return min(100, 80 + pct_change)
            elif pct_change >= 0:
                return 50 + pct_change
            elif pct_change >= -5:
                return 50 + pct_change
            else:
                return max(0, 20 + pct_change)
        except:
            return 50
    
    @staticmethod
    def calculate_momentum_score(row: pd.Series, df_symbol: pd.DataFrame = None) -> float:
        """
        Calculate combined momentum score (0-100) for a single row.
        
        Weights:
        - RSI: 30%
        - MACD: 30%
        - Bollinger Bands: 20%
        - Price Velocity: 20%
        
        Args:
            row: DataFrame row with indicators
            df_symbol: Full DataFrame for stock (needed for velocity calculation)
            
        Returns:
            Momentum score 0-100
        """
        try:
            # Extract indicator values
            rsi = row.get('rsi_14', 50)
            macd = row.get('macd_12_26_9', 0)
            signal = row.get('macd_signal_9', 0)
            histogram = row.get('macd_histogram', 0)
            bb_upper = row.get('bollinger_upper_20', row.get('close', 0))
            bb_middle = row.get('bollinger_middle_20', row.get('close', 0))
            bb_lower = row.get('bollinger_lower_20', row.get('close', 0))
            close = row.get('close', 0)
            
            # Calculate component scores
            rsi_score = MomentumScorer.score_rsi(rsi)
            macd_score = MomentumScorer.score_macd(macd, signal, histogram)
            bb_score = MomentumScorer.score_bollinger_bands(close, bb_upper, bb_middle, bb_lower)
            
            # Price velocity score
            if df_symbol is not None and len(df_symbol) > 0:
                velocity_score = MomentumScorer.score_price_velocity(df_symbol)
            else:
                velocity_score = 50
            
            # Weighted average
            momentum_score = (
                rsi_score * 0.30 +
                macd_score * 0.30 +
                bb_score * 0.20 +
                velocity_score * 0.20
            )
            
            return max(0, min(100, momentum_score))
        
        except Exception as e:
            logger.error(f"Error calculating momentum score: {str(e)}")
            return 50
    
    @staticmethod
    def add_momentum_scores(df: pd.DataFrame) -> pd.DataFrame:
        """
        Add momentum score to DataFrame for all stocks.
        
        Args:
            df: DataFrame with technical indicators (must have 'symbol' column)
            
        Returns:
            DataFrame with added 'momentum_score' column
        """
        df = df.copy()
        df['momentum_score'] = 50.0  # Default
        
        try:
            # Calculate score for each symbol
            for symbol in df['symbol'].unique():
                mask = df['symbol'] == symbol
                symbol_data = df[mask].sort_values('timestamp').reset_index(drop=True)
                
                # Calculate scores
                scores = []
                for idx, row in symbol_data.iterrows():
                    score = MomentumScorer.calculate_momentum_score(
                        row,
                        df_symbol=symbol_data
                    )
                    scores.append(score)
                
                df.loc[mask, 'momentum_score'] = scores
            
            logger.info("Successfully added momentum scores")
            return df
        
        except Exception as e:
            logger.error(f"Error adding momentum scores: {str(e)}")
            return df
    
    @staticmethod
    def get_signal(score: float) -> str:
        """
        Get trading signal from momentum score.
        
        Args:
            score: Momentum score 0-100
            
        Returns:
            Signal string
        """
        if score >= 80:
            return "STRONG BUY"
        elif score >= 60:
            return "BUY"
        elif score >= 40:
            return "NEUTRAL"
        elif score >= 20:
            return "SELL"
        else:
            return "STRONG SELL"
    
    @staticmethod
    def get_latest_score(df: pd.DataFrame, symbol: str) -> Dict:
        """
        Get latest momentum score for a symbol.
        
        Args:
            df: DataFrame with scores
            symbol: Stock symbol
            
        Returns:
            Dictionary with score and signal
        """
        symbol_data = df[df['symbol'] == symbol]
        if symbol_data.empty:
            return {}
        
        latest = symbol_data.iloc[-1]
        score = latest.get('momentum_score', 50)
        
        return {
            'symbol': symbol,
            'date': str(latest.get('timestamp', 'N/A')),
            'close': float(latest.get('close', 0)),
            'momentum_score': float(score),
            'signal': MomentumScorer.get_signal(score),
            'rsi_14': float(latest.get('rsi_14', 50)),
            'macd': float(latest.get('macd_12_26_9', 0)),
            'bb_upper': float(latest.get('bollinger_upper_20', 0))
        }


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    # Example: Add momentum scores to indicators DataFrame
    from src.analysis.quote_fetcher import QuoteFetcher
    from src.analysis.indicators import TechnicalIndicators
    
    # Fetch data
    fetcher = QuoteFetcher()
    data = fetcher.fetch_batch(['AAPL', 'MSFT'], days=30)
    df = fetcher.combine_batches(data)
    
    # Add indicators
    df = TechnicalIndicators.add_indicators_to_dataframe(df)
    
    # Add momentum scores
    df = MomentumScorer.add_momentum_scores(df)
    
    # Show results
    for symbol in ['AAPL', 'MSFT']:
        score_data = MomentumScorer.get_latest_score(df, symbol)
        print(f"\n{symbol}:")
        print(f"  Score: {score_data['momentum_score']:.2f}")
        print(f"  Signal: {score_data['signal']}")
        print(f"  Price: ${score_data['close']:.2f}")
