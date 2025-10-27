"""
Entry/Exit Price Calculator - Professional Trading Signals
Minimal new module - reuses existing indicators data
"""
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class EntryExitCalculator:
    """Calculate entry prices, stop losses, and take profit levels."""
    
    def __init__(self, use_atr: bool = True, default_risk_pct: float = 0.02):
        self.use_atr = use_atr
        self.default_risk_pct = default_risk_pct
    
    def calculate_entry(self, symbol: str, current_price: float, 
                       indicators: pd.DataFrame, score: float) -> float:
        """
        Calculate entry price based on technical signal strength.
        
        Strategy:
        - High score (80+): Enter immediately at current
        - Medium score (60-80): Enter 2% below current  
        - Low score (50-60): Enter 5% below current (dip entry)
        
        Returns:
            float: Recommended entry price
        """
        try:
            if score >= 80:
                # Aggressive entry - momentum is strong
                entry = current_price
                strategy = "immediate"
            elif score >= 65:
                # Moderate entry - slight dip expected
                entry = current_price * 0.98  # 2% below
                strategy = "dip"
            else:
                # Conservative entry - wait for pullback
                entry = current_price * 0.95  # 5% below
                strategy = "pullback"
            
            logger.info(f"{symbol}: Entry {entry:.2f} ({strategy} strategy)")
            return round(entry, 2)
        
        except Exception as e:
            logger.warning(f"Error calculating entry for {symbol}: {e}")
            return current_price
    
    def calculate_stop_loss(self, entry_price: float, current_price: float, 
                           volatility: float) -> float:
        """
        Calculate stop loss level.
        
        Strategy:
        - Use ATR if available (professional standard)
        - Otherwise use percentage-based
        - Minimum 1% loss, maximum 5% loss
        
        Returns:
            float: Stop loss price
        """
        try:
            if volatility > 0:
                # ATR-based: use 2× ATR as stop
                stop_loss = entry_price - (volatility * 2)
            else:
                # Percentage-based: use 3% loss
                stop_loss = entry_price * 0.97
            
            # Enforce boundaries
            max_loss_pct = 0.05  # Don't risk more than 5%
            min_stop = entry_price * (1 - max_loss_pct)
            
            stop_loss = max(stop_loss, min_stop)
            
            logger.debug(f"Stop loss: {stop_loss:.2f} from entry {entry_price:.2f}")
            return round(stop_loss, 2)
        
        except Exception as e:
            logger.warning(f"Error calculating stop loss: {e}")
            return entry_price * 0.97
    
    def calculate_take_profit(self, entry_price: float, 
                             risk_reward_ratio: float = 2.0) -> float:
        """
        Calculate take profit level based on risk/reward ratio.
        
        Professional standard: 1:2 or 1:3 risk/reward
        Formula: TP = Entry + (Entry - SL) × RRR
        
        Returns:
            float: Take profit price
        """
        try:
            # For simplicity, assume standard 2.5% risk
            stop_loss = entry_price * 0.97
            risk = entry_price - stop_loss
            
            # Apply risk/reward ratio
            take_profit = entry_price + (risk * risk_reward_ratio)
            
            logger.debug(f"Take profit: {take_profit:.2f} from entry {entry_price:.2f}")
            return round(take_profit, 2)
        
        except Exception as e:
            logger.warning(f"Error calculating take profit: {e}")
            return entry_price * 1.05
    
    def get_trading_summary(self, symbol: str, current: float, entry: float, 
                           stop: float, tp: float) -> dict:
        """Get summary of recommended trade."""
        risk = entry - stop
        reward = tp - entry
        
        return {
            'symbol': symbol,
            'current_price': current,
            'entry_price': entry,
            'stop_loss': stop,
            'take_profit': tp,
            'risk_per_share': risk,
            'reward_per_share': reward,
            'risk_reward_ratio': reward / risk if risk > 0 else 0,
            'risk_percentage': (risk / entry) * 100,
            'reward_percentage': (reward / entry) * 100
        }

