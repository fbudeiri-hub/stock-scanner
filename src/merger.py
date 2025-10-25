"""
Data merger and validator module
"""

import pandas as pd
from typing import List, Dict, Any

class DataMerger:
    """Merge and validate data from multiple providers"""
    
    @staticmethod
    def merge_provider_data(all_data: List[Dict]) -> pd.DataFrame:
        """Merge data from multiple providers"""
        if not all_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_data)
        return df
    
    @staticmethod
    def validate_data(df: pd.DataFrame) -> pd.DataFrame:
        """Validate stock data"""
        if df.empty:
            return df
        
        # Remove rows with missing prices
        df = df.dropna(subset=['close'])
        
        # Remove rows with zero or negative prices
        df = df[df['close'] > 0]
        
        # Ensure volume is numeric
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        
        # Remove rows with invalid volume
        df = df[df['volume'] > 0]
        
        return df.dropna()
    
    @staticmethod
    def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate entries"""
        if df.empty:
            return df
        
        # Sort by date (newest first)
        df = df.sort_values('date', ascending=False)
        
        # Keep first occurrence of each symbol
        df = df.drop_duplicates(subset=['symbol'], keep='first')
        
        return df.reset_index(drop=True)
    
    @staticmethod
    def get_statistics(df: pd.DataFrame, total_symbols: int) -> Dict:
        """Calculate statistics from merged data"""
        if df.empty:
            return {
                'unique_symbols': 0,
                'total_records': 0,
                'coverage': 0,
                'avg_volume': 0
            }
        
        return {
            'unique_symbols': df['symbol'].nunique(),
            'total_records': len(df),
            'coverage': (df['symbol'].nunique() / total_symbols * 100) if total_symbols > 0 else 0,
            'avg_volume': df['volume'].mean(),
            'price_range': f"{df['close'].min():.2f} - {df['close'].max():.2f}"
        }
