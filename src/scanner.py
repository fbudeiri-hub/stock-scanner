"""
Main scanner orchestration module
Coordinates ALL 7 API providers with intelligent fallback logic
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd
from dotenv import load_dotenv

from providers.marketstack import MarketstackProvider
from providers.finnhub import FinnhubProvider
from providers.twelvedata import TwelvedataProvider
from providers.fmp import FMPProvider
from providers.tiingo import TiingoProvider
from providers.alphavantage import AlphaVantageProvider
from providers.polygon import PolygonProvider

class StockScanner:
    """Orchestrates scanning across ALL 7 data providers"""
    
    def __init__(self, env_file='.env'):
        """Initialize scanner with all 7 API credentials"""
        load_dotenv(env_file)
        
        # Initialize ALL 7 providers in priority order
        self.providers = [
            MarketstackProvider(os.getenv('MARKETSTACK_KEY', '')),
            FinnhubProvider(os.getenv('FINNHUB_KEY', '')),
            TwelvedataProvider(os.getenv('TWELVEDATA_KEY', '')),
            FMPProvider(os.getenv('FMP_KEY', '')),
            TiingoProvider(os.getenv('TIINGO_KEY', '')),
            PolygonProvider(os.getenv('POLYGON_KEY', '')),
            AlphaVantageProvider(os.getenv('ALPHAVANTAGE_KEY', ''))
        ]
        
        self.all_data = []
        self.failed_symbols = []
        self.provider_stats = {}
        
    def scan(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Scan provided symbols using all 7 providers with fallback logic
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            Dict with results and statistics
        """
        print(f"[INFO] Starting scan of {len(symbols)} symbols across 7 providers")
        print(f"[INFO] Provider priority: Marketstack → Finnhub → Twelve Data → FMP → Tiingo → Polygon → Alpha Vantage")
        print("")
        
        remaining = set(symbols)
        
        # Try each provider in sequence
        for provider_num, provider in enumerate(self.providers, 1):
            if not remaining:
                print(f"[INFO] All symbols retrieved! Stopping early.")
                break
            
            print(f"[INFO] === PROVIDER {provider_num}/7: {provider.name} ===")
            print(f"[INFO] Remaining symbols to fetch: {len(remaining)}")
            
            try:
                data = provider.fetch_data(list(remaining))
                normalized = provider.normalize_data(data)
                
                # Track which symbols we got
                retrieved = {item['symbol'] for item in normalized if item.get('symbol')}
                self.all_data.extend(normalized)
                self.provider_stats[provider.name] = len(retrieved)
                
                remaining -= retrieved
                print(f"[INFO] {provider.name}: Retrieved {len(retrieved)} symbols")
                print(f"[INFO] Remaining: {len(remaining)} symbols")
                print("")
                
            except Exception as e:
                print(f"[ERROR] {provider.name}: {e}")
                self.provider_stats[provider.name] = 0
            
            finally:
                provider.close()
        
        # Log final failed symbols
        self.failed_symbols = list(remaining)
        if self.failed_symbols:
            print(f"[WARNING] Failed to retrieve {len(self.failed_symbols)} symbols: {self.failed_symbols[:10]}...")
        
        # Calculate statistics
        coverage = (len(symbols) - len(self.failed_symbols)) / len(symbols) * 100 if symbols else 0
        
        print("")
        print(f"[INFO] ========== SCAN SUMMARY ==========")
        print(f"[INFO] Total symbols requested: {len(symbols)}")
        print(f"[INFO] Successfully retrieved: {len(symbols) - len(self.failed_symbols)}")
        print(f"[INFO] Failed: {len(self.failed_symbols)}")
        print(f"[INFO] Coverage: {coverage:.1f}%")
        print(f"[INFO] Provider breakdown:")
        for provider_name, count in sorted(self.provider_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"[INFO]   - {provider_name}: {count}")
        print(f"[INFO] ===================================")
        print("")
        
        return {
            'data': self.all_data,
            'retrieved': len(symbols) - len(self.failed_symbols),
            'total': len(symbols),
            'coverage': f"{coverage:.1f}%",
            'failed': self.failed_symbols,
            'provider_stats': self.provider_stats,
            'timestamp': datetime.now().isoformat()
        }
    
    def save_results(self, results: Dict, output_dir='data/output'):
        """Save results to CSV and JSON"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save to CSV
        csv_file = os.path.join(output_dir, f'scan_results_{timestamp}.csv')
        latest_csv = os.path.join(output_dir, 'scan_results_latest.csv')
        
        if results['data']:
            df = pd.DataFrame(results['data'])
            df.to_csv(csv_file, index=False)
            df.to_csv(latest_csv, index=False)
            print(f"[INFO] Results saved to {csv_file}")
        
        # Save statistics
        stats = {
            'timestamp': results['timestamp'],
            'total_symbols': results['total'],
            'retrieved': results['retrieved'],
            'coverage': results['coverage'],
            'failed_symbols': results['failed'],
            'provider_breakdown': results['provider_stats']
        }
        
        json_file = os.path.join(output_dir, f'scan_stats_{timestamp}.json')
        with open(json_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        print(f"[INFO] Statistics: {results['coverage']} coverage")
        
        return csv_file, json_file
