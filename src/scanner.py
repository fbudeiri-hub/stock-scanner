"""
Main scanner orchestration module
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

class StockScanner:
    """Orchestrates scanning across multiple data providers"""
    
    def __init__(self, env_file='.env'):
        """Initialize scanner with API credentials"""
        load_dotenv(env_file)
        
        # Initialize providers
        self.providers = [
            MarketstackProvider(os.getenv('MARKETSTACK_KEY', '')),
            FinnhubProvider(os.getenv('FINNHUB_KEY', '')),
            TwelvedataProvider(os.getenv('TWELVEDATA_KEY', ''))
        ]
        
        self.all_data = []
        self.failed_symbols = []
        
    def scan(self, symbols: List[str]) -> Dict[str, Any]:
        """Scan provided symbols using multiple providers"""
        print(f"[INFO] Starting scan of {len(symbols)} symbols")
        
        remaining = set(symbols)
        
        # Try each provider in order
        for provider in self.providers:
            if not remaining:
                break
                
            print(f"[INFO] Using provider: {provider.name}")
            
            try:
                data = provider.fetch_data(list(remaining))
                normalized = provider.normalize_data(data)
                
                # Track which symbols we got
                retrieved = {item['symbol'] for item in normalized if item.get('symbol')}
                self.all_data.extend(normalized)
                
                remaining -= retrieved
                print(f"[INFO] {provider.name}: Retrieved {len(retrieved)} symbols")
                
            except Exception as e:
                print(f"[ERROR] Error with {provider.name}: {e}")
            
            finally:
                provider.close()
        
        # Log failed symbols
        self.failed_symbols = list(remaining)
        if self.failed_symbols:
            print(f"[WARNING] Failed to retrieve: {self.failed_symbols}")
        
        # Calculate statistics
        coverage = (len(symbols) - len(self.failed_symbols)) / len(symbols) * 100
        
        return {
            'data': self.all_data,
            'retrieved': len(symbols) - len(self.failed_symbols),
            'total': len(symbols),
            'coverage': f"{coverage:.1f}%",
            'failed': self.failed_symbols,
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
            'failed_symbols': results['failed']
        }
        
        json_file = os.path.join(output_dir, f'scan_stats_{timestamp}.json')
        with open(json_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        print(f"[INFO] Statistics: {stats['coverage']} coverage")
        
        return csv_file, json_file
