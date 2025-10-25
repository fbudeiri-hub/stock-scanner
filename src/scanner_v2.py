"""
Scanner v2 - Smarter, faster provider strategy
Uses timeout-based detection + intelligent fallback
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Any, Set
import pandas as pd
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from providers.finnhub import FinnhubProvider
from providers.marketstack import MarketstackProvider

class ScannerV2:
    """
    Simplified, production-ready scanner
    Focus: Speed + Reliability over perfection
    """
    
    def __init__(self, env_file='.env'):
        """Initialize with ONLY proven providers"""
        load_dotenv(env_file)
        
        # Use only 2 providers (proven to work)
        self.primary_provider = FinnhubProvider(os.getenv('FINNHUB_KEY', ''))
        self.fallback_provider = MarketstackProvider(os.getenv('MARKETSTACK_KEY', ''))
        
        self.all_data = []
        self.failed_symbols = []
        self.stats = {'finnhub': 0, 'marketstack': 0}
        
    def scan(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Smart scan with timeout-based fallback
        
        Strategy:
        1. Try Finnhub first (fast, 60/min limit but covers most)
        2. For failures, try Marketstack
        3. Track what works best
        """
        print(f"[INFO] Scanning {len(symbols)} symbols")
        print(f"[INFO] Strategy: Try Finnhub → Fallback to Marketstack")
        print("")
        
        remaining = set(symbols)
        
        # ===== STRATEGY 1: Use Finnhub for individual symbols =====
        print(f"[INFO] PHASE 1: Finnhub (individual symbols)")
        print(f"[INFO] Limit: 60/minute (1.2s per call)")
        print(f"[INFO] Processing {len(remaining)} symbols...")
        print("")
        
        finnhub_data = self._fetch_with_provider(
            self.primary_provider, 
            list(remaining),
            timeout_per_call=5
        )
        
        retrieved_finnhub = {item['symbol'] for item in finnhub_data if item.get('symbol')}
        self.all_data.extend(finnhub_data)
        self.stats['finnhub'] = len(retrieved_finnhub)
        remaining -= retrieved_finnhub
        
        print(f"[INFO] Finnhub: Retrieved {len(retrieved_finnhub)} symbols")
        print(f"[INFO] Remaining: {len(remaining)} symbols")
        print("")
        
        # ===== STRATEGY 2: Use Marketstack for batch fetching (fallback) =====
        if remaining:
            print(f"[INFO] PHASE 2: Marketstack (batch fallback)")
            print(f"[INFO] Limit: 100/day (conservative)")
            print(f"[INFO] Processing {len(remaining)} symbols in batches...")
            print("")
            
            marketstack_data = self._fetch_marketstack_batches(
                list(remaining),
                batch_size=30  # Small batches for free tier
            )
            
            retrieved_marketstack = {item['symbol'] for item in marketstack_data if item.get('symbol')}
            self.all_data.extend(marketstack_data)
            self.stats['marketstack'] = len(retrieved_marketstack)
            remaining -= retrieved_marketstack
            
            print(f"[INFO] Marketstack: Retrieved {len(retrieved_marketstack)} symbols")
            print(f"[INFO] Final remaining: {len(remaining)} symbols")
            print("")
        
        self.failed_symbols = list(remaining)
        coverage = (len(symbols) - len(self.failed_symbols)) / len(symbols) * 100 if symbols else 0
        
        print(f"[INFO] ========== FINAL RESULTS ==========")
        print(f"[INFO] Total requested: {len(symbols)}")
        print(f"[INFO] Successfully retrieved: {len(symbols) - len(self.failed_symbols)}")
        print(f"[INFO] Coverage: {coverage:.1f}%")
        print(f"[INFO] Finnhub: {self.stats['finnhub']}")
        print(f"[INFO] Marketstack: {self.stats['marketstack']}")
        print(f"[INFO] Failed: {len(self.failed_symbols)}")
        print(f"[INFO] ===================================")
        print("")
        
        return {
            'data': self.all_data,
            'retrieved': len(symbols) - len(self.failed_symbols),
            'total': len(symbols),
            'coverage': f"{coverage:.1f}%",
            'failed': self.failed_symbols,
            'stats': self.stats,
            'timestamp': datetime.now().isoformat()
        }
    
    def _fetch_with_provider(self, provider, symbols: List[str], timeout_per_call: int) -> List[Dict]:
        """Fetch symbols with single provider (with timeout)"""
        all_data = []
        successful = 0
        failed = 0
        
        for idx, symbol in enumerate(symbols):
            try:
                # Quick timeout check
                params = {'symbol': symbol, 'token': provider.api_key} if hasattr(provider, 'api_key') else {}
                response = provider._make_request(provider.base_url, params)
                
                if response and ('c' in response or 'close' in str(response).lower()):
                    all_data.append({
                        'symbol': symbol,
                        'quote': response
                    })
                    successful += 1
                else:
                    failed += 1
                
                # Progress every 20
                if (idx + 1) % 20 == 0:
                    print(f"[INFO] {provider.name}: {idx + 1}/{len(symbols)} symbols processed")
                    
            except Exception as e:
                failed += 1
                if "429" in str(e) or "rate" in str(e).lower():
                    print(f"[WARNING] {provider.name}: Rate limit hit at {idx + 1}/{len(symbols)}")
                    break
            
            # Smart delay
            if idx < len(symbols) - 1:
                time.sleep(1.5)  # Conservative 1.5s
        
        print(f"[INFO] {provider.name}: {successful} success, {failed} failed")
        return all_data
    
    def _fetch_marketstack_batches(self, symbols: List[str], batch_size: int) -> List[Dict]:
        """Fetch Marketstack with small batches"""
        all_data = []
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        
        for batch_num, i in enumerate(range(0, len(symbols), batch_size)):
            batch = symbols[i:i + batch_size]
            symbol_str = ','.join(batch)
            
            print(f"[INFO] Marketstack: Batch {batch_num + 1}/{total_batches}")
            
            params = {'symbols': symbol_str, 'access_key': self.fallback_provider.api_key, 'limit': 1}
            
            try:
                response = self.fallback_provider._make_request(
                    self.fallback_provider.base_url,
                    params
                )
                
                if response and 'data' in response:
                    all_data.extend(response['data'])
                    print(f"[INFO] Marketstack: Got {len(response['data'])} from batch")
                    
            except Exception as e:
                print(f"[ERROR] Marketstack batch {batch_num + 1}: {e}")
                if "429" in str(e):
                    print(f"[WARNING] Rate limit on Marketstack. Stopping.")
                    break
            
            if batch_num < total_batches - 1:
                time.sleep(3)  # 3 second delay between batches
        
        return all_data
    
    def save_results(self, results: Dict, output_dir='data/output'):
        """Save to CSV and JSON"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Normalize data
        normalized = []
        for item in results['data']:
            if 'quote' in item:
                quote = item['quote']
                normalized.append({
                    'symbol': item.get('symbol'),
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'close': quote.get('c'),
                    'price': quote.get('price'),
                    'provider': 'finnhub'
                })
            elif 'symbol' in item:
                normalized.append({
                    'symbol': item.get('symbol'),
                    'date': item.get('date'),
                    'close': item.get('close'),
                    'price': item.get('close'),
                    'provider': 'marketstack'
                })
        
        # Save CSV
        if normalized:
            df = pd.DataFrame(normalized)
            csv_file = os.path.join(output_dir, f'scan_results_{timestamp}.csv')
            df.to_csv(csv_file, index=False)
            
            # Also save as latest
            latest_csv = os.path.join(output_dir, 'scan_results_latest.csv')
            df.to_csv(latest_csv, index=False)
            
            print(f"[INFO] CSV saved to {csv_file}")
        
        # Save stats
        stats = {
            'timestamp': results['timestamp'],
            'total': results['total'],
            'retrieved': results['retrieved'],
            'coverage': results['coverage'],
            'stats': results['stats']
        }
        
        json_file = os.path.join(output_dir, f'scan_stats_{timestamp}.json')
        with open(json_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        return csv_file, json_file
