"""
Main entry point for stock scanner application
"""

import sys
import os
from datetime import datetime

# Fix the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scanner import StockScanner
from merger import DataMerger

def load_tickers(csv_file='data/tickers.csv'):
    """Load ticker symbols from CSV file"""
    try:
        import csv
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            tickers = [row['symbol'].strip().upper() for row in reader]
        return tickers
    except Exception as e:
        print(f"Error loading tickers: {e}")
        return []

def main():
    """Main execution function"""
    
    print("=" * 60)
    print("Stock Scanner Starting")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)
    
    # Load tickers
    print(f"Loading tickers from data/tickers.csv")
    tickers = load_tickers('data/tickers.csv')
    
    if not tickers:
        print("ERROR: No tickers loaded. Exiting.")
        return 1
    
    print(f"Loaded {len(tickers)} tickers: {tickers[:5]}...")
    
    # Run scanner
    print("Starting scan...")
    scanner = StockScanner()
    results = scanner.scan(tickers)
    
    print(f"Scan complete: {results['coverage']} coverage")
    
    # Merge and validate data
    if results['data']:
        merger = DataMerger()
        df = merger.merge_provider_data(results['data'])
        
        print(f"Before validation: {len(df)} records")
        df = merger.validate_data(df)
        print(f"After validation: {len(df)} records")
        
        df = merger.deduplicate(df)
        print(f"After deduplication: {len(df)} records")
        
        stats = merger.get_statistics(df, len(tickers))
        print(f"Statistics: {stats}")
    
    # Save results
    csv_file, json_file = scanner.save_results(results, 'data/output')
    
    print("=" * 60)
    print("Stock Scanner Complete")
    print(f"CSV Output: {csv_file}")
    print(f"JSON Output: {json_file}")
    print("=" * 60)
    
    return 0

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)


