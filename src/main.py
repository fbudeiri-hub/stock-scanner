"""
Main entry point - Updated to use ScannerV2
"""

import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scanner_v2 import ScannerV2
from merger import DataMerger

def load_tickers(csv_file='data/tickers_100.csv'):
    """Load tickers"""
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
    """Main function"""
    
    parser = argparse.ArgumentParser(description='Stock Scanner V2')
    parser.add_argument('--tickers', 
                       default='data/tickers_100.csv', 
                       help='Tickers CSV file')
    args = parser.parse_args()
    
    print("=" * 60)
    print("Stock Scanner V2 - Simplified & Fast")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)
    print("")
    
    # Load tickers
    tickers_file = args.tickers
    print(f"Loading tickers from {tickers_file}")
    tickers = load_tickers(tickers_file)
    
    if not tickers:
        print("ERROR: No tickers loaded")
        return 1
    
    print(f"Loaded {len(tickers)} tickers")
    print("")
    
    # Scan
    scanner = ScannerV2()
    results = scanner.scan(tickers)
    
    # Save
    csv_file, json_file = scanner.save_results(results, 'data/output')
    
    print("")
    print("=" * 60)
    print("Scan Complete!")
    print(f"Output: {csv_file}")
    print("=" * 60)
    
    return 0

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)

