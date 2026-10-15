"""Canonical public scanner entrypoint: no alternate unvalidated ScannerV2 path."""
from src.analysis.scanner_full import FullStockScanner

def main():
    import argparse
    parser=argparse.ArgumentParser(description='Historical research scanner; no trading')
    parser.add_argument('--tickers',default='data/tickers_5000.csv')
    args=parser.parse_args()
    FullStockScanner().run(tickers_file=args.tickers)
    return 0
if __name__=='__main__':
    raise SystemExit(main())
