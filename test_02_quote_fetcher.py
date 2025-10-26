from src.analysis.quote_fetcher import QuoteFetcher
import os
from dotenv import load_dotenv

load_dotenv()

print("=" * 70)
print("TEST 2: QUOTE FETCHER (yfinance + Polygon Fallback)")
print("=" * 70)

# Initialize
polygon_key = os.getenv('POLYGON_KEY')
fetcher = QuoteFetcher(polygon_key=polygon_key)

print(f"\nConfiguration:")
print(f"  Polygon Key configured: {bool(polygon_key)}")

# Test with 3 stocks (quick)
test_symbols = ['AAPL', 'MSFT', 'GOOGL']

print(f"\nFetching {len(test_symbols)} stocks...")
print("-" * 70)

# Fetch
data_dict = fetcher.fetch_batch(test_symbols, days=30)

print(f"\nResults:")
print(f"  Successfully fetched: {len(data_dict)}/{len(test_symbols)}")

# Combine
combined = fetcher.combine_batches(data_dict)

print(f"  Total records: {len(combined)}")
print(f"  Unique symbols: {combined['symbol'].nunique()}")

print(f"\nData sample:")
print(combined[['symbol', 'timestamp', 'close']].head(10))

print("\n" + "=" * 70)
if len(data_dict) == len(test_symbols):
    print("TEST 2: PASSED - Quote Fetcher working!")
else:
    print(f"TEST 2: PARTIAL - Got {len(data_dict)}/{len(test_symbols)} stocks")
print("=" * 70)
