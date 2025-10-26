import os
import sys
from src.analysis.historical_fetcher import HistoricalDataFetcher
from src.analysis.indicators import TechnicalIndicators

# Get API key
FINNHUB_KEY = os.getenv('FINNHUB_KEY')
if not FINNHUB_KEY:
    print("ERROR: FINNHUB_KEY environment variable not set")
    sys.exit(1)

print("Testing Technical Indicators Pipeline...")
print("=" * 60)

# Initialize fetcher
fetcher = HistoricalDataFetcher(FINNHUB_KEY)

# Test with 3 stocks
symbols = ['AAPL', 'MSFT', 'GOOGL']
print(f"\nFetching 30 days of data for: {symbols}")

# Fetch historical data
historical_dict = fetcher.fetch_batch(symbols, days=30)

if not historical_dict:
    print("ERROR: No data fetched")
    sys.exit(1)

print(f"Successfully fetched data for {len(historical_dict)} stocks")

# Combine
combined_df = fetcher.combine_batches(historical_dict)
print(f"Combined DataFrame shape: {combined_df.shape}")

# Add indicators
print("\nCalculating technical indicators...")
df_with_indicators = TechnicalIndicators.add_indicators_to_dataframe(combined_df)

print("\nLatest Data with Indicators:")
print("=" * 60)

# Show latest for each stock
for symbol in symbols:
    latest = TechnicalIndicators.get_latest_indicators(df_with_indicators, symbol)
    if latest:
        print(f"\n{symbol}:")
        print(f"  Close Price: ${latest['close']:.2f}")
        print(f"  RSI (14): {latest['rsi_14']:.2f} (0-100)")
        print(f"  MACD: {latest['macd']:.6f}")
        print(f"  MACD Signal: {latest['macd_signal']:.6f}")
        print(f"  MACD Histogram: {latest['macd_histogram']:.6f}")
        print(f"  BB Upper: ${latest['bb_upper']:.2f}")
        print(f"  BB Middle: ${latest['bb_middle']:.2f}")
        print(f"  BB Lower: ${latest['bb_lower']:.2f}")

print("\n" + "=" * 60)
print("Test Complete! All indicators calculated successfully.")
