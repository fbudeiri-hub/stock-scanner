from src.analysis.quote_fetcher import QuoteFetcher
from src.analysis.indicators import TechnicalIndicators
import os
from dotenv import load_dotenv

load_dotenv()

print("=" * 70)
print("TEST 3: FULL PIPELINE (Quote Fetcher + Indicators)")
print("=" * 70)

# Step 1: Initialize fetcher
polygon_key = os.getenv('POLYGON_KEY')
fetcher = QuoteFetcher(polygon_key=polygon_key)

# Step 2: Fetch historical data
test_symbols = ['AAPL', 'MSFT', 'GOOGL']
print(f"\nStep 1: Fetching historical data for {len(test_symbols)} stocks...")
data_dict = fetcher.fetch_batch(test_symbols, days=30)

# Step 3: Combine
print(f"\nStep 2: Combining data...")
combined_df = fetcher.combine_batches(data_dict)
print(f"  Combined {len(data_dict)} stocks: {len(combined_df)} total records")

# Step 4: Add indicators
print(f"\nStep 3: Calculating technical indicators...")
df_with_indicators = TechnicalIndicators.add_indicators_to_dataframe(combined_df)

# Step 5: Show results
print(f"\nStep 4: Results for latest day of each stock...")
print("-" * 70)

for symbol in test_symbols:
    latest = TechnicalIndicators.get_latest_indicators(df_with_indicators, symbol)
    if latest:
        print(f"\n{symbol}:")
        print(f"  Date: {latest['date']}")
        print(f"  Price: ${latest['close']:.2f}")
        print(f"  RSI(14): {latest['rsi_14']:.2f} {'(Overbought)' if latest['rsi_14'] > 70 else '(Oversold)' if latest['rsi_14'] < 30 else '(Neutral)'}")
        print(f"  MACD: {latest['macd']:.6f}")
        print(f"  Bollinger Upper: ${latest['bb_upper']:.2f}")

# Step 6: Save
output_file = 'data/test_full_pipeline_output.csv'
fetcher.save_to_csv(df_with_indicators, output_file)

print("\n" + "=" * 70)
print(f"TEST 3: PASSED - Full pipeline working!")
print(f"Output saved to: {output_file}")
print("=" * 70)
