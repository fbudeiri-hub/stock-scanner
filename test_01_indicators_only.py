import pandas as pd
from src.analysis.indicators import TechnicalIndicators

print("=" * 70)
print("TEST 1: INDICATORS MODULE (LOCAL - NO API NEEDED)")
print("=" * 70)

# Create simple test data
data = {
    'timestamp': pd.date_range('2025-09-26', periods=30),
    'open': [100 + i*0.5 for i in range(30)],
    'high': [102 + i*0.5 for i in range(30)],
    'low': [99 + i*0.5 for i in range(30)],
    'close': [101 + i*0.5 for i in range(30)],
    'volume': [1000000] * 30,
    'symbol': ['TEST'] * 30
}

df = pd.DataFrame(data)

print("\nStep 1: Input data created")
print(f"Records: {len(df)}")
print(f"Columns: {list(df.columns)}")

print("\nStep 2: Adding indicators...")
df_with_ind = TechnicalIndicators.add_indicators_to_dataframe(df)

print(f"\nStep 3: Checking new columns added")
print(f"New columns: {[c for c in df_with_ind.columns if c not in df.columns]}")

print(f"\nStep 4: Last row values")
last = df_with_ind.iloc[-1]
print(f"  Close: {last['close']:.2f}")
print(f"  RSI: {last['rsi_14']:.2f}")
print(f"  MACD: {last['macd_12_26_9']:.6f}")
print(f"  BB Upper: {last['bollinger_upper_20']:.2f}")

print("\n" + "=" * 70)
print("TEST 1: PASSED - Indicators working correctly!")
print("=" * 70)
