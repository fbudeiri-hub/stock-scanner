from src.analysis.scanner_integration import ScannerIntegration

print("TEST 5: Scanner Integration (Batch Processing)")
print("=" * 70)

# Initialize scanner (4 workers, 50 stocks per batch)
scanner = ScannerIntegration(batch_size=50, max_workers=4)

# Test with 10 symbols (represents 932 scan)
test_symbols = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
    'META', 'NVDA', 'JPM', 'V', 'WMT'
]

print(f"\nScanning {len(test_symbols)} stocks...\n")

# Run scan
results = scanner.scan_all(test_symbols, days=30, score_threshold=50)

# Save
if not results.empty:
    filepath = scanner.save_results(results)
    print(f"\nResults saved to: {filepath}")
    print(f"\nTop scores:")
    print(results[['symbol', 'momentum_score', 'signal']].head(10))
    
    summary = scanner.get_summary(results)
    print(f"\nSummary:")
    print(f"  Total: {summary['total_scanned']}")
    print(f"  Avg Score: {summary['avg_score']:.1f}")
    print(f"  Top: {summary['top_stock']} ({summary['top_score']:.1f})")
