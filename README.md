# Historical stock scanner: data-integrity repair candidate

This repository is a historical research scanner, not a trade executor. The active CLI, full scan and focus scan now share one reviewed fetch -> indicators -> score -> latest-per-symbol path. A focus scan means fewer symbols, not real-time intraday data.

Based on `main` at `b03b3ebe0f6b4c7b363adbd7bb9425cf6c19f80c`. The repair is an overlay; unmodified historical alternatives are retained in Git but are not certified. Use the entrypoints below rather than old ScannerV2 or demo scripts.

## Changes

Normalize flat or multi-level yfinance columns to one timestamped OHLCV schema. Reject malformed bars, handle empty batches, return string ticker keys, and remove invented fallback prices. Calculate Wilder RSI and EMA MACD with explicit warm-up. Score each bar only using its historical prefix, and rank the latest bar instead of a stock's best historical score. Missing/failed inputs cannot become a default neutral score.

Full scan writes an atomic CSV, a latest watchlist and a JSON dashboard manifest. An empty candidate list stays empty; it does not fall back to historical peaks. The dashboard fetches the actual manifest, displays dates and stale/unavailable status, and no longer claims hard-coded 2025 results are real time. HTML browser acceptance remains untested.

## Test and run

```bash
python -m pip install -r requirements-test.txt
python -m pytest tests/
# Provider access is separate and was not tested against the live service here.
python -m pip install -r requirements.txt
python -m src.main --tickers data/tickers_5000.csv
python -m src.analysis.scanner_full --days 120 --threshold 50
python -m src.analysis.scanner_momentum_focus --days 120 --threshold 40
```

Serve the repository directory through a local static HTTP server to inspect `index.html`; it reads `data/output/scan_latest.json`. Do not interpret an available web page as proof of fresh market data. No credentials belong in this public repository. Rate limits, provider entitlements, symbols, corporate actions, complete universe coverage, real-data results and browser appearance still require acceptance testing.

The replacement workflow performs offline validation only. It neither runs market scans nor commits runtime market data. Passing fixture tests proves those scenarios, not profitable signals or complete production readiness.
