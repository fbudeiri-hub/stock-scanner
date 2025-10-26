from src.analysis.quote_fetcher import QuoteFetcher
from src.analysis.indicators import TechnicalIndicators
from src.analysis.momentum_scorer import MomentumScorer

print("TEST 4: Momentum Scorer")
fetcher = QuoteFetcher()
data = fetcher.fetch_batch(['AAPL', 'MSFT', 'GOOGL'], days=30)
df = fetcher.combine_batches(data)
df = TechnicalIndicators.add_indicators_to_dataframe(df)
df = MomentumScorer.add_momentum_scores(df)

for symbol in ['AAPL', 'MSFT', 'GOOGL']:
    score = MomentumScorer.get_latest_score(df, symbol)
    print(f"{symbol}: Score={score['momentum_score']:.1f}, Signal={score['signal']}")
