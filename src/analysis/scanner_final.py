import pandas as pd
import numpy as np
import logging
from concurrent.futures import ThreadPoolExecutor
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockScanner:
    def __init__(self):
        self.batch_size = 100
        self.workers = 6
        
    def run(self, tickers_file='data/tickers_5000.csv', output_dir='data/output'):
        """Scan FULL 5000 stocks"""
        # Load ALL 5000 tickers
        df_tickers = pd.read_csv(tickers_file)
        symbols = df_tickers['symbol'].tolist()
        
        logger.info(f"🚀 Starting scan of {len(symbols)} STOCKS")
        
        from src.analysis.quote_fetcher import QuoteFetcher
        from src.analysis.indicators import TechnicalIndicators
        from src.analysis.momentum_scorer import MomentumScorer
        from src.trading.entry_exit_calculator import EntryExitCalculator
        
        fetcher = QuoteFetcher()
        indicators = TechnicalIndicators()
        scorer = MomentumScorer()
        calculator = EntryExitCalculator()
        
        # Parallel batch fetch
        logger.info(f"📊 Fetching data with {self.workers} workers...")
        batch_data = fetcher.fetch_batch(symbols, workers=self.workers)
        logger.info(f"✅ Retrieved {len(batch_data)}/{len(symbols)} stocks")
        
        if not batch_data:
            logger.error("❌ No data retrieved!")
            return
        
        # Process with indicators
        logger.info("📈 Calculating indicators...")
        for symbol, df in batch_data.items():
            try:
                batch_data[symbol] = indicators.add_indicators_to_dataframe(df)
            except Exception as e:
                logger.warning(f"⚠ Indicator calc failed for {symbol}: {e}")
        
        # Score momentum
        logger.info("⚡ Scoring momentum...")
        scores = scorer.calculate_momentum_scores(batch_data)
        
        # Filter & rank
        if not scores:
            logger.error("❌ No momentum scores!")
            return
            
        df_results = pd.DataFrame(list(scores.items()), columns=['symbol', 'momentum_score'])
        df_results = df_results.sort_values('momentum_score', ascending=False)
        
        # Calculate trading levels
        logger.info("💰 Calculating entry/exit prices...")
        for idx, row in df_results.iterrows():
            symbol = row['symbol']
            if symbol in batch_data:
                levels = calculator.calculate_levels(batch_data[symbol])
                df_results.loc[idx, 'entry'] = levels.get('entry', np.nan)
                df_results.loc[idx, 'stop_loss'] = levels.get('stop_loss', np.nan)
                df_results.loc[idx, 'profit_target'] = levels.get('profit_target', np.nan)
        
        # Save results
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{output_dir}/scan_results_latest.csv"
        df_results.to_csv(output_file, index=False)
        
        logger.info(f"✅ SCAN COMPLETE!")
        logger.info(f"📁 Results: {output_file}")
        logger.info(f"🏆 Top 10 signals:")
        print(df_results.head(10).to_string())

if __name__ == '__main__':
    scanner = StockScanner()
    scanner.run()
