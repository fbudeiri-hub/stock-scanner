import pandas as pd
import numpy as np
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockScanner:
    """
    Production-grade stock scanner for 5000+ US stocks
    with technical indicators, momentum scoring, and sentiment analysis
    """
    
    def __init__(self, batch_size=100, workers=6):
        self.batch_size = batch_size
        self.workers = workers

    def run(self, tickers_file='data/tickers_5000.csv', output_dir='data/output', 
            days=30, threshold=50):
        """
        Main scanner execution:
        1. Load tickers from CSV
        2. Fetch historical data from multiple providers
        3. Calculate technical indicators
        4. Score momentum
        5. Calculate entry/exit prices
        6. Save results
        """
        
        try:
            # Load tickers
            logger.info("Loading ticker list...")
            df_tickers = pd.read_csv(tickers_file)
            symbols = df_tickers['symbol'].tolist()
            logger.info(f"🚀 Starting scan of {len(symbols)} STOCKS")
            
            # Import modules
            from src.analysis.quote_fetcher import QuoteFetcher
            from src.analysis.indicators import TechnicalIndicators
            from src.analysis.momentum_scorer import MomentumScorer
            from src.trading.entry_exit_calculator import EntryExitCalculator
            
            fetcher = QuoteFetcher()
            indicators = TechnicalIndicators()
            scorer = MomentumScorer()
            calculator = EntryExitCalculator()
            
            # Parallel batch fetch with days parameter
            logger.info(f"📊 Fetching {days} days of data with {self.workers} workers...")
            batch_data = fetcher.fetch_batch(symbols, days=days, workers=self.workers)
            logger.info(f"✅ Retrieved {len(batch_data)}/{len(symbols)} stocks")
            
            if not batch_data:
                logger.error("❌ No data retrieved!")
                return
            
            # Process with indicators
            logger.info("📈 Calculating technical indicators...")
            for symbol, df in batch_data.items():
                try:
                    batch_data[symbol] = indicators.add_indicators_to_dataframe(df)
                except Exception as e:
                    logger.warning(f"⚠ Indicator calc failed for {symbol}: {e}")
            
            # Score momentum
            logger.info("⚡ Scoring momentum...")
            all_data = pd.concat(batch_data.values(), ignore_index=False)
            all_data = scorer.add_momentum_scores(all_data)
            
            # Filter & rank above threshold
            df_results = all_data[all_data['momentum_score'] >= threshold].copy()
            df_results = df_results.sort_values('momentum_score', ascending=False)
            df_results = df_results.drop_duplicates(subset=['symbol'], keep='first')
            
            if df_results.empty:
                logger.warning(f"⚠ No stocks above threshold {threshold}")
                df_results = all_data.nlargest(100, 'momentum_score')
            
            # Calculate trading levels (entry/exit/stop)
            logger.info("💰 Calculating entry/exit prices...")
            for idx, row in df_results.iterrows():
                symbol = row['symbol']
                if symbol in batch_data:
                    try:
                        levels = calculator.calculate_levels(batch_data[symbol])
                        df_results.loc[idx, 'entry'] = levels.get('entry', np.nan)
                        df_results.loc[idx, 'stop_loss'] = levels.get('stop_loss', np.nan)
                        df_results.loc[idx, 'profit_target'] = levels.get('profit_target', np.nan)
                    except Exception as e:
                        logger.warning(f"⚠ Entry/exit calc failed for {symbol}: {e}")
            
            # Save results with timestamp
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{output_dir}/scan_results_{timestamp}.csv"
            df_results.to_csv(output_file, index=False)
            
            logger.info(f"✅ SCAN COMPLETE!")
            logger.info(f"📁 Results saved: {output_file}")
            logger.info(f"📊 Total recommendations: {len(df_results)}")
            logger.info(f"🏆 Top 10 recommendations:")
            print(df_results.head(10).to_string())
            
            return df_results

        except Exception as e:
            logger.error(f"❌ Scanner failed: {e}")
            raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Production Stock Scanner')
    parser.add_argument('--tickers', type=str, default='data/tickers_5000.csv', 
                        help='Path to tickers CSV file')
    parser.add_argument('--output-dir', type=str, default='data/output', 
                        help='Output directory for results')
    parser.add_argument('--batch-size', type=int, default=100, 
                        help='Batch size for processing')
    parser.add_argument('--workers', type=int, default=6, 
                        help='Number of parallel workers')
    parser.add_argument('--days', type=int, default=30, 
                        help='Days of historical data to fetch')
    parser.add_argument('--threshold', type=int, default=50, 
                        help='Minimum momentum score threshold')
    
    args = parser.parse_args()
    
    scanner = StockScanner(batch_size=args.batch_size, workers=args.workers)
    scanner.run(
        tickers_file=args.tickers,
        output_dir=args.output_dir,
        days=args.days,
        threshold=args.threshold
    )
