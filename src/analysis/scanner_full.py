import pandas as pd
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FullStockScanner:
    """TIER 1: Full scan of all 5000 stocks (2x daily)"""
    
    def __init__(self, batch_size=100, workers=6):
        self.batch_size = batch_size
        self.workers = workers

    def run(self, tickers_file='data/tickers_5000.csv', output_dir='data/output',
            days=30, threshold=50):
        """Full scan: all 4975 stocks"""
        
        try:
            logger.info("=" * 60)
            logger.info("TIER 1: FULL SCAN - ALL 4,975 STOCKS")
            logger.info("=" * 60)
            
            # Load tickers
            logger.info("Loading ticker list...")
            df_tickers = pd.read_csv(tickers_file)
            symbols = df_tickers['symbol'].tolist()
            logger.info(f"📊 Starting FULL scan of {len(symbols)} stocks")

            # Import modules
            from src.analysis.quote_fetcher import QuoteFetcher
            from src.analysis.indicators import TechnicalIndicators
            from src.analysis.momentum_scorer import MomentumScorer

            fetcher = QuoteFetcher()
            indicators = TechnicalIndicators()
            scorer = MomentumScorer()

            # Fetch data for ALL stocks
            logger.info(f"Fetching {days} days of data with {self.workers} workers...")
            batch_data = fetcher.fetch_batch(symbols, days=days, workers=self.workers)
            logger.info(f"✅ Retrieved {len(batch_data)}/{len(symbols)} stocks")

            if not batch_data:
                logger.error("No data retrieved!")
                return None

            # Calculate indicators
            logger.info("Calculating technical indicators...")
            processed_data = []
            
            for symbol, df in batch_data.items():
                try:
                    df_with_indicators = indicators.add_indicators_to_dataframe(df)
                    processed_data.append(df_with_indicators)
                except Exception as e:
                    logger.debug(f"Indicator calc failed for {symbol}: {e}")

            if not processed_data:
                logger.error("No data after indicators!")
                return None

            # Combine all data
            all_data = pd.concat(processed_data, ignore_index=True)
            logger.info(f"Combined data shape: {all_data.shape}")

            # Score momentum
            logger.info("Scoring momentum...")
            all_data = scorer.add_momentum_scores(all_data)

            # Filter & rank
            df_results = all_data[all_data['momentum_score'] >= threshold].copy()
            df_results = df_results.sort_values('momentum_score', ascending=False)
            df_results = df_results.drop_duplicates(subset=['symbol'], keep='first')

            if df_results.empty:
                logger.warning(f"No stocks above threshold {threshold}, using top 500")
                df_results = all_data.nlargest(500, 'momentum_score')

            # Save FULL results
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            full_output = f"{output_dir}/full_scan_{timestamp}.csv"
            df_results.to_csv(full_output, index=False)

            logger.info(f"✅ FULL SCAN COMPLETE")
            logger.info(f"Results: {full_output}")
            logger.info(f"Total recommendations: {len(df_results)}")
            logger.info(f"Top 5 stocks:")
            print(df_results[['symbol', 'close', 'momentum_score']].head(5).to_string())

            # Save TOP 100 to watchlist for TIER 2 scanning
            top_100 = df_results.head(100)['symbol'].tolist()
            watchlist_file = f"{output_dir}/watchlist_top100.txt"
            with open(watchlist_file, 'w') as f:
                for symbol in top_100:
                    f.write(f"{symbol}\n")
            
            logger.info(f"📋 Watchlist saved: {watchlist_file} ({len(top_100)} symbols)")

            return df_results

        except Exception as e:
            logger.error(f"Full scanner failed: {e}", exc_info=True)
            raise

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='TIER 1: Full Stock Scanner')
    parser.add_argument('--tickers', type=str, default='data/tickers_5000.csv')
    parser.add_argument('--output-dir', type=str, default='data/output')
    parser.add_argument('--days', type=int, default=30)
    parser.add_argument('--threshold', type=int, default=50)

    args = parser.parse_args()

    scanner = FullStockScanner()
    scanner.run(
        tickers_file=args.tickers,
        output_dir=args.output_dir,
        days=args.days,
        threshold=args.threshold
    )
