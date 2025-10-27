import pandas as pd
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MomentumFocusScanner:
    """TIER 2: Focus on top momentum stocks (hourly intraday)"""
    
    def __init__(self, workers=3):
        self.workers = workers

    def run(self, watchlist_file='data/output/watchlist_top100.txt', 
            output_dir='data/output', days=5, threshold=40):
        """Focus scan: only top momentum stocks"""
        
        try:
            logger.info("=" * 60)
            logger.info("TIER 2: MOMENTUM FOCUS SCAN - TOP 100 STOCKS")
            logger.info("=" * 60)
            
            # Load watchlist
            if not os.path.exists(watchlist_file):
                logger.error(f"Watchlist not found: {watchlist_file}")
                logger.info("Run TIER 1 scan first!")
                return None
            
            with open(watchlist_file, 'r') as f:
                symbols = [line.strip() for line in f if line.strip()]
            
            logger.info(f"📈 Starting FOCUS scan of {len(symbols)} momentum stocks")

            # Import modules
            from src.analysis.quote_fetcher import QuoteFetcher
            from src.analysis.indicators import TechnicalIndicators
            from src.analysis.momentum_scorer import MomentumScorer

            fetcher = QuoteFetcher()
            indicators = TechnicalIndicators()
            scorer = MomentumScorer()

            # Fetch data for ONLY top stocks (minimal API usage)
            logger.info(f"Fetching {days} days of data for {len(symbols)} symbols...")
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

            # Score momentum
            logger.info("Scoring momentum...")
            all_data = scorer.add_momentum_scores(all_data)

            # Filter & rank (only highest momentum)
            df_results = all_data[all_data['momentum_score'] >= threshold].copy()
            df_results = df_results.sort_values('momentum_score', ascending=False)
            df_results = df_results.drop_duplicates(subset=['symbol'], keep='first')

            if df_results.empty:
                logger.warning("No stocks above threshold, showing all")
                df_results = all_data.sort_values('momentum_score', ascending=False)

            # Save FOCUS results
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            focus_output = f"{output_dir}/focus_scan_{timestamp}.csv"
            df_results.to_csv(focus_output, index=False)

            logger.info(f"✅ FOCUS SCAN COMPLETE")
            logger.info(f"Results: {focus_output}")
            logger.info(f"High-momentum stocks: {len(df_results)}")
            logger.info(f"Top 10 movers:")
            print(df_results[['symbol', 'close', 'momentum_score']].head(10).to_string())

            return df_results

        except Exception as e:
            logger.error(f"Focus scanner failed: {e}", exc_info=True)
            raise

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='TIER 2: Momentum Focus Scanner')
    parser.add_argument('--watchlist', type=str, default='data/output/watchlist_top100.txt')
    parser.add_argument('--output-dir', type=str, default='data/output')
    parser.add_argument('--days', type=int, default=5)
    parser.add_argument('--threshold', type=int, default=40)

    args = parser.parse_args()

    scanner = MomentumFocusScanner()
    scanner.run(
        watchlist_file=args.watchlist,
        output_dir=args.output_dir,
        days=args.days,
        threshold=args.threshold
    )
