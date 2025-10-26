import pandas as pd
import os
from typing import List, Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from src.analysis.quote_fetcher import QuoteFetcher
from src.analysis.indicators import TechnicalIndicators
from src.analysis.momentum_scorer import MomentumScorer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScannerIntegration:
    """
    Batch process large stock lists (100 to 5,000+ stocks) efficiently.
    
    Strategy:
    - Divide symbols into batches of 50-100 stocks
    - Process each batch in parallel threads
    - Fetch data → Calculate indicators → Score momentum
    - Filter by score threshold (default 60+)
    - Save results with timestamp
    """
    
    def __init__(self, batch_size: int = 50, max_workers: int = 4):
        """
        Initialize scanner.
        
        Args:
            batch_size: Number of stocks per batch (50-100 recommended)
            max_workers: Number of parallel threads (4-8 recommended)
        """
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.fetcher = QuoteFetcher()
        self.results = []
        self.failed_symbols = []
    
    def load_symbols_from_csv(self, filepath: str, symbol_column: str = 'symbol') -> List[str]:
        """
        Load stock symbols from CSV file.
        
        Args:
            filepath: Path to CSV file
            symbol_column: Column name containing symbols
            
        Returns:
            List of symbols
        """
        try:
            df = pd.read_csv(filepath)
            symbols = df[symbol_column].unique().tolist()
            logger.info(f"Loaded {len(symbols)} symbols from {filepath}")
            return symbols
        except Exception as e:
            logger.error(f"Error loading symbols: {str(e)}")
            return []
    
    def create_batches(self, symbols: List[str]) -> List[List[str]]:
        """
        Divide symbols into batches.
        
        Args:
            symbols: List of all symbols
            
        Returns:
            List of batches
        """
        batches = []
        for i in range(0, len(symbols), self.batch_size):
            batch = symbols[i:i + self.batch_size]
            batches.append(batch)
        
        logger.info(f"Created {len(batches)} batches of {self.batch_size} stocks")
        return batches
    
    def process_batch(self, symbols: List[str], days: int = 30) -> Dict:
        """
        Process a single batch of stocks.
        
        Args:
            symbols: List of symbols in this batch
            days: Days of history to fetch
            
        Returns:
            Dictionary with results and failures
        """
        batch_results = []
        batch_failures = []
        
        try:
            # Fetch data for all symbols in batch
            logger.info(f"Fetching {len(symbols)} stocks...")
            data_dict = self.fetcher.fetch_batch(symbols, days=days)
            
            if not data_dict:
                logger.warning(f"No data fetched for batch")
                return {'results': [], 'failures': symbols}
            
            # Combine
            combined_df = self.fetcher.combine_batches(data_dict)
            
            # Add indicators
            logger.info(f"Calculating indicators for batch...")
            df_with_indicators = TechnicalIndicators.add_indicators_to_dataframe(combined_df)
            
            # Add momentum scores
            logger.info(f"Calculating momentum scores for batch...")
            df_with_scores = MomentumScorer.add_momentum_scores(df_with_indicators)
            
            # Get results
            for symbol in data_dict.keys():
                try:
                    score_info = MomentumScorer.get_latest_score(df_with_scores, symbol)
                    if score_info:
                        batch_results.append(score_info)
                except Exception as e:
                    logger.warning(f"Error getting score for {symbol}: {str(e)}")
                    batch_failures.append(symbol)
        
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            batch_failures.extend(symbols)
        
        return {'results': batch_results, 'failures': batch_failures}
    
    def scan_all(self, symbols: List[str], days: int = 30, score_threshold: float = 60) -> pd.DataFrame:
        """
        Scan all symbols using parallel batch processing.
        
        Args:
            symbols: List of all stock symbols
            days: Days of history to fetch
            score_threshold: Minimum momentum score to include (0-100)
            
        Returns:
            DataFrame with top scoring stocks
        """
        logger.info(f"Starting scan of {len(symbols)} stocks with {self.max_workers} workers")
        
        # Create batches
        batches = self.create_batches(symbols)
        
        # Process batches in parallel
        all_results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.process_batch, batch, days): i 
                for i, batch in enumerate(batches)
            }
            
            for future in as_completed(futures):
                batch_num = futures[future]
                try:
                    batch_result = future.result()
                    all_results.extend(batch_result['results'])
                    self.failed_symbols.extend(batch_result['failures'])
                    logger.info(f"Batch {batch_num + 1}/{len(batches)} complete: {len(batch_result['results'])} results")
                except Exception as e:
                    logger.error(f"Error in batch {batch_num}: {str(e)}")
        
        # Convert to DataFrame
        if all_results:
            df_results = pd.DataFrame(all_results)
            
            # Filter by score
            df_filtered = df_results[df_results['momentum_score'] >= score_threshold].copy()
            df_filtered = df_filtered.sort_values('momentum_score', ascending=False)
            
            logger.info(f"Scan complete: {len(df_filtered)} stocks above score {score_threshold}")
            logger.info(f"Failed to process: {len(self.failed_symbols)} stocks")
            
            return df_filtered
        else:
            logger.warning("No results generated")
            return pd.DataFrame()
    
    def save_results(self, df: pd.DataFrame, output_dir: str = 'data', prefix: str = 'scan_results') -> str:
        """
        Save scan results to CSV.
        
        Args:
            df: Results DataFrame
            output_dir: Output directory
            prefix: Filename prefix
            
        Returns:
            Filepath of saved file
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = os.path.join(output_dir, f'{prefix}_{timestamp}.csv')
        
        df.to_csv(filepath, index=False)
        logger.info(f"Results saved to {filepath}")
        
        return filepath
    
    def get_summary(self, df: pd.DataFrame) -> Dict:
        """
        Get summary statistics of scan.
        
        Args:
            df: Results DataFrame
            
        Returns:
            Summary dictionary
        """
        if df.empty:
            return {'total': 0}
        
        return {
            'total_scanned': len(df),
            'top_signal': df['signal'].mode() if not df['signal'].mode().empty else 'N/A',
            'avg_score': float(df['momentum_score'].mean()),
            'max_score': float(df['momentum_score'].max()),
            'min_score': float(df['momentum_score'].min()),
            'top_stock': df.iloc[0]['symbol'] if len(df) > 0 else 'N/A',
            'top_score': float(df.iloc[0]['momentum_score']) if len(df) > 0 else 0
        }


# ============================================================================
# USAGE EXAMPLE - Scan 932 stocks
# ============================================================================

if __name__ == "__main__":
    # Initialize scanner (4 parallel workers, 50 stocks per batch)
    scanner = ScannerIntegration(batch_size=50, max_workers=4)
    
    # Option 1: Load from CSV file
    # symbols = scanner.load_symbols_from_csv('data/sp500_symbols.csv')
    
    # Option 2: Use your own list (example: 932 stocks)
    symbols = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
        'META', 'NVDA', 'JPM', 'V', 'WMT'
        # ... add more symbols here (932 total)
    ]
    
    # Run scan
    print(f"\nScanning {len(symbols)} stocks...\n")
    results = scanner.scan_all(symbols, days=30, score_threshold=60)
    
    # Save results
    if not results.empty:
        filepath = scanner.save_results(results)
        
        # Print summary
        summary = scanner.get_summary(results)
        print(f"\n{'='*60}")
        print(f"SCAN SUMMARY")
        print(f"{'='*60}")
        print(f"Total stocks above threshold: {summary['total_scanned']}")
        print(f"Average momentum score: {summary['avg_score']:.2f}")
        print(f"Top stock: {summary['top_stock']} (Score: {summary['top_score']:.1f})")
        print(f"Most common signal: {summary['top_signal']}")
        print(f"\nResults saved to: {filepath}")
        
        # Show top 10
        print(f"\n{'='*60}")
        print(f"TOP 10 STOCKS")
        print(f"{'='*60}")
        print(results[['symbol', 'close', 'momentum_score', 'signal']].head(10).to_string(index=False))
    else:
        print("No results generated")
    
    # Report failures
    if scanner.failed_symbols:
        print(f"\nFailed to process {len(scanner.failed_symbols)} symbols")
