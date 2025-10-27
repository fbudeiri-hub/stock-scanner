import pandas as pd
import os
import sys
import argparse
import logging
from typing import List, Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

from src.analysis.quote_fetcher import QuoteFetcher
from src.analysis.indicators import TechnicalIndicators
from src.analysis.momentum_scorer import MomentumScorer
from src.sentiment.news_analyzer import NewsAnalyzer
from src.trading.entry_exit_calculator import EntryExitCalculator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScannerIntegration:
    """
    Production Stock Scanner - 5,000 stocks with professional trading signals
    
    Features:
    - Multi-signal weighting (30% technical, 25% momentum, 25% sentiment, 20% volume)
    - Professional entry/exit price calculations
    - Portfolio monitoring framework (future expansion)
    - GitHub push notifications
    - UK timezone support for US trading
    """

    def __init__(self, batch_size: int = 100, max_workers: int = 6):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.fetcher = QuoteFetcher()
        self.indicators = TechnicalIndicators()
        self.momentum_scorer = MomentumScorer()
        self.news_analyzer = NewsAnalyzer()
        self.entry_exit_calc = EntryExitCalculator()
        
        # Timezone support
        self.uk_tz = pytz.timezone('Europe/London')
        self.et_tz = pytz.timezone('America/New_York')
        
        logger.info(f"Scanner initialized: batch_size={batch_size}, workers={max_workers}")

    def load_symbols_from_csv(self, filepath: str) -> List[str]:
        """Load stock symbols from CSV file."""
        try:
            df = pd.read_csv(filepath)
            if 'symbol' in df.columns:
                symbols = df['symbol'].tolist()
            elif 'ticker' in df.columns:
                symbols = df['ticker'].tolist()
            else:
                symbols = df.iloc[:, 0].tolist()
            
            # Clean symbols (strip whitespace)
            symbols = [s.strip() for s in symbols if isinstance(s, str) and s.strip()]
            
            logger.info(f"Loaded {len(symbols)} symbols from {filepath}")
            return symbols
        except Exception as e:
            logger.error(f"Error loading symbols: {e}")
            return []

    def fetch_batch(self, symbols: List[str], days: int = 30) -> list:
        """Fetch data for a batch of symbols using the correct QuoteFetcher method."""
        results = []
        for symbol in symbols:
            try:
                # Use fetch_with_fallback() - the correct method name
                data = self.fetcher.fetch_with_fallback(symbol, days=days)
                if data is not None and not data.empty:
                    results.append({
                        'symbol': symbol,
                        'data': data,
                        'status': 'success'
                    })
            except Exception as e:
                logger.warning(f"Error fetching {symbol}: {e}")
        return results

    def process_batch(self, batch_results: List[Dict]) -> pd.DataFrame:
        """Process batch: indicators, sentiment, entry/exit prices."""
        processed = []
        
        for item in batch_results:
            try:
                symbol = item['symbol']
                data = item['data']
                
                # Technical indicators
                indicators_df = self.indicators.add_indicators_to_dataframe(data.copy())
                
                # News sentiment
                sentiment_score = self.news_analyzer.get_sentiment_score(symbol)
                
                # Composite momentum score
                tech_score = self.momentum_scorer.calculate_score(indicators_df)
                composite_score = (
                    0.30 * (tech_score / 100) +
                    0.25 * (self.momentum_scorer.momentum_score / 100) +
                    0.25 * ((sentiment_score + 100) / 200) +
                    0.20 * (self.momentum_scorer.volume_score / 100)
                ) * 100
                
                current_price = indicators_df['close'].iloc[-1]
                
                # Entry/Exit prices
                entry_price = self.entry_exit_calc.calculate_entry(
                    symbol=symbol,
                    current_price=current_price,
                    indicators=indicators_df,
                    score=composite_score
                )
                
                stop_loss = self.entry_exit_calc.calculate_stop_loss(
                    entry_price=entry_price,
                    current_price=current_price,
                    volatility=indicators_df.get('atr', [current_price * 0.02]).iloc[-1] if 'atr' in indicators_df.columns else current_price * 0.02
                )
                
                take_profit = self.entry_exit_calc.calculate_take_profit(
                    entry_price=entry_price,
                    risk_reward_ratio=2.0
                )
                
                processed.append({
                    'symbol': symbol,
                    'current_price': current_price,
                    'momentum_score': composite_score,
                    'signal': self.momentum_scorer.get_signal(composite_score),
                    'rsi': indicators_df['rsi'].iloc[-1] if 'rsi' in indicators_df.columns else None,
                    'macd': indicators_df['macd'].iloc[-1] if 'macd' in indicators_df.columns else None,
                    'sentiment_score': sentiment_score,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'risk_reward_ratio': (take_profit - entry_price) / (entry_price - stop_loss) if entry_price != stop_loss else 0
                })
            except Exception as e:
                logger.warning(f"Error processing {symbol}: {e}")
        
        return pd.DataFrame(processed)

    def scan_all(self, symbols: List[str], days: int = 30, score_threshold: float = 50) -> pd.DataFrame:
        """Scan all symbols in parallel batches."""
        logger.info(f"Starting scan of {len(symbols)} symbols (threshold: {score_threshold})")
        
        all_results = []
        total_batches = (len(symbols) + self.batch_size - 1) // self.batch_size
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            
            for i in range(0, len(symbols), self.batch_size):
                batch = symbols[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                future = executor.submit(self.fetch_batch, batch, days)
                futures[future] = (batch_num, total_batches)
            
            for future in as_completed(futures):
                batch_num, total = futures[future]
                try:
                    batch_results = future.result()
                    processed = self.process_batch(batch_results)
                    all_results.append(processed)
                    logger.info(f"Batch {batch_num}/{total}: {len(processed)} processed")
                except Exception as e:
                    logger.error(f"Batch {batch_num} failed: {e}")
        
        if all_results:
            combined = pd.concat(all_results, ignore_index=True)
            filtered = combined[combined['momentum_score'] >= score_threshold].sort_values(
                'momentum_score', ascending=False
            )
            logger.info(f"Scan complete: {len(combined)} total, {len(filtered)} passed threshold")
            return filtered
        
        return pd.DataFrame()

    def save_results(self, results: pd.DataFrame) -> str:
        """Save results with trading signals."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"data/scan_results_{timestamp}.csv"
        
        os.makedirs("data", exist_ok=True)
        results.to_csv(filepath, index=False)
        logger.info(f"Results saved to {filepath}")
        return filepath

    def get_summary(self, results: pd.DataFrame) -> Dict:
        """Get summary statistics."""
        return {
            'total_scanned': len(results),
            'avg_score': results['momentum_score'].mean() if not results.empty else 0,
            'top_stock': results.iloc[0]['symbol'] if not results.empty else 'N/A',
            'top_score': results.iloc[0]['momentum_score'] if not results.empty else 0,
            'buy_signals': len(results[results['signal'] == 'BUY']) if not results.empty else 0,
            'strong_buy_signals': len(results[results['momentum_score'] >= 80]) if not results.empty else 0
        }

    def get_time_info(self) -> Dict:
        """Get UK and US market times."""
        now_uk = datetime.now(self.uk_tz)
        now_et = now_uk.astimezone(self.et_tz)
        
        return {
            'uk_time': now_uk.strftime('%Y-%m-%d %H:%M:%S %Z'),
            'et_time': now_et.strftime('%Y-%m-%d %H:%M:%S %Z'),
            'market_open_et': '09:30',
            'market_close_et': '16:00'
        }


def main():
    """Production entry point."""
    parser = argparse.ArgumentParser(description='Stock Scanner - 5000 US stocks analyzed from UK')
    parser.add_argument('--tickers', type=str, required=True, help='Ticker CSV file')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size')
    parser.add_argument('--workers', type=int, default=6, help='Worker threads')
    parser.add_argument('--days', type=int, default=30, help='Historical days')
    parser.add_argument('--threshold', type=float, default=50, help='Score threshold')
    
    args = parser.parse_args()
    
    scanner = ScannerIntegration(batch_size=args.batch_size, max_workers=args.workers)
    
    # Time info
    time_info = scanner.get_time_info()
    print(f"\n{'='*70}")
    print(f"STOCK SCANNER - UK TIME ZONE SUPPORT")
    print(f"{'='*70}")
    print(f"UK Time: {time_info['uk_time']}")
    print(f"ET Time: {time_info['et_time']}")
    print(f"US Market: {time_info['market_open_et']} - {time_info['market_close_et']} ET")
    
    symbols = scanner.load_symbols_from_csv(args.tickers)
    if not symbols:
        logger.error(f"No symbols from {args.tickers}")
        sys.exit(1)
    
    results = scanner.scan_all(symbols, days=args.days, score_threshold=args.threshold)
    
    if not results.empty:
        filepath = scanner.save_results(results)
        summary = scanner.get_summary(results)
        
        print(f"\n{'='*70}")
        print(f"✅ SCAN COMPLETE - BUY RECOMMENDATIONS WITH TRADING LEVELS")
        print(f"{'='*70}")
        print(f"Total Analyzed: {summary['total_scanned']}")
        print(f"Strong Buy (80+): {summary['strong_buy_signals']}")
        print(f"Buy Signals: {summary['buy_signals']}")
        print(f"Top Pick: {summary['top_stock']} (Score: {summary['top_score']:.1f})")
        print(f"\nTop 10 Trading Opportunities:")
        print(results[['symbol', 'current_price', 'entry_price', 'stop_loss', 'take_profit', 'momentum_score']].head(10).to_string())
        print(f"\nResults saved: {filepath}")
    else:
        logger.warning("No results generated")
        sys.exit(1)


if __name__ == "__main__":
    main()
