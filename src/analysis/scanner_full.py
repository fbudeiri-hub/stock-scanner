"""One observation per symbol at its LATEST bar, with explicit failure propagation."""
from __future__ import annotations
from datetime import datetime,timezone
from pathlib import Path
import json
import os
import tempfile
import pandas as pd
from .quote_fetcher import QuoteFetcher
from .indicators import TechnicalIndicators
from .momentum_scorer import MomentumScorer

def analyze_symbols(symbols,days=120,workers=4):
    if type(days) is not int or days<90:
        raise ValueError('At least 90 calendar days required for indicator warm-up')
    frames=QuoteFetcher().fetch_batch(symbols,days=days,workers=workers)
    if not frames:
        raise RuntimeError('No market data fetched; scan failed')
    processed=[]
    for symbol,frame in frames.items():
        indicators=TechnicalIndicators.add_indicators_to_dataframe(frame)
        scores=MomentumScorer.add_momentum_scores(indicators)
        latest=scores.sort_values('timestamp').tail(1)
        if latest.momentum_score.notna().all():
            processed.append(latest)
    if not processed:
        raise RuntimeError('No symbols completed indicator warm-up')
    return pd.concat(processed,ignore_index=True).sort_values(['momentum_score','symbol'],ascending=[False,True])

def atomic_text(path,text):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(dir=path.parent,prefix='.scan-')
    try:
        with os.fdopen(fd,'w',encoding='utf-8') as f:
            f.write(text); f.flush(); os.fsync(f.fileno())
        os.replace(tmp,path)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)

class FullStockScanner:
    def __init__(self,batch_size=100,workers=4): self.batch_size,self.workers=batch_size,workers
    def run(self,tickers_file='data/tickers_5000.csv',output_dir='data/output',days=120,threshold=50):
        symbols=list(dict.fromkeys(pd.read_csv(tickers_file)['symbol'].dropna().astype(str).str.strip().str.upper()))
        latest=analyze_symbols(symbols,days,self.workers)
        results=latest[latest.momentum_score>=threshold].copy()
        # No arbitrary historical high-score fallback when there are no candidates.
        now=datetime.now(timezone.utc)
        target=Path(output_dir)
        atomic_text(target/f'full_scan_{now.strftime("%Y%m%d_%H%M%S_%f")}.csv',results.to_csv(index=False))
        atomic_text(target/'watchlist_top100.txt',''.join(f'{s}\n' for s in results.head(100).symbol))
        bundle={'generated_at':now.isoformat(),'source':'yfinance_daily_adjusted','requested_symbols':len(symbols),
                'valid_symbols':len(latest),'candidate_count':len(results),'execution_authorized':False,
                'records':[{'symbol':r.symbol,'as_of':r.timestamp.isoformat(),'close':float(r.close),
                            'momentum_score':float(r.momentum_score),'signal':MomentumScorer.get_signal(r.momentum_score)}
                           for r in results.itertuples()]}
        atomic_text(target/'scan_latest.json',json.dumps(bundle,indent=2,allow_nan=False))
        return results

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(); p.add_argument('--tickers',default='data/tickers_5000.csv')
    p.add_argument('--output-dir',default='data/output'); p.add_argument('--days',type=int,default=120)
    p.add_argument('--threshold',type=float,default=50); a=p.parse_args()
    FullStockScanner().run(a.tickers,a.output_dir,a.days,a.threshold)
