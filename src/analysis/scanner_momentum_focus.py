"""Research refresh of existing watchlist. Daily bars, not real-time intraday signals."""
from pathlib import Path
from datetime import datetime,timezone
from .scanner_full import analyze_symbols,atomic_text

class MomentumFocusScanner:
    def __init__(self,workers=3): self.workers=workers
    def run(self,watchlist_file='data/output/watchlist_top100.txt',output_dir='data/output',days=120,threshold=40):
        path=Path(watchlist_file)
        if not path.exists(): raise FileNotFoundError('Run the full scan first')
        symbols=list(dict.fromkeys(s.strip() for s in path.read_text().splitlines() if s.strip()))
        if not symbols:
            # Empty watchlist after a valid full scan is not a provider failure.
            import pandas as pd
            results=pd.DataFrame(columns=['symbol','timestamp','close','momentum_score'])
        else:
            latest=analyze_symbols(symbols,days,self.workers)
            results=latest[latest.momentum_score>=threshold].copy()
        atomic_text(Path(output_dir)/f'focus_scan_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")}.csv',results.to_csv(index=False))
        return results

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(); p.add_argument('--watchlist',default='data/output/watchlist_top100.txt')
    p.add_argument('--output-dir',default='data/output'); p.add_argument('--days',type=int,default=120)
    p.add_argument('--threshold',type=float,default=40); a=p.parse_args()
    MomentumFocusScanner().run(a.watchlist,a.output_dir,a.days,a.threshold)
