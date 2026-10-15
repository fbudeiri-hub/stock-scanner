"""Preserved 30/30/20/20 research heuristic with causal, index-correct scoring."""
import math
import numpy as np
import pandas as pd

class MomentumScorer:
    @staticmethod
    def score_rsi(rsi):
        if not math.isfinite(rsi): return float('nan')
        if rsi<30: return min(100,80+(30-rsi)/3)
        if rsi<50: return 40+(rsi-30)/2
        if rsi<70: return 60+(rsi-50)/2
        return max(0,40-(rsi-70)/3)
    @staticmethod
    def score_macd(macd,signal,histogram):
        if not all(math.isfinite(v) for v in (macd,signal,histogram)): return float('nan')
        return min(100,max(0,50+histogram/2*50))
    @staticmethod
    def score_bollinger_bands(close,bb_upper,bb_middle,bb_lower):
        if not all(math.isfinite(v) for v in (close,bb_upper,bb_middle,bb_lower)): return float('nan')
        if bb_upper==bb_lower: return 50
        return 100*(1-min(1,max(0,(close-bb_lower)/(bb_upper-bb_lower))))
    @staticmethod
    def score_price_velocity(df_symbol,days=5):
        if len(df_symbol)<days+1: return float('nan')
        current,past=df_symbol.close.iloc[-1],df_symbol.close.iloc[-days-1]
        if not math.isfinite(current) or not math.isfinite(past) or past<=0: return float('nan')
        pct=(current/past-1)*100
        if pct>=5: return min(100,80+pct)
        if pct>=-5: return 50+pct
        return max(0,20+pct)
    @staticmethod
    def calculate_momentum_score(row,df_symbol=None):
        parts=[MomentumScorer.score_rsi(row.get('rsi_14',float('nan'))),
               MomentumScorer.score_macd(row.get('macd_12_26_9',float('nan')),row.get('macd_signal_9',float('nan')),row.get('macd_histogram',float('nan'))),
               MomentumScorer.score_bollinger_bands(row['close'],row.get('bollinger_upper_20',float('nan')),row.get('bollinger_middle_20',float('nan')),row.get('bollinger_lower_20',float('nan'))),
               MomentumScorer.score_price_velocity(df_symbol) if df_symbol is not None else float('nan')]
        if not all(math.isfinite(v) for v in parts): return float('nan')
        return sum(v*w for v,w in zip(parts,(0.3,0.3,0.2,0.2)))
    @staticmethod
    def add_momentum_scores(df):
        data=df.sort_values(['symbol','timestamp']).reset_index(drop=True).copy()
        data['momentum_score']=float('nan')
        for _,g in data.groupby('symbol',sort=False):
            for offset,(idx,row) in enumerate(g.iterrows()):
                # Only the prefix available at this bar, never future rows.
                data.at[idx,'momentum_score']=MomentumScorer.calculate_momentum_score(row,g.iloc[:offset+1])
        return data
    @staticmethod
    def get_signal(score):
        if not math.isfinite(score): return 'UNAVAILABLE'
        if score>=80: return 'STRONG BUY'
        if score>=60: return 'BUY'
        if score>=40: return 'NEUTRAL'
        if score>=20: return 'SELL'
        return 'STRONG SELL'
    @staticmethod
    def get_latest_score(df,symbol):
        data=df[df.symbol==symbol].sort_values('timestamp')
        if data.empty: return {}
        r=data.iloc[-1]; score=float(r['momentum_score'])
        if not math.isfinite(score): return {}
        return {'symbol':symbol,'date':str(r.timestamp),'close':float(r.close),'momentum_score':score,
                'signal':MomentumScorer.get_signal(score),'rsi_14':float(r['rsi_14']),
                'macd':float(r['macd_12_26_9']),'bb_upper':float(r['bollinger_upper_20'])}
