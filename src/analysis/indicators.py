"""Chronological RSI, EMA MACD and Bollinger Bands with explicit warm-up NaNs."""
import numpy as np
import pandas as pd

class TechnicalIndicators:
    @staticmethod
    def calculate_rsi(prices,period=14):
        values=pd.to_numeric(prices,errors='raise').astype(float)
        if type(period) is not int or period<1 or not np.isfinite(values.to_numpy()).all():
            raise ValueError('Valid prices and period required')
        out=pd.Series(np.nan,index=values.index,dtype=float)
        if len(values)<=period: return out
        delta=values.diff().to_numpy(); gain=np.maximum(delta,0); loss=np.maximum(-delta,0)
        avg_gain=float(np.mean(gain[1:period+1])); avg_loss=float(np.mean(loss[1:period+1]))
        def rsi(g,l): return 50.0 if g==l==0 else (100.0 if l==0 else 100-100/(1+g/l))
        out.iloc[period]=rsi(avg_gain,avg_loss)
        for i in range(period+1,len(values)):
            avg_gain=(avg_gain*(period-1)+gain[i])/period
            avg_loss=(avg_loss*(period-1)+loss[i])/period
            out.iloc[i]=rsi(avg_gain,avg_loss)
        return out
    @staticmethod
    def calculate_macd(prices,fast=12,slow=26,signal=9):
        if not 0<fast<slow or signal<1: raise ValueError('Invalid MACD periods')
        line=prices.ewm(span=fast,adjust=False,min_periods=fast).mean()-prices.ewm(span=slow,adjust=False,min_periods=slow).mean()
        sig=line.ewm(span=signal,adjust=False,min_periods=signal).mean()
        return line,sig,line-sig
    @staticmethod
    def calculate_bollinger_bands(prices,period=20,std_dev=2):
        middle=prices.rolling(period,min_periods=period).mean()
        width=prices.rolling(period,min_periods=period).std(ddof=0)*std_dev
        return middle+width,middle,middle-width
    @staticmethod
    def add_indicators_to_dataframe(df,price_col='close'):
        data=df.copy()
        if 'timestamp' not in data:
            if 'date' not in data: raise ValueError('timestamp/date missing')
            data['timestamp']=data['date']
        data['timestamp']=pd.to_datetime(data['timestamp'],utc=True,errors='raise')
        if 'symbol' not in data: data['symbol']='SINGLE'
        if data.duplicated(['symbol','timestamp']).any(): raise ValueError('Duplicate bar')
        data=data.sort_values(['symbol','timestamp']).reset_index(drop=True)
        chunks=[]
        for _,g in data.groupby('symbol',sort=False):
            g=g.copy(); price=pd.to_numeric(g[price_col],errors='raise').astype(float)
            if not np.isfinite(price.to_numpy()).all() or (price<=0).any(): raise ValueError('Invalid close')
            g['rsi_14']=TechnicalIndicators.calculate_rsi(price)
            g['macd_12_26_9'],g['macd_signal_9'],g['macd_histogram']=TechnicalIndicators.calculate_macd(price)
            g['bollinger_upper_20'],g['bollinger_middle_20'],g['bollinger_lower_20']=TechnicalIndicators.calculate_bollinger_bands(price)
            chunks.append(g)
        return pd.concat(chunks,ignore_index=True) if chunks else data
    @staticmethod
    def get_latest_indicators(df,symbol):
        data=df[df.symbol==symbol].sort_values('timestamp')
        if data.empty: return {}
        row=data.iloc[-1]
        names={'rsi_14':'rsi_14','macd':'macd_12_26_9','macd_signal':'macd_signal_9','macd_histogram':'macd_histogram',
               'bb_upper':'bollinger_upper_20','bb_middle':'bollinger_middle_20','bb_lower':'bollinger_lower_20'}
        if not np.isfinite([row.get(v,np.nan) for v in names.values()]).all(): return {}
        return {'symbol':symbol,'date':str(row.timestamp),'close':float(row.close),**{k:float(row[v]) for k,v in names.items()}}
