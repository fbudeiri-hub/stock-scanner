"""Validated daily OHLCV download boundary. Missing data is never a successful scan."""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timedelta,timezone
import logging
import re
import pandas as pd
import numpy as np
logger=logging.getLogger(__name__)

class QuoteFetcher:
    def __init__(self):
        self.rate_limit_wait=0.01
    @staticmethod
    def normalize_frame(frame,symbol):
        if not isinstance(frame,pd.DataFrame) or frame.empty:
            return None
        df=frame.copy()
        if isinstance(df.columns,pd.MultiIndex):
            # Find the symbol level explicitly; never mix columns of other stocks.
            matches=[i for i in range(df.columns.nlevels) if symbol in df.columns.get_level_values(i)]
            if len(matches)!=1:
                raise ValueError('Ambiguous ticker level in quote response')
            df=df.xs(symbol,axis=1,level=matches[0])
        df.columns=[str(c).lower() for c in df.columns]
        required=['open','high','low','close','volume']
        if not set(required)<=set(df.columns) or not df.columns.is_unique:
            raise ValueError('Missing or duplicate OHLCV columns')
        if 'timestamp' in df.columns:
            times=pd.to_datetime(df['timestamp'],utc=True,errors='coerce')
        elif 'date' in df.columns:
            times=pd.to_datetime(df['date'],utc=True,errors='coerce')
        elif isinstance(df.index,pd.DatetimeIndex):
            times=pd.to_datetime(df.index,utc=True,errors='coerce')
        else:
            raise ValueError('Datetime index or timestamp column required')
        if times.isna().any() or times.duplicated().any():
            raise ValueError('Invalid/duplicate bar dates')
        data=df[required].apply(pd.to_numeric,errors='coerce').reset_index(drop=True)
        if not np.isfinite(data.to_numpy()).all() or (data[required[:-1]]<=0).any().any() or (data.volume<0).any():
            raise ValueError('Invalid prices/volume')
        if ((data.high<data[['open','low','close']].max(axis=1)) | (data.low>data[['open','high','close']].min(axis=1))).any():
            raise ValueError('Inconsistent OHLC bounds')
        data['timestamp']=list(times)
        data['date']=data['timestamp']
        data['symbol']=symbol
        return data.sort_values('timestamp').reset_index(drop=True)
    def fetch_yfinance(self,symbol,days=120):
        if not isinstance(symbol,str) or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}',symbol):
            raise ValueError('Invalid ticker')
        if type(days) is not int or days<2:
            raise ValueError('days must be an integer >=2')
        try:
            import yfinance as yf
            end=datetime.now(timezone.utc).date(); start=end-timedelta(days=days)
            frame=yf.download(symbol,start=str(start),end=str(end),progress=False,auto_adjust=True,threads=False,timeout=15)
            result=self.normalize_frame(frame,symbol)
            if result is None or len(result)<2:
                return None
            age=(pd.Timestamp.now(tz='UTC')-result.timestamp.iloc[-1]).total_seconds()
            if age<0 or age>96*3600:
                raise ValueError('Stale/future quote history')
            return result
        except Exception as exc:
            logger.warning('Quote unavailable for %s (%s)',symbol,type(exc).__name__)
            return None
    def fetch_with_fallback(self,symbol,days=120):
        return self.fetch_yfinance(symbol,days)
    def fetch_batch(self,symbols,days=120,workers=4):
        if not symbols:
            return {}
        if type(workers) is not int or not 1<=workers<=8:
            raise ValueError('workers must be 1..8')
        if any(not isinstance(s,str) or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}',s) for s in symbols):
            raise ValueError('Invalid ticker list')
        symbols=list(dict.fromkeys(symbols)); results={}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures={executor.submit(self.fetch_with_fallback,s,days):s for s in symbols}
            for future in as_completed(futures):
                s=futures[future]
                try:
                    df=future.result()
                    if df is not None and len(df)>=2 and set(df['symbol'])=={s}:
                        results[s]=df
                except Exception as exc:
                    logger.warning('Batch item failed for %s (%s)',s,type(exc).__name__)
        logger.info('Retrieved %s/%s symbols',len(results),len(symbols))
        return results
