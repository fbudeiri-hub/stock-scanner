from datetime import datetime,timezone
from unittest.mock import patch
import socket
import pandas as pd
import numpy as np
import pytest
from src.analysis.quote_fetcher import QuoteFetcher
from src.analysis.indicators import TechnicalIndicators as TI
from src.analysis.momentum_scorer import MomentumScorer as MS

@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k: (_ for _ in ()).throw(AssertionError('offline test')))
@pytest.fixture
def bars():
    index=pd.bdate_range('2026-05-01',periods=100,tz='UTC')
    close=np.linspace(100,130,100)+np.sin(np.arange(100))
    return pd.DataFrame({'Open':close,'High':close+1,'Low':close-1,'Close':close,'Volume':1000},index=index)

def test_flat_response(bars):
    df=QuoteFetcher.normalize_frame(bars,'AAPL')
    assert df.symbol.unique().tolist()==['AAPL'] and 'timestamp' in df
@pytest.mark.parametrize('reverse',[False,True])
def test_multiindex_response(bars,reverse):
    bars.columns=pd.MultiIndex.from_tuples([(c,'AAPL') for c in bars.columns])
    if reverse: bars.columns=bars.columns.swaplevel()
    df=QuoteFetcher.normalize_frame(bars,'AAPL')
    assert df.close.iloc[-1]>0 and len(df)==100

def test_empty_batch(): assert QuoteFetcher().fetch_batch([])=={}
def test_keys_are_symbol_strings(bars):
    f=QuoteFetcher()
    with patch.object(f,'fetch_with_fallback',side_effect=lambda s,d:QuoteFetcher.normalize_frame(bars,s)):
        result=f.fetch_batch(['AAPL','MSFT','AAPL'])
    assert set(result)=={'AAPL','MSFT'}
@pytest.mark.parametrize('column,value',[('Close',np.nan),('Open',0),('High',1),('Low',10000),('Volume',-1)])
def test_bad_values_rejected(bars,column,value):
    bars.loc[bars.index[-1],column]=value
    with pytest.raises(ValueError): QuoteFetcher.normalize_frame(bars,'AAPL')

def test_bad_dates_rejected(bars):
    with pytest.raises(ValueError): QuoteFetcher.normalize_frame(pd.concat([bars,bars.tail(1)]),'AAPL')

def test_warmup_not_fake_neutral(bars):
    df=TI.add_indicators_to_dataframe(QuoteFetcher.normalize_frame(bars,'AAPL'))
    scores=MS.add_momentum_scores(df)
    assert scores.momentum_score.head(33).isna().all()
    assert scores.momentum_score.tail(50).notna().all()

def test_append_does_not_rewrite_past(bars):
    normalized=QuoteFetcher.normalize_frame(bars,'AAPL')
    prefix=MS.add_momentum_scores(TI.add_indicators_to_dataframe(normalized.iloc[:70]))
    complete=MS.add_momentum_scores(TI.add_indicators_to_dataframe(normalized))
    pd.testing.assert_series_equal(prefix.momentum_score,complete.momentum_score.iloc[:70],check_names=False)

def test_unsorted_same_as_sorted(bars):
    normalized=QuoteFetcher.normalize_frame(bars,'AAPL')
    expected=MS.add_momentum_scores(TI.add_indicators_to_dataframe(normalized))
    actual=MS.add_momentum_scores(TI.add_indicators_to_dataframe(normalized.sample(frac=1,random_state=7)))
    pd.testing.assert_frame_equal(actual,expected)

def test_wilder_rsi_edges():
    assert TI.calculate_rsi(pd.Series(range(1,31))).iloc[-1]==100
    assert TI.calculate_rsi(pd.Series(range(30,0,-1))).iloc[-1]==0
    assert TI.calculate_rsi(pd.Series([50]*30)).iloc[-1]==50

def test_macd_is_ema(bars):
    close=bars.Close
    macd,signal,hist=TI.calculate_macd(close)
    expected=close.ewm(span=12,adjust=False,min_periods=12).mean()-close.ewm(span=26,adjust=False,min_periods=26).mean()
    pd.testing.assert_series_equal(macd,expected)
    assert MS.get_signal(float('nan'))=='UNAVAILABLE'

def test_full_scan_to_watchlist_and_manifest(tmp_path,bars):
    from src.analysis.scanner_full import FullStockScanner
    from src.analysis.scanner_momentum_focus import MomentumFocusScanner
    import json
    pd.DataFrame({'symbol':['AAPL','MSFT']}).to_csv(tmp_path/'tickers.csv',index=False)
    def batch(self,symbols,**kwargs):
        return {s:QuoteFetcher.normalize_frame(bars,s) for s in symbols}
    with patch.object(QuoteFetcher,'fetch_batch',batch):
        result=FullStockScanner().run(tmp_path/'tickers.csv',tmp_path/'out',threshold=0)
        assert len(result)==2 and result.timestamp.nunique()==1
        manifest=json.loads((tmp_path/'out/scan_latest.json').read_text())
        assert manifest['valid_symbols']==2 and manifest['execution_authorized'] is False
        focused=MomentumFocusScanner().run(tmp_path/'out/watchlist_top100.txt',tmp_path/'out',threshold=0)
        assert len(focused)==2

def test_no_candidates_does_not_reuse_historical_peaks(tmp_path,bars):
    from src.analysis.scanner_full import FullStockScanner
    pd.DataFrame({'symbol':['AAPL']}).to_csv(tmp_path/'tickers.csv',index=False)
    with patch.object(QuoteFetcher,'fetch_batch',return_value={'AAPL':QuoteFetcher.normalize_frame(bars,'AAPL')}):
        result=FullStockScanner().run(tmp_path/'tickers.csv',tmp_path/'out',threshold=101)
    assert result.empty and (tmp_path/'out/watchlist_top100.txt').read_text()==''

def test_all_provider_failures_propagate(tmp_path):
    from src.analysis.scanner_full import FullStockScanner
    pd.DataFrame({'symbol':['AAPL']}).to_csv(tmp_path/'tickers.csv',index=False)
    with patch.object(QuoteFetcher,'fetch_batch',return_value={}):
        with pytest.raises(RuntimeError,match='failed'):
            FullStockScanner().run(tmp_path/'tickers.csv',tmp_path/'out')
    assert not (tmp_path/'out/scan_latest.json').exists()

def test_insufficient_lookback_fails_before_provider():
    from src.analysis.scanner_full import analyze_symbols
    with pytest.raises(ValueError,match='warm-up'): analyze_symbols(['AAPL'],days=5)
