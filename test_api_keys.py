# -*- coding: utf-8 -*-
import requests
from datetime import datetime, timedelta
import sys

# Fix Windows encoding
if sys.platform == 'win32':
    import os
    os.chdir(os.getcwd())

FINNHUB_KEY = "d3u8rh1r01qvr0dm29ngd3u8rh1r01qvr0dm29o0"
MARKETSTACK_KEY = "5a4df39cc2c7510fd0406d0083d67d40"

print("=" * 60)
print("TESTING API KEYS")
print("=" * 60)

# Test 1: Finnhub
print("\nTESTING FINNHUB...")
try:
    resp = requests.get(
        "https://finnhub.io/api/v1/stock/candle",
        params={
            'symbol': 'AAPL',
            'resolution': 'D',
            'from': int((datetime.now() - timedelta(days=5)).timestamp()),
            'to': int(datetime.now().timestamp()),
            'token': FINNHUB_KEY
        },
        timeout=5
    )
    print(f"   Status: {resp.status_code}")
    data = resp.json()
    print(f"   Response keys: {list(data.keys())}")
    if 's' in data:
        print(f"   Result: {data['s']}")
    if 'c' in data:
        print(f"   Got {len(data.get('c', []))} candles")
except Exception as e:
    print(f"   FAILED: {str(e)}")

# Test 2: Marketstack
print("\nTESTING MARKETSTACK...")
try:
    resp = requests.get(
        "http://api.marketstack.com/v1/eod",
        params={
            'symbols': 'AAPL',
            'date_from': (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),
            'date_to': datetime.now().strftime('%Y-%m-%d'),
            'access_key': MARKETSTACK_KEY,
            'limit': 10
        },
        timeout=5
    )
    print(f"   Status: {resp.status_code}")
    data = resp.json()
    print(f"   Response keys: {list(data.keys())}")
    if 'data' in data:
        print(f"   Got {len(data.get('data', []))} records")
except Exception as e:
    print(f"   FAILED: {str(e)}")

print("\n" + "=" * 60)
