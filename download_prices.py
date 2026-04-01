"""
Download futures price history from TradingView for COT dashboard.
Uses tvDatafeed (same approach as download_fx_history.py).
Saves to data/prices/<TICKER>_daily.csv
"""
from tvDatafeed import TvDatafeed, Interval
import pandas as pd
import os

tv = TvDatafeed()
N_BARS = 1500  # ~5+ years of daily data
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'prices')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# (our_ticker, tv_symbol, tv_exchange)
CONTRACTS = [
    # Crypto
    ('BTC',  'BTCUSD',   'COINBASE'),
    # Equity Indices
    ('ES',   'ES1!',     'CME_MINI'),
    ('NQ',   'NQ1!',     'CME_MINI'),
    ('YM',   'YM1!',     'CBOT_MINI'),
    ('RTY',  'RTY1!',    'CME_MINI'),
    ('VIX',  'VIX1!',    'CBOE'),
    ('EMD',  'EMD1!',    'CME_MINI'),
    ('MES',  'MES1!',    'CME_MINI'),
    ('MNQ',  'MNQ1!',    'CME_MINI'),
    # FX
    ('AUD',  'AUDUSD',   'FX_IDC'),
    ('GBP',  'GBPUSD',   'FX_IDC'),
    ('CAD',  'USDCAD',   'FX_IDC'),
    ('EUR',  'EURUSD',   'FX_IDC'),
    ('JPY',  'USDJPY',   'FX_IDC'),
    ('CHF',  'USDCHF',   'FX_IDC'),
    ('NZD',  'NZDUSD',   'FX_IDC'),
    ('MXN',  'USDMXN',   'FX_IDC'),
    ('BRL',  'USDBRL',   'FX_IDC'),
    ('ZAR',  'USDZAR',   'FX_IDC'),
    ('DXY',  'DXY',      'TVC'),
    # Rates
    ('ZB',   'ZB1!',     'CBOT'),
    ('UB',   'UB1!',     'CBOT'),
    ('ZN',   'ZN1!',     'CBOT'),
    ('TN',   'TN1!',     'CBOT'),
    ('ZF',   'ZF1!',     'CBOT'),
    ('ZT',   'ZT1!',     'CBOT'),
    ('ZQ',   'ZQ1!',     'CBOT'),
    ('SR3',  'SR31!',    'CME'),
    # Metals
    ('GC',   'GC1!',     'COMEX'),
    ('SI',   'SI1!',     'COMEX'),
    ('HG',   'HG1!',     'COMEX'),
    ('PL',   'PL1!',     'NYMEX'),
    ('PA',   'PA1!',     'NYMEX'),
    ('ALMWP','ALI1!',    'COMEX'),
    # Energy
    ('CL',   'CL1!',     'NYMEX'),
    ('BZ',   'BB1!',     'ICEEUR'),
    ('HO',   'HO1!',     'NYMEX'),
    ('RB',   'RB1!',     'NYMEX'),
    ('NG',   'NG1!',     'NYMEX'),
    ('EH',   'EH1!',     'CBOT'),
    # Grains
    ('ZW',   'ZW1!',     'CBOT'),
    ('KE',   'KE1!',     'CBOT'),
    ('MWE',  'MWE1!',    'MGEX'),
    ('ZC',   'ZC1!',     'CBOT'),
    ('ZS',   'ZS1!',     'CBOT'),
    ('ZM',   'ZM1!',     'CBOT'),
    ('ZL',   'ZL1!',     'CBOT'),
    ('ZR',   'ZR1!',     'CBOT'),
    ('RS',   'RS1!',     'ICE'),
    # Softs
    ('KC',   'KC1!',     'ICEUS'),
    ('SB',   'SB1!',     'ICEUS'),
    ('CC',   'CC1!',     'ICEUS'),
    ('CT',   'CT1!',     'ICEUS'),
    ('OJ',   'OJ1!',     'ICEUS'),
    ('LB',   'LBR1!',    'CME'),
    # Livestock & Dairy
    ('LE',   'LE1!',     'CME'),
    ('GF',   'GF1!',     'CME'),
    ('HE',   'HE1!',     'CME'),
    ('DC',   'DC1!',     'CME'),
    ('NF',   'GNF1!',    'CME'),
    ('CB',   'CB1!',     'CME'),
    ('CSC',  'CSC1!',    'CME'),
]

ok, failed = [], []

for ticker, symbol, exchange in CONTRACTS:
    out_path = os.path.join(OUTPUT_DIR, f'{ticker}_daily.csv')
    print(f'  {ticker:<6} {exchange}:{symbol}...', end=' ', flush=True)
    try:
        df = tv.get_hist(symbol=symbol, exchange=exchange,
                         interval=Interval.in_daily, n_bars=N_BARS)
        if df is None or df.empty:
            print('NO DATA')
            failed.append((ticker, symbol, exchange))
            continue
        df.index = pd.to_datetime(df.index)
        df.to_csv(out_path)
        print(f'{len(df)} rows -> {out_path}')
        ok.append(ticker)
    except Exception as e:
        print(f'ERROR: {e}')
        failed.append((ticker, symbol, exchange))

print(f'\nDone: {len(ok)} ok, {len(failed)} failed')
if failed:
    print('Failed:')
    for t, s, e in failed:
        print(f'  {t}: {e}:{s}')
