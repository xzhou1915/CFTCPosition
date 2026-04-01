"""
CFTC Financial Traders (TFF) Dashboard
- Financial Traders report: Dealer/Intermediary, Asset Manager/Institutional,
  Leveraged Funds (= spec equivalent), Other Reportables
- Markets: Currencies, Indices, Financials, Crypto
"""
import pandas as pd
import numpy as np
import urllib.request, ssl, zipfile, io, os, json

# ── Ticker & Asset Class mappings (shared with Legacy) ────────────────────────

TICKER_MAP = {
    # FX (old names pre-Feb 2022 rename)
    "BRITISH POUND STERLING - CHICAGO MERCANTILE EXCHANGE":           "GBP",
    "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE":               "NZD",
    "SOUTH AFRICAN RAND - CHICAGO MERCANTILE EXCHANGE":               "ZAR",
    "U.S. DOLLAR INDEX - ICE FUTURES U.S.":                           "DXY",
    "CHINESE RENMINBI-HK (CNH) - CHICAGO MERCANTILE EXCHANGE":        "CNH",
    "USD/CHINESE RENMINBI-OFFSHORE - CHICAGO MERCANTILE EXCHANGE":    "CNH",
    # Equity Index (old names)
    "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE":    "NQ",
    "E-MINI RUSSELL 2000 INDEX - CHICAGO MERCANTILE EXCHANGE":        "RTY",
    "DOW JONES INDUSTRIAL AVG- x $5 - CHICAGO BOARD OF TRADE":       "YM($5)",
    # Rates (old names)
    "10-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE":           "ZN",
    "5-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE":            "ZF",
    "2-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE":            "ZT",
    "U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE":                   "ZB",
    "ULTRA U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE":             "UB",
    "ULTRA US T BOND - CHICAGO BOARD OF TRADE":                       "UB",
    "ULTRA 10-YEAR U.S. T-NOTES - CHICAGO BOARD OF TRADE":            "TN",
    "30-DAY FEDERAL FUNDS - CHICAGO BOARD OF TRADE":                  "ZQ",
    "3-MONTH SOFR - CHICAGO MERCANTILE EXCHANGE":                     "SR3",
    "3-MONTH EURODOLLARS - CHICAGO MERCANTILE EXCHANGE":              "GE",
    "EURODOLLARS-3M - CHICAGO MERCANTILE EXCHANGE":                   "GE",
    # FX (current names)
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE":      "AUD",
    "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE":          "GBP",
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE":        "CAD",
    "EURO FX - CHICAGO MERCANTILE EXCHANGE":                "EUR",
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE":           "JPY",
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE":            "CHF",
    "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE":              "NZD",
    "MEXICAN PESO - CHICAGO MERCANTILE EXCHANGE":           "MXN",
    "SO AFRICAN RAND - CHICAGO MERCANTILE EXCHANGE":        "ZAR",
    "BRAZILIAN REAL - CHICAGO MERCANTILE EXCHANGE":         "BRL",
    "USD INDEX - ICE FUTURES U.S.":                         "DXY",
    # Equity Indices
    "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE":               "ES",
    "E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE":   "ES",
    "MICRO E-MINI S&P 500 INDEX - CHICAGO MERCANTILE EXCHANGE":   "MES",
    "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE":            "NQ",
    "MICRO E-MINI NASDAQ-100 INDEX - CHICAGO MERCANTILE EXCHANGE": "MNQ",
    "DJIA Consolidated - CHICAGO BOARD OF TRADE":           "YM",
    "DJIA x $5 - CHICAGO BOARD OF TRADE":                  "YM($5)",
    "RUSSELL E-MINI - CHICAGO MERCANTILE EXCHANGE":         "RTY",
    "E-MINI S&P 400 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE": "EMD",
    "VIX FUTURES - CBOE FUTURES EXCHANGE":                  "VIX",
    # Rates
    "UST BOND - CHICAGO BOARD OF TRADE":                    "ZB",
    "ULTRA UST BOND - CHICAGO BOARD OF TRADE":              "UB",
    "UST 10Y NOTE - CHICAGO BOARD OF TRADE":                "ZN",
    "ULTRA UST 10Y - CHICAGO BOARD OF TRADE":               "TN",
    "UST 5Y NOTE - CHICAGO BOARD OF TRADE":                 "ZF",
    "UST 2Y NOTE - CHICAGO BOARD OF TRADE":                 "ZT",
    "FED FUNDS - CHICAGO BOARD OF TRADE":                   "ZQ",
    "SOFR-3M - CHICAGO MERCANTILE EXCHANGE":                "SR3",
    # Crypto
    "BITCOIN - CHICAGO MERCANTILE EXCHANGE":                "BTC",
    "MICRO BITCOIN - CHICAGO MERCANTILE EXCHANGE":          "MBT",
    "ETHER CASH SETTLED - CHICAGO MERCANTILE EXCHANGE":     "ETH",
    "MICRO ETHER - CHICAGO MERCANTILE EXCHANGE":            "MET",
}

TICKER_CATEGORY = {
    'DXY': 'Currencies', 'GBP': 'Currencies', 'CAD': 'Currencies', 'JPY': 'Currencies',
    'CHF': 'Currencies', 'EUR': 'Currencies', 'AUD': 'Currencies', 'MXN': 'Currencies',
    'NZD': 'Currencies', 'ZAR': 'Currencies', 'BRL': 'Currencies',
    'ZB': 'Financials', 'UB': 'Financials', 'ZN': 'Financials', 'TN': 'Financials',
    'ZF': 'Financials', 'ZT': 'Financials', 'ZQ': 'Financials', 'SR3': 'Financials',
    'ES': 'Indices', 'NQ': 'Indices', 'YM': 'Indices', 'RTY': 'Indices',
    'EMD': 'Indices', 'VIX': 'Indices', 'MES': 'Indices', 'MNQ': 'Indices',
    'BTC': 'Crypto', 'MBT': 'Crypto', 'ETH': 'Crypto', 'MET': 'Crypto',
}

KEEP_TICKERS = {
    'DXY', 'GBP', 'CAD', 'JPY', 'CHF', 'EUR', 'AUD', 'MXN', 'NZD', 'ZAR', 'BRL',
    'ZB', 'UB', 'ZN', 'TN', 'ZF', 'ZT', 'ZQ', 'SR3',
    'ES', 'NQ', 'YM', 'RTY', 'EMD', 'VIX', 'MES', 'MNQ',
    'BTC', 'MBT', 'ETH',
}

def get_ticker(name):
    if name in TICKER_MAP:
        return TICKER_MAP[name]
    base = name.rsplit(' - ', 1)[0].strip()
    return base[:12].strip()

# ── Data loading ──────────────────────────────────────────────────────────────

def download_year(year, ctx):
    url = f"https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    print(f"  Downloading {year}...", end=' ', flush=True)
    try:
        with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
            data = r.read()
        z = zipfile.ZipFile(io.BytesIO(data))
        with z.open(z.namelist()[0]) as f:
            df = pd.read_csv(f, encoding='latin1', low_memory=False)
        print(f"{len(df):,} rows")
        return df
    except Exception as e:
        print(f"ERROR: {e}")
        return None

def load_all_data():
    ctx = ssl._create_unverified_context()
    os.makedirs('data', exist_ok=True)
    frames = []
    current_year = pd.Timestamp.now().year
    for year in range(2021, current_year + 1):
        cache = f'data/tff_{year}.csv'
        if os.path.exists(cache) and year < current_year:
            print(f"  Cache hit: {cache}")
            df = pd.read_csv(cache, low_memory=False)
        else:
            df = download_year(year, ctx)
            if df is not None:
                df.to_csv(cache, index=False)
        if df is not None:
            frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    print(f"Total: {len(combined):,} rows across {combined['Report_Date_as_YYYY-MM-DD'].nunique()} dates")
    return combined

# ── Compute net positions ─────────────────────────────────────────────────────

def to_num(s):
    return pd.to_numeric(s, errors='coerce').fillna(0)

def col(df, name):
    return df[name] if name in df.columns else pd.Series(0, index=df.index)

def compute_nets(df):
    df = df.copy()
    df['date']         = pd.to_datetime(df['Report_Date_as_YYYY-MM-DD'], errors='coerce')
    df['lev_net']      = to_num(col(df, 'Lev_Money_Positions_Long_All'))    - to_num(col(df, 'Lev_Money_Positions_Short_All'))
    df['dealer_net']   = to_num(col(df, 'Dealer_Positions_Long_All'))        - to_num(col(df, 'Dealer_Positions_Short_All'))
    df['asset_net']    = to_num(col(df, 'Asset_Mgr_Positions_Long_All'))     - to_num(col(df, 'Asset_Mgr_Positions_Short_All'))
    df['other_net']    = to_num(col(df, 'Other_Rept_Positions_Long_All'))    - to_num(col(df, 'Other_Rept_Positions_Short_All'))
    df['oi']           = to_num(col(df, 'Open_Interest_All'))
    df['oi_chg']       = to_num(col(df, 'Change_in_Open_Interest_All'))
    df['lev_chg']      = to_num(col(df, 'Change_in_Lev_Money_Long_All'))     - to_num(col(df, 'Change_in_Lev_Money_Short_All'))
    df['dealer_chg']   = to_num(col(df, 'Change_in_Dealer_Long_All'))        - to_num(col(df, 'Change_in_Dealer_Short_All'))
    df['asset_chg']    = to_num(col(df, 'Change_in_Asset_Mgr_Long_All'))     - to_num(col(df, 'Change_in_Asset_Mgr_Short_All'))
    df['other_chg']    = to_num(col(df, 'Change_in_Other_Rept_Long_All'))    - to_num(col(df, 'Change_in_Other_Rept_Short_All'))
    df['traders']      = to_num(col(df, 'Traders_Tot_All'))
    return df

# ── Percentile & sparkline ────────────────────────────────────────────────────

def pct_rank(series, value):
    s = series.dropna()
    if len(s) == 0: return np.nan
    return round(100 * (s <= value).sum() / len(s), 1)

def pct_color_style(pct):
    if np.isnan(pct): return ''
    if pct >= 80: return 'background:#1a7a3c;color:white;font-weight:bold'
    if pct >= 60: return 'background:#d4edda'
    if pct >= 40: return 'background:#f8f9fa'
    if pct >= 20: return 'background:#f8d7da'
    return 'background:#a93226;color:white;font-weight:bold'

def make_sparkline(values, width=90, height=26):
    vals = [v for v in values if v is not None and not np.isnan(v)]
    if len(vals) < 3:
        return ''
    mn, mx = min(vals), max(vals)
    rng = mx - mn if mx != mn else 1
    xs = [round(i / (len(vals)-1) * width, 1) for i in range(len(vals))]
    ys = [round(height - (v - mn) / rng * (height - 2) - 1, 1) for v in vals]
    points = ' '.join(f'{x},{y}' for x, y in zip(xs, ys))
    color = '#27ae60' if vals[-1] >= vals[0] else '#e74c3c'
    zero_line = ''
    if mn < 0 < mx:
        zy = round(height - (0 - mn) / rng * (height - 2) - 1, 1)
        zero_line = f'<line x1="0" y1="{zy}" x2="{width}" y2="{zy}" stroke="#bbb" stroke-width="0.5" stroke-dasharray="2,2"/>'
    return (f'<svg width="{width}" height="{height}" style="display:block">'
            f'{zero_line}'
            f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="1.5" stroke-linejoin="round"/>'
            f'</svg>')

# ── Build matrix ──────────────────────────────────────────────────────────────

def build_matrix(df):
    now     = df['date'].max()
    one_y   = now - pd.DateOffset(years=1)
    three_y = now - pd.DateOffset(years=3)
    five_y  = now - pd.DateOffset(years=5)
    one_y_s = now - pd.DateOffset(weeks=52)

    df['_ticker'] = df['Market_and_Exchange_Names'].apply(get_ticker)
    active_cutoff = now - pd.DateOffset(months=6)
    rows = []
    ts_data = {}

    for ticker, grp in df.groupby('_ticker'):
        grp = grp.sort_values('date').drop_duplicates('date', keep='last')
        latest = grp.iloc[-1]
        if latest['date'] < active_cutoff:
            continue
        cur_lev = latest['lev_net']
        market  = latest['Market_and_Exchange_Names']
        asset   = TICKER_CATEGORY.get(ticker, 'Other')

        hist_1y = grp[grp['date'] >= one_y]['lev_net']
        hist_3y = grp[grp['date'] >= three_y]['lev_net']
        hist_5y = grp[grp['date'] >= five_y]['lev_net']

        pct1y = pct_rank(hist_1y, cur_lev)
        pct3y = pct_rank(hist_3y, cur_lev)
        pct5y = pct_rank(hist_5y, cur_lev)

        spark_vals = grp[grp['date'] >= one_y_s]['lev_net'].tolist()

        ts_data[ticker] = {
            'd':  grp['date'].dt.strftime('%Y-%m-%d').tolist(),
            'lv': [round(v) for v in grp['lev_net'].tolist()],
            'dl': [round(v) for v in grp['dealer_net'].tolist()],
            'am': [round(v) for v in grp['asset_net'].tolist()],
            'or': [round(v) for v in grp['other_net'].tolist()],
            'p':  [],
        }

        rows.append({
            'Market':       market,
            'Ticker':       ticker,
            'Asset Class':  asset,
            'Date':         latest['date'].strftime('%Y-%m-%d'),
            'OI':           int(latest['oi']),
            'OI Chg':       int(latest['oi_chg']),
            'Lev Net':      int(cur_lev),
            'Lev Chg':      int(latest['lev_chg']),
            'Pct1Y':        pct1y,
            'Pct3Y':        pct3y,
            'Pct5Y':        pct5y,
            'Dealer Net':   int(latest['dealer_net']),
            'Dealer Chg':   int(latest['dealer_chg']),
            'Asset Net':    int(latest['asset_net']),
            'Asset Chg':    int(latest['asset_chg']),
            '# Traders':    int(latest['traders']),
            '_spark':       make_sparkline(spark_vals),
        })

    out = pd.DataFrame(rows).sort_values('Ticker').reset_index(drop=True)
    return out, ts_data

# ── Price loading ─────────────────────────────────────────────────────────────

def load_prices(ts_data, price_dir='data/prices'):
    loaded = 0
    for ticker, d in ts_data.items():
        path = os.path.join(price_dir, f'{ticker}_daily.csv')
        if not os.path.exists(path):
            continue
        try:
            px = pd.read_csv(path, index_col=0, parse_dates=True)
            px.index = pd.to_datetime(px.index, utc=True).tz_localize(None)
            px = px.sort_index()
            close_col = 'close' if 'close' in px.columns else px.columns[-1]
            px_series = px[close_col].dropna()
            cot_dates = pd.to_datetime(d['d'])
            prices = []
            for dt in cot_dates:
                subset = px_series[px_series.index <= dt]
                if len(subset) == 0 or (dt - subset.index[-1]).days > 7:
                    prices.append(None)
                else:
                    prices.append(round(float(subset.iloc[-1]), 4))
            d['p'] = prices
            loaded += 1
        except Exception:
            pass
    print(f"  Prices loaded for {loaded}/{len(ts_data)} tickers")
    return ts_data

# ── HTML helpers ──────────────────────────────────────────────────────────────

def net_color(val, col):
    try:
        v = float(val)
    except:
        return ''
    if col in ('Lev Net','Lev Chg','Dealer Net','Dealer Chg','Asset Net','Asset Chg'):
        if v > 0: return 'background:#d4edda'
        if v < 0: return 'background:#f8d7da'
    if col == 'OI Chg':
        if v > 0: return 'background:#e8f4f8'
        if v < 0: return 'background:#fef9e7'
    return ''

def fmt(val, col):
    if col in ('Pct1Y', 'Pct3Y', 'Pct5Y'):
        try:    return f'{float(val):.0f}%'
        except: return 'n/a'
    try:    return f'{int(float(val)):,}'
    except: return str(val)

# ── Build HTML ────────────────────────────────────────────────────────────────

def build_html(df, ts_data, out_path):
    df = df[df['Ticker'].isin(KEEP_TICKERS)].copy()
    ts_data = {k: v for k, v in ts_data.items() if df['Ticker'].eq(k).any()}
    report_date = df['Date'].max()
    n_markets   = len(df)

    display_cols = ['Ticker','Asset Class','Date','OI','OI Chg',
                    'Lev Net','Lev Chg','Pct1Y','Pct3Y','Pct5Y',
                    'Dealer Net','Dealer Chg','Asset Net','Asset Chg',
                    '# Traders','_spark']

    col_labels = {c: c for c in display_cols}
    col_labels['_spark'] = 'Lev Trend (1Y)'

    col_tips = {
        'Ticker':      'Short ticker — hover for full market name',
        'Asset Class': 'Asset class category',
        'Date':        'Most recent report date',
        'OI':          'Total open interest',
        'OI Chg':      'Weekly change in open interest',
        'Lev Net':     'Leveraged Funds net position (Long − Short) — hedge funds & CTAs',
        'Lev Chg':     'Weekly change in Lev Net',
        'Pct1Y':       'Percentile of current Lev Net vs past 1 year (100% = most bullish ever)',
        'Pct3Y':       'Percentile vs past 3 years',
        'Pct5Y':       'Percentile vs past 5 years',
        'Dealer Net':  'Dealer/Intermediary net position — sell-side banks',
        'Dealer Chg':  'Weekly change in Dealer Net',
        'Asset Net':   'Asset Manager/Institutional net position — pension funds, mutual funds',
        'Asset Chg':   'Weekly change in Asset Net',
        '# Traders':   'Total reportable traders',
        '_spark':      'Lev Net trend over last 52 weeks. Click row for full chart.',
    }

    rows_html = ''
    for _, row in df.iterrows():
        tkr_escaped = row['Ticker'].replace("'", "\\'")
        rows_html += f'<tr onclick="showChart(\'{tkr_escaped}\')" style="cursor:pointer">'
        for c in display_cols:
            if c == '_spark':
                rows_html += f'<td style="padding:2px 6px">{row[c]}</td>'
            elif c == 'Ticker':
                full = row['Market'].replace('"','&quot;')
                rows_html += f'<td title="{full}" style="text-align:left;white-space:nowrap;font-weight:bold">{row[c]}</td>'
            elif c == 'Asset Class':
                rows_html += f'<td style="text-align:left;white-space:nowrap;font-size:10px;color:#555">{row[c]}</td>'
            elif c in ('Pct1Y','Pct3Y','Pct5Y'):
                style = pct_color_style(row[c])
                rows_html += f'<td style="text-align:right;{style}">{fmt(row[c], c)}</td>'
            else:
                style = net_color(row[c], c)
                rows_html += f'<td style="text-align:right;{style}">{fmt(row[c], c)}</td>'
        rows_html += '</tr>\n'

    header_html = ''
    for i, c in enumerate(display_cols):
        tip   = col_tips.get(c, '')
        label = col_labels[c]
        if c == '_spark':
            header_html += f'<th title="{tip}" style="white-space:nowrap">{label}</th>'
        else:
            header_html += f'<th onclick="sortTable({i})" title="{tip}" style="cursor:pointer;white-space:nowrap">{label} &#8597;</th>'

    ts_json = json.dumps(ts_data, separators=(',', ':'))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>CFTC TFF Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  body {{ font-family: Arial, sans-serif; font-size: 12px; margin: 20px; background: #f5f5f5; }}
  h1 {{ font-size: 18px; margin-bottom: 4px; }}
  .subtitle {{ color: #666; margin-bottom: 10px; font-size: 11px; }}
  .controls {{ margin-bottom: 10px; display:flex; align-items:center; gap:10px; flex-wrap:wrap; }}
  input#search {{ padding:6px; font-size:12px; border:1px solid #ccc; border-radius:4px; width:260px; }}
  select#assetFilter {{ padding:6px; font-size:12px; border:1px solid #ccc; border-radius:4px; }}
  .legend {{ font-size:11px; color:#555; }}
  .legend span {{ display:inline-block; padding:2px 8px; border-radius:3px; margin-right:6px; }}
  table {{ border-collapse:collapse; width:100%; background:white; box-shadow:0 1px 3px rgba(0,0,0,0.1); }}
  th {{ background:#1a3a5c; color:white; padding:8px 10px; font-size:11px; position:sticky; top:0; cursor:pointer; white-space:nowrap; }}
  th:hover {{ background:#2c5282; }}
  td {{ padding:5px 10px; border-bottom:1px solid #eee; font-size:11px; }}
  tr:hover td {{ background:#f0f7ff !important; }}
  tr {{ cursor:pointer; }}
  #overlay {{ display:none; position:fixed; inset:0; background:rgba(0,0,0,0.55); z-index:200; justify-content:center; align-items:center; }}
  #overlay.show {{ display:flex; }}
  #modal {{ background:white; border-radius:8px; padding:20px; width:820px; max-width:95vw; position:relative; box-shadow:0 4px 20px rgba(0,0,0,0.3); }}
  #modal h2 {{ font-size:14px; margin-bottom:4px; }}
  #modal .msub {{ font-size:11px; color:#777; margin-bottom:12px; }}
  #modal .btns {{ margin-bottom:10px; }}
  #modal .btns button {{ padding:4px 12px; margin-right:6px; font-size:11px; cursor:pointer; border:1px solid #ccc; border-radius:4px; background:#f8f8f8; }}
  #modal .btns button.active {{ background:#1a3a5c; color:white; border-color:#1a3a5c; }}
  #closeBtn {{ position:absolute; top:12px; right:14px; font-size:18px; cursor:pointer; color:#777; border:none; background:none; }}
  #chartBox {{ position:relative; height:320px; }}
</style>
</head>
<body>

<h1>CFTC Financial Traders (TFF) — Positioning Matrix</h1>
<div class="subtitle">Latest report: <b>{report_date}</b> &nbsp;|&nbsp; {n_markets} markets &nbsp;|&nbsp; Leveraged Funds = hedge funds &amp; CTAs &nbsp;|&nbsp; Click any row for time series</div>
<div class="controls">
  <input type="text" id="search" onkeyup="filterTable()" placeholder="Search ticker or market name...">
  <select id="assetFilter" onchange="filterTable()">
    <option value="">All</option>
    <option>Currencies</option>
    <option>Indices</option>
    <option>Financials</option>
    <option>Crypto</option>
  </select>
  <div class="legend">
    Pct rank: <span style="background:#1a7a3c;color:white">80-100%</span>
    <span style="background:#d4edda">60-80%</span>
    <span style="background:#f8f9fa;color:#333;border:1px solid #ddd">40-60%</span>
    <span style="background:#f8d7da">20-40%</span>
    <span style="background:#a93226;color:white">0-20%</span>
  </div>
</div>

<div>
<table id="cotTable">
<thead><tr>{header_html}</tr></thead>
<tbody id="tableBody">{rows_html}</tbody>
</table>
</div>

<!-- Modal -->
<div id="overlay" onclick="closeModal(event)">
  <div id="modal">
    <button id="closeBtn" onclick="closeChart()">&#x2715;</button>
    <h2 id="modalTitle"></h2>
    <div class="msub" id="modalSub"></div>
    <div class="btns">
      <button onclick="setWindow(52)"  id="btn1y" class="active">1Y</button>
      <button onclick="setWindow(156)" id="btn3y">3Y</button>
      <button onclick="setWindow(260)" id="btn5y">5Y</button>
      <button onclick="setWindow(9999)"id="btnAll">All</button>
    </div>
    <div id="chartBox"><canvas id="myChart"></canvas></div>
  </div>
</div>

<script>
var TS = {ts_json};
var chart = null;
var currentMarket = null;
var currentWindow = 52;

function showChart(market) {{
  currentMarket = market;
  var d = TS[market];
  if (!d) return;
  document.getElementById('modalTitle').textContent = market;
  document.getElementById('overlay').classList.add('show');
  renderChart(d, currentWindow);
}}

function setWindow(w) {{
  currentWindow = w;
  ['btn1y','btn3y','btn5y','btnAll'].forEach(function(id) {{
    document.getElementById(id).classList.remove('active');
  }});
  var map = {{52:'btn1y', 156:'btn3y', 260:'btn5y', 9999:'btnAll'}};
  if (map[w]) document.getElementById(map[w]).classList.add('active');
  if (currentMarket) renderChart(TS[currentMarket], w);
}}

function renderChart(d, weeks) {{
  var n      = Math.min(weeks, d.d.length);
  var dates  = d.d.slice(-n);
  var lev    = d.lv.slice(-n);
  var dealer = d.dl.slice(-n);
  var asset  = d.am.slice(-n);
  var other  = d.or.slice(-n);
  var price  = (d.p && d.p.length) ? d.p.slice(-n) : [];
  var hasPrice = price.some(function(v) {{ return v !== null && v !== undefined; }});

  if (chart) chart.destroy();
  var ctx = document.getElementById('myChart').getContext('2d');

  var datasets = [
    {{ label: 'Lev Money',  data: lev,    borderColor:'#2980b9', backgroundColor:'rgba(41,128,185,0.08)',  borderWidth:2,   pointRadius:0, tension:0.3, yAxisID:'y' }},
    {{ label: 'Dealer',     data: dealer, borderColor:'#e67e22', backgroundColor:'rgba(230,126,34,0.08)',  borderWidth:2,   pointRadius:0, tension:0.3, yAxisID:'y' }},
    {{ label: 'Asset Mgr',  data: asset,  borderColor:'#27ae60', backgroundColor:'rgba(39,174,96,0.08)',   borderWidth:2,   pointRadius:0, tension:0.3, yAxisID:'y' }},
    {{ label: 'Other',      data: other,  borderColor:'#95a5a6', backgroundColor:'rgba(149,165,166,0.08)', borderWidth:1.5, pointRadius:0, tension:0.3, borderDash:[4,4], yAxisID:'y' }},
  ];
  if (hasPrice) {{
    datasets.push({{ label: 'Price', data: price, borderColor:'#8e44ad', backgroundColor:'transparent', borderWidth:2, pointRadius:0, tension:0.3, yAxisID:'yPrice' }});
  }}

  var scales = {{
    x:      {{ ticks:{{ maxTicksLimit:10, font:{{ size:10 }} }}, grid:{{ display:false }} }},
    y:      {{ position:'left',  ticks:{{ font:{{ size:10 }} }}, grid:{{ color:'#f0f0f0' }}, title:{{ display:true, text:'Net Position', font:{{ size:10 }} }} }},
  }};
  if (hasPrice) {{
    scales.yPrice = {{ position:'right', ticks:{{ font:{{ size:10 }} }}, grid:{{ display:false }}, title:{{ display:true, text:'Price', font:{{ size:10 }} }} }};
  }}

  chart = new Chart(ctx, {{
    type: 'line',
    data: {{ labels: dates, datasets: datasets }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      interaction: {{ mode:'index', intersect:false }},
      plugins: {{ legend:{{ position:'top', labels:{{ font:{{ size:11 }} }} }} }},
      scales: scales,
    }}
  }});
}}

function closeChart() {{
  document.getElementById('overlay').classList.remove('show');
  if (chart) {{ chart.destroy(); chart = null; }}
}}
function closeModal(e) {{
  if (e.target === document.getElementById('overlay')) closeChart();
}}
document.addEventListener('keydown', function(e) {{ if (e.key==='Escape') closeChart(); }});

var sortDir = {{}};
function sortTable(col) {{
  var tbody = document.getElementById('tableBody');
  var rows  = Array.from(tbody.querySelectorAll('tr'));
  var asc   = !sortDir[col]; sortDir[col] = asc;
  rows.sort(function(a, b) {{
    var av = a.cells[col].innerText.replace(/[,%]/g,'').trim();
    var bv = b.cells[col].innerText.replace(/[,%]/g,'').trim();
    var an = Number(av), bn = Number(bv);
    if (av !== '' && bv !== '' && !isNaN(an) && !isNaN(bn)) return asc ? an-bn : bn-an;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  }});
  rows.forEach(function(r) {{ tbody.appendChild(r); }});
}}

function filterTable() {{
  var q  = document.getElementById('search').value.toLowerCase();
  var ac = document.getElementById('assetFilter').value.toLowerCase();
  document.getElementById('tableBody').querySelectorAll('tr').forEach(function(r) {{
    var ticker = r.cells[0].innerText.toLowerCase();
    var full   = (r.cells[0].getAttribute('title')||'').toLowerCase();
    var aclass = r.cells[1].innerText.toLowerCase();
    var mQ  = !q  || ticker.includes(q) || full.includes(q);
    var mAC = !ac || aclass === ac;
    r.style.display = (mQ && mAC) ? '' : 'none';
  }});
}}
</script>
</body>
</html>"""

    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Dashboard saved: {out_path}  ({os.path.getsize(out_path)//1024:,} KB)")

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print("Loading TFF data...")
    raw = load_all_data()
    print("Computing nets...")
    raw = compute_nets(raw)
    print("Building matrix...")
    matrix, ts_data = build_matrix(raw)
    print("Loading prices...")
    ts_data = load_prices(ts_data)
    print("Generating HTML...")
    build_html(matrix, ts_data, 'dashboard_tff.html')
