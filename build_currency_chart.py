"""
CFTC Legacy COT — FX Positioning Chart
- Non-Commercial net position (speculators / hedge funds)
- Currencies only, 1-year percentile rank
- Single bar chart, sorted most bullish → most bearish
"""
import pandas as pd
import numpy as np
import os, json

TICKER_MAP = {
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "AUD",
    "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE":     "GBP",
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE":   "CAD",
    "EURO FX - CHICAGO MERCANTILE EXCHANGE":           "EUR",
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE":      "JPY",
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE":       "CHF",
    "NZ DOLLAR - CHICAGO MERCANTILE EXCHANGE":         "NZD",
    "MEXICAN PESO - CHICAGO MERCANTILE EXCHANGE":      "MXN",
    "SO AFRICAN RAND - CHICAGO MERCANTILE EXCHANGE":   "ZAR",
    "BRAZILIAN REAL - CHICAGO MERCANTILE EXCHANGE":    "BRL",
    # old names
    "BRITISH POUND STERLING - CHICAGO MERCANTILE EXCHANGE": "GBP",
    "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE":     "NZD",
    "SOUTH AFRICAN RAND - CHICAGO MERCANTILE EXCHANGE":     "ZAR",
}

def to_num(s):
    return pd.to_numeric(s, errors='coerce').fillna(0)

def pct_rank(series, value):
    s = series.dropna()
    if len(s) == 0:
        return np.nan
    return round(100 * (s <= value).sum() / len(s), 1)

# ── Load ──────────────────────────────────────────────────────────────────────

def load_data():
    frames = []
    for year in range(2021, pd.Timestamp.now().year + 1):
        path = f'data/legacy_cot_{year}.csv'
        if os.path.exists(path):
            frames.append(pd.read_csv(path, low_memory=False))
    if not frames:
        raise FileNotFoundError("No legacy COT cache files found.")
    return pd.concat(frames, ignore_index=True)

def process(raw):
    df = raw.copy()
    df['date']    = pd.to_datetime(df['As of Date in Form YYYY-MM-DD'], errors='coerce')
    df['nc_net']  = to_num(df['Noncommercial Positions-Long (All)']) - to_num(df['Noncommercial Positions-Short (All)'])
    df['nc_chg']  = to_num(df['Change in Noncommercial-Long (All)']) - to_num(df['Change in Noncommercial-Short (All)'])
    df['ticker']  = df['Market and Exchange Names'].map(TICKER_MAP)
    return df[df['ticker'].notna()].copy()

def build_rows(df):
    now    = df['date'].max()
    one_y  = now - pd.DateOffset(years=1)
    cutoff = now - pd.DateOffset(months=6)
    report_date = now.strftime('%B %d, %Y')

    rows = []
    for ticker, grp in df.groupby('ticker'):
        grp    = grp.sort_values('date').drop_duplicates('date', keep='last')
        latest = grp.iloc[-1]
        if latest['date'] < cutoff:
            continue
        hist_1y = grp[grp['date'] >= one_y]['nc_net']
        cur     = latest['nc_net']
        pct     = pct_rank(hist_1y, cur)
        rows.append({
            'ticker':  ticker,
            'pct':     pct,
            'net':     int(cur),
            'chg':     int(latest['nc_chg']),
            'date':    latest['date'].strftime('%Y-%m-%d'),
        })

    rows.sort(key=lambda r: r['pct'] if not np.isnan(r['pct']) else -999, reverse=True)
    return rows, report_date

# ── HTML ──────────────────────────────────────────────────────────────────────

def pct_color(p):
    if np.isnan(p):  return 'rgba(180,180,180,1)'
    if p >= 70:      return 'rgba(26,122,60,1)'
    if p >= 55:      return 'rgba(82,171,110,1)'
    if p >= 45:      return 'rgba(160,160,160,1)'
    if p >= 30:      return 'rgba(220,100,100,1)'
    return           'rgba(169,50,38,1)'

def build_html(rows, report_date, out_path):
    tickers = [r['ticker'] for r in rows]
    pcts    = [r['pct']    for r in rows]
    chgs    = [r['chg']    for r in rows]
    colors  = [pct_color(p) for p in pcts]
    detail_json = json.dumps(rows, separators=(',', ':'), default=str)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>FX Positioning — CFTC COT</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #f0f2f5; margin: 0; padding: 24px; }}
  .card {{ background: white; border-radius: 10px; box-shadow: 0 2px 12px rgba(0,0,0,0.10); padding: 28px 32px; max-width: 960px; margin: 0 auto; }}
  h1 {{ font-size: 20px; font-weight: 700; margin: 0 0 4px; color: #1a1a2e; }}
  .sub {{ font-size: 12px; color: #666; margin-bottom: 18px; }}
  .legend {{ display: flex; align-items: center; gap: 0; margin-bottom: 16px; font-size: 11px; flex-wrap: wrap; row-gap: 6px; }}
  .seg {{ width: 52px; height: 15px; display:flex; align-items:center; justify-content:center; color:white; font-weight:600; font-size:9px; }}
  .seg.mid {{ color: #333; }}
  .seg-label {{ margin-left: 8px; color: #888; }}
  .legend-diamond {{ display:flex; align-items:center; gap:6px; margin-left:20px; color:#555; font-size:11px; }}
  .diamond-icon {{ width:10px; height:10px; background:#555; transform:rotate(45deg); display:inline-block; flex-shrink:0; }}
  .chart-wrap {{ position: relative; height: 420px; }}
  .note {{ font-size: 10px; color: #aaa; margin-top: 12px; text-align: right; }}
  #tooltip {{
    display: none; position: fixed; background: white; border: 1px solid #ddd;
    border-radius: 8px; padding: 12px 16px; box-shadow: 0 4px 16px rgba(0,0,0,0.15);
    font-size: 12px; min-width: 200px; z-index: 999; pointer-events: none;
  }}
  #tooltip h3 {{ margin: 0 0 8px; font-size: 15px; color: #1a1a2e; }}
  #tooltip table {{ width: 100%; border-collapse: collapse; }}
  #tooltip td {{ padding: 3px 0; color: #333; }}
  #tooltip td:last-child {{ text-align: right; font-weight: 700; }}
</style>
</head>
<body>
<div class="card">
  <h1>FX Speculative Positioning &mdash; CFTC Legacy COT</h1>
  <div class="sub">Latest data: <b>{report_date}</b> &nbsp;|&nbsp; Non-Commercial (speculator) net position &nbsp;|&nbsp; 1-year percentile rank &nbsp;|&nbsp; Sorted most bullish → most bearish</div>
  <div class="legend">
    <div class="seg" style="background:#1a7a3c">≥ 70%</div>
    <div class="seg" style="background:#52ab6e">55–70</div>
    <div class="seg mid" style="background:#a0a0a0">45–55</div>
    <div class="seg" style="background:#dc6464">30–45</div>
    <div class="seg" style="background:#a93226">≤ 30%</div>
    <span class="seg-label">1-year percentile rank</span>
    <div class="legend-diamond"><div class="diamond-icon"></div><span>WoW net change (contracts, right axis)</span></div>
  </div>
  <div class="chart-wrap"><canvas id="mainChart"></canvas></div>
  <div class="note">Source: CFTC Commitments of Traders (Legacy). Non-Commercial Net = Long − Short (excl. spreading).</div>
</div>
<div id="tooltip">
  <h3 id="tt-ticker"></h3>
  <table>
    <tr><td>Report date</td><td id="tt-date"></td></tr>
    <tr><td>Net position</td><td id="tt-net"></td></tr>
    <tr><td>Week change</td><td id="tt-chg"></td></tr>
    <tr><td>1Y percentile</td><td id="tt-pct"></td></tr>
  </table>
</div>
<script>
var DETAIL  = {detail_json};
var tickers = {json.dumps(tickers)};
var pcts    = {json.dumps(pcts)};
var colors  = {json.dumps(colors)};

var chgs    = {json.dumps(chgs)};
var chgColors = chgs.map(function(v) {{ return v >= 0 ? 'rgba(26,122,60,0.85)' : 'rgba(169,50,38,0.85)'; }});

var ctx = document.getElementById('mainChart').getContext('2d');
var chart = new Chart(ctx, {{
  data: {{
    labels: tickers,
    datasets: [
      {{
        type: 'bar',
        label: '1Y Percentile',
        data: pcts,
        backgroundColor: colors,
        borderWidth: 0,
        borderRadius: 5,
        barPercentage: 0.6,
        categoryPercentage: 0.8,
        yAxisID: 'y',
        order: 2,
      }},
      {{
        type: 'scatter',
        label: 'WoW Change',
        data: chgs.map(function(v, i) {{ return {{ x: tickers[i], y: v }}; }}),
        backgroundColor: chgColors,
        borderColor: 'white',
        borderWidth: 2,
        pointStyle: 'rectRot',
        pointRadius: 9,
        pointHoverRadius: 11,
        yAxisID: 'yChg',
        order: 1,
      }},
    ]
  }},
  options: {{
    responsive: true,
    maintainAspectRatio: false,
    plugins: {{ legend: {{ display: false }}, tooltip: {{ enabled: false }} }},
    scales: {{
      x: {{
        type: 'category',
        grid: {{ display: false }},
        ticks: {{ font: {{ size: 15, weight: '700' }}, color: '#1a1a2e' }},
      }},
      y: {{
        position: 'left',
        min: 0, max: 108,
        grid: {{ color: '#f0f0f0' }},
        ticks: {{ font: {{ size: 11 }}, callback: function(v) {{ return v + '%'; }}, stepSize: 10 }},
        title: {{ display: true, text: '1-Year Percentile Rank', font: {{ size: 12 }}, color: '#666' }},
      }},
      yChg: {{
        position: 'right',
        grid: {{ display: false }},
        ticks: {{ font: {{ size: 11 }}, callback: function(v) {{ return v.toLocaleString(); }} }},
        title: {{ display: true, text: 'WoW Net Change (contracts)', font: {{ size: 12 }}, color: '#555' }},
      }},
    }},
  }},
  plugins: [{{
    id: 'decorations',
    afterDraw: function(ch) {{
      var yA = ch.scales['y'], xA = ch.scales['x'], c2 = ch.ctx;
      var y50 = yA.getPixelForValue(50);
      // 50% dashed line
      c2.save();
      c2.beginPath(); c2.moveTo(xA.left, y50); c2.lineTo(xA.right, y50);
      c2.strokeStyle = '#bbb'; c2.lineWidth = 1.2; c2.setLineDash([5, 4]); c2.stroke();
      c2.setLineDash([]);
      c2.font = '10px Segoe UI, Arial'; c2.fillStyle = '#bbb'; c2.textAlign = 'right';
      c2.fillText('Neutral 50%', xA.right - 4, y50 - 4);
      // Value labels
      var meta = ch.getDatasetMeta(0);  // bars
      meta.data.forEach(function(bar, i) {{
        var val = pcts[i];
        if (val === null || isNaN(val)) return;
        c2.font = 'bold 11px Segoe UI, Arial';
        c2.fillStyle = '#333';
        c2.textAlign = 'center';
        c2.fillText(Math.round(val) + '%', bar.x, bar.y - 7);
      }});
      c2.restore();
    }}
  }}],
}});

var tooltip = document.getElementById('tooltip');
ctx.canvas.addEventListener('mousemove', function(e) {{
  var pts = chart.getElementsAtEventForMode(e, 'nearest', {{ intersect: true }}, true);
  if (!pts.length) {{ tooltip.style.display = 'none'; return; }}
  var i = pts[0].index, r = DETAIL[i];
  document.getElementById('tt-ticker').textContent = r.ticker;
  document.getElementById('tt-date').textContent   = r.date;
  document.getElementById('tt-net').textContent    = r.net.toLocaleString() + ' contracts';
  var chg = r.chg;
  document.getElementById('tt-chg').textContent    = (chg >= 0 ? '+' : '') + chg.toLocaleString();
  document.getElementById('tt-pct').textContent    = isNaN(r.pct) ? 'n/a' : Math.round(r.pct) + '%';
  tooltip.style.display = 'block';
  var tx = e.clientX + 16, ty = e.clientY - 20;
  if (tx + 220 > window.innerWidth) tx = e.clientX - 220;
  tooltip.style.left = tx + 'px'; tooltip.style.top = ty + 'px';
}});
ctx.canvas.addEventListener('mouseleave', function() {{ tooltip.style.display = 'none'; }});
</script>
</body>
</html>"""

    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Saved: {out_path}  ({os.path.getsize(out_path) // 1024} KB)")

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("Loading legacy COT...")
    raw = load_data()
    print(f"  {len(raw):,} rows")

    print("Processing...")
    df = process(raw)
    print(f"  {df['ticker'].nunique()} currency markets")

    print("Computing percentiles...")
    rows, report_date = build_rows(df)
    for r in rows:
        print(f"  {r['ticker']:4s}  {r['pct']:5.1f}%  net={r['net']:+,}  chg={r['chg']:+,}")

    print("Building chart...")
    build_html(rows, report_date, 'currency_positioning.html')
