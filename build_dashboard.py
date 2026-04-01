"""
CFTC COT Dashboard Builder v2
- Downloads 2021-2026 data
- Percentile ranks: 1Y / 3Y / 5Y
- Sparklines per market
- Click row → full time series modal (Chart.js)
"""
import pandas as pd
import numpy as np
import urllib.request, ssl, zipfile, io, os, json

# ── Ticker & Asset Class mappings ────────────────────────────────────────────

TICKER_MAP = {
    # ── FX (old names pre-Feb 2022 rename) ──
    "BRITISH POUND STERLING - CHICAGO MERCANTILE EXCHANGE":           "GBP",
    "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE":               "NZD",
    "SOUTH AFRICAN RAND - CHICAGO MERCANTILE EXCHANGE":               "ZAR",
    "U.S. DOLLAR INDEX - ICE FUTURES U.S.":                           "DXY",
    "CHINESE RENMINBI-HK (CNH) - CHICAGO MERCANTILE EXCHANGE":        "CNH",  # old name
    "USD/CHINESE RENMINBI-OFFSHORE - CHICAGO MERCANTILE EXCHANGE":    "CNH",  # newer name, discontinued Apr 2024
    # ── Equity Index (old names) ──
    "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE":    "NQ",
    "E-MINI RUSSELL 2000 INDEX - CHICAGO MERCANTILE EXCHANGE":        "RTY",
    "DOW JONES INDUSTRIAL AVG- x $5 - CHICAGO BOARD OF TRADE":       "YM($5)",
    "BLOOMBERG COMMODITY INDEX - CHICAGO BOARD OF TRADE":             "BCOM",
    "MSCI EMERGING MKTS INDEX - ICE FUTURES U.S.":                    "MXEF",
    "MSCI EAFE MINI INDEX - ICE FUTURES U.S.":                        "EAFE",
    # ── Rates (old names) ──
    "10-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE":           "ZN",
    "5-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE":            "ZF",
    "2-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE":            "ZT",
    "U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE":                   "ZB",
    "ULTRA U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE":             "UB",
    "ULTRA US T BOND - CHICAGO BOARD OF TRADE":                       "UB",   # third variant
    "ULTRA 10-YEAR U.S. T-NOTES - CHICAGO BOARD OF TRADE":            "TN",
    "30-DAY FEDERAL FUNDS - CHICAGO BOARD OF TRADE":                  "ZQ",
    "3-MONTH SOFR - CHICAGO MERCANTILE EXCHANGE":                     "SR3",
    "1-MONTH SOFR - CHICAGO MERCANTILE EXCHANGE":                     "SR1",
    "3-MONTH EURODOLLARS - CHICAGO MERCANTILE EXCHANGE":              "GE",   # predecessor to SOFR
    "EURODOLLARS-3M - CHICAGO MERCANTILE EXCHANGE":                   "GE",
    # ── Metals (old names) ──
    "COPPER-GRADE #1 - COMMODITY EXCHANGE INC.":                      "HG",
    "ALUMINUM MW US TR PLATTS - COMMODITY EXCHANGE INC.":             "ALMWP",
    "US MIDWEST DOMESTIC HOT-ROLL - COMMODITY EXCHANGE INC.":         "HRC",
    "ALUMINIUM EURO PREM DUTYUNPAID - COMMODITY EXCHANGE INC.":       "ALEU",
    "ALUM EUR UNPAID - COMMODITY EXCHANGE INC.":                      "ALEU",
    "US MIDWEST BUSHELING FERROUS S - COMMODITY EXCHANGE INC.":       "BUS",
    # ── Energy-Oil (old names) ──
    "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE":          "CL",
    "BRENT CRUDE OIL LAST DAY - NEW YORK MERCANTILE EXCHANGE":        "BZ",
    "GASOLINE BLENDSTOCK (RBOB) - NEW YORK MERCANTILE EXCHANGE":      "RB",
    "#2 HEATING OIL- NY HARBOR-ULSD - NEW YORK MERCANTILE EXCHANGE":  "HO",
    "NY HARBOR USLD - NEW YORK MERCANTILE EXCHANGE":                  "HO",   # typo variant
    # ── FX (current names) ──
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
    "EURO FX/BRITISH POUND XRATE - CHICAGO MERCANTILE EXCHANGE": "EUR/GBP",
    "EURO FX/JAPANESE YEN XRATE - CHICAGO MERCANTILE EXCHANGE":  "EUR/JPY",
    "USD INDEX - ICE FUTURES U.S.":                         "DXY",
    # Equity Indices
    "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE":               "ES",
    "E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE":   "ES",   # renamed → ES Feb 2022
    "S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE":          "SPX",  # renamed → SPX Consolidated
    "S&P 500 TOTAL RETURN INDEX - CHICAGO MERCANTILE EXCHANGE":   "SPX-TR",
    "S&P 500 Consolidated - CHICAGO MERCANTILE EXCHANGE":         "SPX",
    "MICRO E-MINI S&P 500 INDEX - CHICAGO MERCANTILE EXCHANGE": "MES",
    "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE":            "NQ",
    "NASDAQ-100 Consolidated - CHICAGO MERCANTILE EXCHANGE":"NDX",
    "MICRO E-MINI NASDAQ-100 INDEX - CHICAGO MERCANTILE EXCHANGE": "MNQ",
    "DJIA Consolidated - CHICAGO BOARD OF TRADE":           "YM",
    "DJIA x $5 - CHICAGO BOARD OF TRADE":                  "YM($5)",
    "MICRO E-MINI DJIA (x$0.5) - CHICAGO BOARD OF TRADE":  "MYM",
    "RUSSELL E-MINI - CHICAGO MERCANTILE EXCHANGE":         "RTY",
    "MICRO E-MINI RUSSELL 2000 INDX - CHICAGO MERCANTILE EXCHANGE": "M2K",
    "E-MINI S&P 400 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE": "EMD",
    "NIKKEI STOCK AVERAGE - CHICAGO MERCANTILE EXCHANGE":   "NKD",
    "NIKKEI STOCK AVERAGE YEN DENOM - CHICAGO MERCANTILE EXCHANGE": "NIY",
    "MSCI EAFE - ICE FUTURES U.S.":                         "EAFE",
    "MSCI EM INDEX - ICE FUTURES U.S.":                     "MXEF",
    "MINI MSCI ACWI NTR INDEX - ICE FUTURES U.S.":          "ACWI",
    "VIX FUTURES - CBOE FUTURES EXCHANGE":                  "VIX",
    "BBG COMMODITY - CHICAGO BOARD OF TRADE":               "BCOM",
    "DOW JONES U.S. REAL ESTATE IDX - CHICAGO BOARD OF TRADE": "DJRE",
    "EMINI RUSSELL 1000 GROWTH - CHICAGO MERCANTILE EXCHANGE": "RLG",
    "EMINI RUSSELL 1000 VALUE INDEX - CHICAGO MERCANTILE EXCHANGE": "RLV",
    "E-MINI S&P COMMUNICATION INDEX - CHICAGO MERCANTILE EXCHANGE": "XLC",
    "E-MINI S&P CONSU STAPLES INDEX - CHICAGO MERCANTILE EXCHANGE": "XLP",
    "E-MINI S&P ENERGY INDEX - CHICAGO MERCANTILE EXCHANGE":"XLE",
    "E-MINI S&P FINANCIAL INDEX - CHICAGO MERCANTILE EXCHANGE": "XLF",
    "E-MINI S&P HEALTH CARE INDEX - CHICAGO MERCANTILE EXCHANGE": "XLV",
    "E-MINI S&P INDUSTRIAL INDEX - CHICAGO MERCANTILE EXCHANGE": "XLI",
    "E-MINI S&P MATERIALS INDEX - CHICAGO MERCANTILE EXCHANGE": "XLB",
    "E-MINI S&P REAL ESTATE INDEX - CHICAGO MERCANTILE EXCHANGE": "XLRE",
    "E-MINI S&P TECHNOLOGY INDEX - CHICAGO MERCANTILE EXCHANGE": "XLK",
    "E-MINI S&P UTILITIES INDEX - CHICAGO MERCANTILE EXCHANGE": "XLU",
    # Rates
    "UST BOND - CHICAGO BOARD OF TRADE":                    "ZB",
    "ULTRA UST BOND - CHICAGO BOARD OF TRADE":              "UB",
    "ULTRA US T BOND - CHICAGO BOARD OF TRADE":             "UB",
    "UST 10Y NOTE - CHICAGO BOARD OF TRADE":                "ZN",
    "ULTRA UST 10Y - CHICAGO BOARD OF TRADE":               "TN",
    "UST 5Y NOTE - CHICAGO BOARD OF TRADE":                 "ZF",
    "UST 2Y NOTE - CHICAGO BOARD OF TRADE":                 "ZT",
    "FED FUNDS - CHICAGO BOARD OF TRADE":                   "ZQ",
    "SOFR-3M - CHICAGO MERCANTILE EXCHANGE":                "SR3",
    "SOFR-1M - CHICAGO MERCANTILE EXCHANGE":                "SR1",
    "MICRO 10 YEAR YIELD - CHICAGO BOARD OF TRADE":         "10Y",
    "EURO SHORT TERM RATE - CHICAGO MERCANTILE EXCHANGE":   "ESTR",
    # Crypto
    "BITCOIN - CHICAGO MERCANTILE EXCHANGE":                "BTC",
    "MICRO BITCOIN - CHICAGO MERCANTILE EXCHANGE":          "MBT",
    "Nano Bitcoin - COINBASE DERIVATIVES, LLC":             "nBTC",
    "Nano Bitcoin - LMX LABS LLC":                          "nBTC(LMX)",
    "NANO BITCOIN PERP STYLE - COINBASE DERIVATIVES, LLC":  "nBTC-P",
    "ETHER CASH SETTLED - CHICAGO MERCANTILE EXCHANGE":     "ETH",
    "MICRO ETHER - CHICAGO MERCANTILE EXCHANGE":            "MET",
    "NANO ETHER - COINBASE DERIVATIVES, LLC":               "nETH",
    "NANO ETHER - LMX LABS LLC":                            "nETH(LMX)",
    "NANO ETHER PERP STYLE - COINBASE DERIVATIVES, LLC":    "nETH-P",
    "SOL - CHICAGO MERCANTILE EXCHANGE":                    "SOL",
    "MICRO SOL - CHICAGO MERCANTILE EXCHANGE":              "MSOL",
    "NANO SOLANA - COINBASE DERIVATIVES, LLC":              "nSOL",
    "NANO SOLANA - LMX LABS LLC":                           "nSOL(LMX)",
    "NANO SOLANA PERP STYLE - COINBASE DERIVATIVES, LLC":   "nSOL-P",
    "XRP - CHICAGO MERCANTILE EXCHANGE":                    "XRP",
    "MICRO XRP - CHICAGO MERCANTILE EXCHANGE":              "MXRP",
    "NANO XRP - COINBASE DERIVATIVES, LLC":                 "nXRP",
    "NANO XRP - LMX LABS LLC":                              "nXRP(LMX)",
    "NANO XRP PERP STYLE - COINBASE DERIVATIVES, LLC":      "nXRP-P",
    "DOGECOIN - COINBASE DERIVATIVES, LLC":                 "DOGE",
    "DOGECOIN - LMX LABS LLC":                              "DOGE(LMX)",
    "AVALANCHE - COINBASE DERIVATIVES, LLC":                "AVAX",
    "AVALANCHE - LMX LABS LLC":                             "AVAX(LMX)",
    "AVALANCHE PERP STYLE - COINBASE DERIVATIVES, LLC":     "AVAX-P",
    "CHAINLINK - COINBASE DERIVATIVES, LLC":                "LINK",
    "CHAINLINK - LMX LABS LLC":                             "LINK(LMX)",
    "CHAINLINK PERP STYLE - COINBASE DERIVATIVES, LLC":     "LINK-P",
    "CARDONA - COINBASE DERIVATIVES, LLC":                  "ADA",
    "CARDONA - LMX LABS LLC":                               "ADA(LMX)",
    "CARDONA PERP STYLE - COINBASE DERIVATIVES, LLC":       "ADA-P",
    "POLKADOT - COINBASE DERIVATIVES, LLC":                 "DOT",
    "POLKADOT - LMX LABS LLC":                              "DOT(LMX)",
    "LITECOIN CASH - COINBASE DERIVATIVES, LLC":            "LTC",
    "LITECOIN CASH - LMX LABS LLC":                         "LTC(LMX)",
    "HEDERA - COINBASE DERIVATIVES, LLC":                   "HBAR",
    "1K SHIB - COINBASE DERIVATIVES, LLC":                  "SHIB",
    "1K SHIB - LMX LABS LLC":                               "SHIB(LMX)",
    "NANO STELLAR - COINBASE DERIVATIVES, LLC":             "nXLM",
    "NANO STELLAR - LMX LABS LLC":                          "nXLM(LMX)",
    "STELLAR - LMX LABS LLC":                               "XLM",
    "GOLD -1 TROY OUNCE - COINBASE DERIVATIVES, LLC":       "GLD1oz",
    # Metals
    "GOLD - COMMODITY EXCHANGE INC.":                       "GC",
    "MICRO GOLD - COMMODITY EXCHANGE INC.":                 "MGC",
    "SILVER - COMMODITY EXCHANGE INC.":                     "SI",
    "COPPER- #1 - COMMODITY EXCHANGE INC.":                 "HG",
    "PLATINUM - NEW YORK MERCANTILE EXCHANGE":              "PL",
    "PALLADIUM - NEW YORK MERCANTILE EXCHANGE":             "PA",
    "ALUMINUM - COMMODITY EXCHANGE INC.":                   "ALI",
    "ALUMINUM MWP - COMMODITY EXCHANGE INC.":               "ALMWP",
    "ALUMINIUM EURO PREM DUTY-PAID - COMMODITY EXCHANGE INC.": "ALEU",
    "COBALT - COMMODITY EXCHANGE INC.":                     "CBT-M",
    "LITHIUM HYDROXIDE - COMMODITY EXCHANGE INC.":          "LiOH",
    "STEEL-HRC - COMMODITY EXCHANGE INC.":                  "HRC",
    "NORTH EURO HOT-ROLL COIL STEEL - COMMODITY EXCHANGE INC.": "EURHRC",
    "CHICAGO BUSHELING FERROUS - COMMODITY EXCHANGE INC.":  "BUS",
    # Energy
    "WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE":          "CL",
    "WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE": "CLF",
    "CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE":     "CL(ICE)",
    "BRENT LAST DAY - NEW YORK MERCANTILE EXCHANGE":        "BZ",
    "NAT GAS NYME - NEW YORK MERCANTILE EXCHANGE":          "NG",
    "HENRY HUB - NEW YORK MERCANTILE EXCHANGE":             "NG-HH",
    "HENRY HUB LAST DAY FIN - NEW YORK MERCANTILE EXCHANGE": "NG-LDF",
    "HENRY HUB PENULTIMATE FIN - NEW YORK MERCANTILE EXCHANGE": "NG-PF",
    "HENRY HUB PENULTIMATE NAT GAS - NEW YORK MERCANTILE EXCHANGE": "NG-PNG",
    "GASOLINE RBOB - NEW YORK MERCANTILE EXCHANGE":         "RB",
    "RBOB CALENDAR - NEW YORK MERCANTILE EXCHANGE":         "RB-C",
    "NY HARBOR ULSD - NEW YORK MERCANTILE EXCHANGE":        "HO",
    "ETHANOL - NEW YORK MERCANTILE EXCHANGE":               "EH",
    "PROPANE - NEW YORK MERCANTILE EXCHANGE":               "PG",
    "PROPANE NON-LDH MT BEL - NEW YORK MERCANTILE EXCHANGE": "PG-NL",
    "CONWAY PROPANE (OPIS) - NEW YORK MERCANTILE EXCHANGE": "PG-CON",
    "EUROPEAN PROPANE CIF ARA - NEW YORK MERCANTILE EXCHANGE": "PG-EU",
    "MT BELV NAT GASOLINE OPIS - NEW YORK MERCANTILE EXCHANGE": "NGL",
    "MT BELVIEU ETHANE OPIS - NEW YORK MERCANTILE EXCHANGE": "ETH-G",
    "MT BELV NORM BUTANE OPIS - NEW YORK MERCANTILE EXCHANGE": "BUT",
    "MT BELVIEU LDH PROPANE BALMO - NEW YORK MERCANTILE EXCHANGE": "PG-BAL",
    "LUMBER - CHICAGO MERCANTILE EXCHANGE":                 "LB",
    "CANOLA - ICE FUTURES U.S.":                            "RS",
    "USD Malaysian Crude Palm Oil C - CHICAGO MERCANTILE EXCHANGE": "CPO",
    # Agriculture
    "CORN - CHICAGO BOARD OF TRADE":                        "ZC",
    "SOYBEANS - CHICAGO BOARD OF TRADE":                    "ZS",
    "SOYBEAN MEAL - CHICAGO BOARD OF TRADE":                "ZM",
    "SOYBEAN OIL - CHICAGO BOARD OF TRADE":                 "ZL",
    "WHEAT-SRW - CHICAGO BOARD OF TRADE":                   "ZW",
    "WHEAT-HRW - CHICAGO BOARD OF TRADE":                   "KE",
    "WHEAT-HRSpring - MIAX FUTURES EXCHANGE":               "MWE",
    "OATS - CHICAGO BOARD OF TRADE":                        "ZO",
    "ROUGH RICE - CHICAGO BOARD OF TRADE":                  "ZR",
    "COTTON NO. 2 - ICE FUTURES U.S.":                      "CT",
    "COFFEE C - ICE FUTURES U.S.":                          "KC",
    "SUGAR NO. 11 - ICE FUTURES U.S.":                      "SB",
    "COCOA - ICE FUTURES U.S.":                             "CC",
    "FRZN CONCENTRATED ORANGE JUICE - ICE FUTURES U.S.":    "OJ",
    # Livestock / Dairy
    "LIVE CATTLE - CHICAGO MERCANTILE EXCHANGE":            "LE",
    "FEEDER CATTLE - CHICAGO MERCANTILE EXCHANGE":          "GF",
    "LEAN HOGS - CHICAGO MERCANTILE EXCHANGE":              "HE",
    "MILK, Class III - CHICAGO MERCANTILE EXCHANGE":        "DC",
    "BUTTER (CASH SETTLED) - CHICAGO MERCANTILE EXCHANGE":  "CB",
    "CHEESE (CASH-SETTLED) - CHICAGO MERCANTILE EXCHANGE":  "CSC",
    "NON FAT DRY MILK - CHICAGO MERCANTILE EXCHANGE":       "NF",
}

def get_ticker(name):
    if name in TICKER_MAP:
        return TICKER_MAP[name]
    base = name.rsplit(' - ', 1)[0].strip()
    return base[:12].strip()

def get_asset_class(name):
    m = name.upper()
    if any(x in m for x in ['BITCOIN','ETHER','SOLANA','SOL -','SOL-',' SOL ','XRP','DOGECOIN',
                              'AVALANCHE','CHAINLINK','CARDONA','POLKADOT','LITECOIN','HEDERA',
                              'SHIB','STELLAR','NANO BIT','NANO ETH','NANO SOL','NANO XRP',
                              'NANO STEL','MICRO BIT','MICRO ETH','MICRO SOL','MICRO XRP']):
        return 'Crypto'
    if any(x in m for x in ['AUSTRALIAN DOLLAR','BRITISH POUND','CANADIAN DOLLAR','EURO FX',
                              'JAPANESE YEN','SWISS FRANC','NZ DOLLAR','NEW ZEALAND DOLLAR',
                              'MEXICAN PESO','SO AFRICAN RAND','SOUTH AFRICAN RAND',
                              'BRAZILIAN REAL','USD INDEX','U.S. DOLLAR INDEX',
                              'EURO FX/BRITISH','EURO FX/JAPANESE',
                              'RENMINBI','CHINESE RENMINBI']):
        return 'FX'
    if any(x in m for x in ['S&P','NASDAQ','DJIA','RUSSELL','NIKKEI','MSCI','VIX',
                              'DOW JONES','EMINI RUSSELL','BBG COMMODITY']):
        return 'Equity Index'
    if any(x in m for x in ['UST ','U.S. TREASURY','SOFR','FED FUNDS','EURO SHORT TERM RATE',
                              'ERIS SOFR','MICRO 10 YEAR','ULTRA UST','ULTRA US T',
                              'EURODOLLAR','FEDERAL FUNDS','TREASURY NOTES','TREASURY BONDS']):
        return 'Rates'
    if any(x in m for x in ['GOLD','SILVER','COPPER','PLATINUM','PALLADIUM','ALUMINUM',
                              'ALUMINIUM','COBALT','LITHIUM','STEEL','FERROUS']):
        return 'Metals'
    if any(x in m for x in ['CRUDE OIL','WTI','BRENT','GASOLINE','RBOB','ULSD',
                              'HEATING OIL','ETHANOL','GULF JET','GULF # 6','FUEL OIL',
                              'MARINE FUEL','MARINE .5%','GASOLINE CRK','JET UP-DOWN','UP DOWN GC']):
        return 'Energy-Oil'
    if any(x in m for x in ['NAT GAS','NATURAL GAS','HENRY HUB','ALGONQUIN','PANHANDLE',
                              'TRANSCO','DOMINION','WAHA','MALIN','SOCAL','PG&E','MICHCON',
                              'NGPL','REX ZONE','NWP ','NNG ','CG ','CIG ','AECO','TCO ',
                              'TETCO','TGT ZONE','SONAT','EP SAN JUAN','FLORIDA GAS',
                              'ONEOK','HSC FIN','HOUSTON SHIP','CHICAGO CITYGATE','CHICAGO FIN']):
        return 'Energy-Gas'
    if any(x in m for x in ['PROPANE','ETHANE','BUTANE','ISOBUTANE','MT BELV','MT. BELV',
                              'CONWAY','NATURAL GASOLINE','PGP PROPYLENE','CONDENSATE']):
        return 'Energy-NGL'
    if any(x in m for x in ['ERCOT','PJM','MISO','NYISO','CAISO','ISO NE','ISONE',
                              'MID-C','PALO VERDE','SP15','SPP SOUTH','AEP DAYTON','NODAL']):
        return 'Power'
    if any(x in m for x in ['CARBON','RGGI','REC ','RECS','GREEN-E','SOLAR REC','AEC TIER',
                              'LOW CARBON','COMPLIANCE REC','SREC','D4 BIODIESEL','D6 RINs']):
        return 'Carbon/REC'
    if any(x in m for x in ['CORN','SOYBEAN','WHEAT','OATS','RICE','CANOLA','PALM OIL']):
        return 'Agri-Grains'
    if any(x in m for x in ['COFFEE','SUGAR','COCOA','COTTON','ORANGE JUICE','LUMBER']):
        return 'Agri-Softs'
    if any(x in m for x in ['CATTLE','HOGS','MILK','BUTTER','CHEESE','NON FAT DRY']):
        return 'Livestock'
    return 'Other'

# ── Data loading ──────────────────────────────────────────────────────────────

def download_year(year, ctx):
    url = f"https://www.cftc.gov/files/dea/history/deacot{year}.zip"
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
        cache = f'data/legacy_cot_{year}.csv'
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
    print(f"Total: {len(combined):,} rows across {combined['As of Date in Form YYYY-MM-DD'].nunique()} dates")
    return combined

# ── Compute net positions ─────────────────────────────────────────────────────

def to_num(s):
    return pd.to_numeric(s, errors='coerce').fillna(0)

def compute_nets(df):
    df = df.copy()
    df['date']         = pd.to_datetime(df['As of Date in Form YYYY-MM-DD'], errors='coerce')
    df['spec_net']     = to_num(df['Noncommercial Positions-Long (All)'])   - to_num(df['Noncommercial Positions-Short (All)'])
    df['comm_net']     = to_num(df['Commercial Positions-Long (All)'])      - to_num(df['Commercial Positions-Short (All)'])
    df['nr_net']       = to_num(df['Nonreportable Positions-Long (All)'])   - to_num(df['Nonreportable Positions-Short (All)'])
    df['oi']           = to_num(df['Open Interest (All)'])
    df['oi_chg']       = to_num(df['Change in Open Interest (All)'])
    df['spec_net_chg'] = to_num(df['Change in Noncommercial-Long (All)'])   - to_num(df['Change in Noncommercial-Short (All)'])
    df['comm_net_chg'] = to_num(df['Change in Commercial-Long (All)'])      - to_num(df['Change in Commercial-Short (All)'])
    df['nr_net_chg']   = to_num(df['Change in Nonreportable-Long (All)'])   - to_num(df['Change in Nonreportable-Short (All)'])
    df['traders']      = to_num(df['Traders-Total (All)'])
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
    now      = df['date'].max()
    one_y    = now - pd.DateOffset(years=1)
    three_y  = now - pd.DateOffset(years=3)
    five_y   = now - pd.DateOffset(years=5)
    one_y_s  = now - pd.DateOffset(weeks=52)

    # Tag each row with ticker and asset class, then group by ticker
    # so renamed markets (e.g. ES old name → ES new name) share one combined history
    df['_ticker'] = df['Market and Exchange Names'].apply(get_ticker)
    df['_asset']  = df['Market and Exchange Names'].apply(get_asset_class)

    active_cutoff = now - pd.DateOffset(months=6)
    rows = []
    ts_data = {}

    for ticker, grp in df.groupby('_ticker'):
        grp = grp.sort_values('date').drop_duplicates('date', keep='last')
        latest = grp.iloc[-1]
        # Skip discontinued markets (no data in last 6 months)
        if latest['date'] < active_cutoff:
            continue
        cur_spec = latest['spec_net']
        # Use the most common market name (latest) for display
        market   = latest['Market and Exchange Names']
        asset    = TICKER_CATEGORY.get(ticker, latest['_asset'])

        hist_1y = grp[grp['date'] >= one_y]['spec_net']
        hist_3y = grp[grp['date'] >= three_y]['spec_net']
        hist_5y = grp[grp['date'] >= five_y]['spec_net']

        pct1y = pct_rank(hist_1y, cur_spec)
        pct3y = pct_rank(hist_3y, cur_spec)
        pct5y = pct_rank(hist_5y, cur_spec)

        spark_vals = grp[grp['date'] >= one_y_s]['spec_net'].tolist()

        ts_data[ticker] = {
            'd': grp['date'].dt.strftime('%Y-%m-%d').tolist(),
            's': [round(v) for v in grp['spec_net'].tolist()],
            'c': [round(v) for v in grp['comm_net'].tolist()],
            'n': [round(v) for v in grp['nr_net'].tolist()],
            'p': [],   # price — filled in load_prices()
        }

        rows.append({
            'Market':       market,
            'Ticker':       ticker,
            'Asset Class':  asset,
            'Date':         latest['date'].strftime('%Y-%m-%d'),
            'OI':           int(latest['oi']),
            'OI Chg':       int(latest['oi_chg']),
            'Spec Net':     int(cur_spec),
            'Spec Chg':     int(latest['spec_net_chg']),
            'Pct1Y':        pct1y,
            'Pct3Y':        pct3y,
            'Pct5Y':        pct5y,
            'Comm Net':     int(latest['comm_net']),
            'Comm Chg':     int(latest['comm_net_chg']),
            'NR Net':       int(latest['nr_net']),
            'NR Chg':       int(latest['nr_net_chg']),
            '# Traders':    int(latest['traders']),
            '_spark':       make_sparkline(spark_vals),
        })

    out = pd.DataFrame(rows).sort_values('Ticker').reset_index(drop=True)
    return out, ts_data

# ── Price loading ─────────────────────────────────────────────────────────────

def load_prices(ts_data, price_dir='data/prices'):
    """For each ticker that has a price CSV, align weekly closes to COT dates."""
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

            # For each COT date, find the nearest price (within 7 days)
            cot_dates = pd.to_datetime(d['d'])
            prices = []
            for dt in cot_dates:
                # Get last available price on or before COT date
                subset = px_series[px_series.index <= dt]
                if len(subset) == 0 or (dt - subset.index[-1]).days > 7:
                    prices.append(None)
                else:
                    prices.append(round(float(subset.iloc[-1]), 4))
            d['p'] = prices
            loaded += 1
        except Exception as e:
            pass
    print(f"  Prices loaded for {loaded}/{len(ts_data)} tickers")
    return ts_data

# ── HTML helpers ──────────────────────────────────────────────────────────────

def net_color(val, col):
    try:
        v = float(val)
    except:
        return ''
    if col in ('Spec Net', 'Spec Chg', 'Comm Net', 'Comm Chg', 'NR Net', 'NR Chg'):
        if v > 0: return 'background:#d4edda'
        if v < 0: return 'background:#f8d7da'
    if col in ('OI Chg',):
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

# Maps our internal ticker → category from cftc_markets.csv
# CSV uses different codes (B6, E6, QR...) so we map manually
TICKER_CATEGORY = {
    # Grains
    'ZW': 'Grains', 'ZC': 'Grains', 'ZS': 'Grains', 'ZM': 'Grains',
    'ZL': 'Grains', 'ZR': 'Grains', 'KE': 'Grains', 'MWE': 'Grains', 'RS': 'Grains',
    # Energies
    'CL': 'Energies', 'HO': 'Energies', 'RB': 'Energies', 'NG': 'Energies', 'EH': 'Energies',
    # Livestock & Dairy
    'LE': 'Livestock & Dairy', 'GF': 'Livestock & Dairy', 'HE': 'Livestock & Dairy',
    'DC': 'Livestock & Dairy', 'NF': 'Livestock & Dairy', 'CB': 'Livestock & Dairy', 'CSC': 'Livestock & Dairy',
    # Metals
    'GC': 'Metals', 'SI': 'Metals', 'HG': 'Metals', 'PL': 'Metals', 'PA': 'Metals', 'ALMWP': 'Metals',
    # Softs
    'CT': 'Softs', 'OJ': 'Softs', 'KC': 'Softs', 'SB': 'Softs', 'CC': 'Softs', 'LB': 'Softs',
    # Currencies
    'DXY': 'Currencies', 'GBP': 'Currencies', 'CAD': 'Currencies', 'JPY': 'Currencies',
    'CHF': 'Currencies', 'EUR': 'Currencies', 'AUD': 'Currencies', 'MXN': 'Currencies',
    'NZD': 'Currencies', 'ZAR': 'Currencies', 'BRL': 'Currencies',
    # Financials
    'ZB': 'Financials', 'UB': 'Financials', 'ZN': 'Financials', 'TN': 'Financials',
    'ZF': 'Financials', 'ZT': 'Financials', 'ZQ': 'Financials', 'SR3': 'Financials',
    # Indices
    'ES': 'Indices', 'NQ': 'Indices', 'YM': 'Indices', 'RTY': 'Indices',
    'EMD': 'Indices', 'VIX': 'Indices', 'MES': 'Indices', 'MNQ': 'Indices',
    # Crypto (not in CSV, added separately)
    'BTC': 'Crypto', 'MBT': 'Crypto',
}

KEEP_TICKERS = {
    # Grains
    'ZW', 'ZC', 'ZS', 'ZM', 'ZL', 'ZR', 'KE', 'MWE', 'RS',
    # Energy
    'CL', 'HO', 'RB', 'NG', 'EH',
    # Livestock & Dairy
    'LE', 'GF', 'HE', 'DC', 'NF', 'CB', 'CSC',
    # Metals
    'GC', 'SI', 'HG', 'PL', 'PA', 'ALMWP',
    # Softs
    'CT', 'OJ', 'KC', 'SB', 'CC', 'LB',
    # FX
    'DXY', 'GBP', 'CAD', 'JPY', 'CHF', 'EUR', 'AUD', 'MXN', 'NZD', 'ZAR', 'BRL',
    # Rates
    'ZB', 'UB', 'ZN', 'TN', 'ZF', 'ZT', 'ZQ', 'SR3',
    # Equity Index
    'ES', 'NQ', 'YM', 'RTY', 'EMD', 'VIX', 'MES', 'MNQ',
    # Crypto
    'BTC', 'MBT',
}

def build_html(df, ts_data, out_path):
    df = df[df['Ticker'].isin(KEEP_TICKERS)].copy()
    ts_data = {k: v for k, v in ts_data.items() if df['Ticker'].eq(k).any()}
    report_date = df['Date'].max()
    n_markets   = len(df)

    display_cols = ['Ticker','Asset Class','Date','OI','OI Chg',
                    'Spec Net','Spec Chg','Pct1Y','Pct3Y','Pct5Y',
                    'Comm Net','Comm Chg','NR Net','NR Chg','# Traders','_spark']

    col_labels = {c: c for c in display_cols}
    col_labels['_spark'] = 'Spec Trend (1Y)'

    col_tips = {
        'Ticker':      'Short ticker — hover for full market name',
        'Asset Class': 'Asset class category',
        'Date':        'Most recent report date',
        'OI':          'Total open interest',
        'OI Chg':      'Weekly change in open interest',
        'Spec Net':    'Speculator net position (Long − Short)',
        'Spec Chg':    'Weekly change in Spec Net',
        'Pct1Y':       'Percentile of current Spec Net vs past 1 year (100% = most bullish ever)',
        'Pct3Y':       'Percentile vs past 3 years',
        'Pct5Y':       'Percentile vs past 5 years',
        'Comm Net':    'Commercial net position',
        'Comm Chg':    'Weekly change in Comm Net',
        'NR Net':      'Non-Reportable (retail) net position',
        'NR Chg':      'Weekly change in NR Net',
        '# Traders':   'Total reportable traders',
        '_spark':      'Spec Net trend over last 52 weeks. Click row for full chart.',
    }

    # Build table rows
    rows_html = ''
    for _, row in df.iterrows():
        tkr_escaped = row['Ticker'].replace("'", "\\'")
        mkt_escaped = row['Market'].replace('"', '&quot;')
        rows_html += f'<tr onclick="showChart(\'{tkr_escaped}\')" style="cursor:pointer">'
        for col in display_cols:
            if col == '_spark':
                rows_html += f'<td style="padding:2px 6px">{row[col]}</td>'
            elif col == 'Ticker':
                full = row['Market'].replace('"','&quot;')
                rows_html += f'<td title="{full}" style="text-align:left;white-space:nowrap;font-weight:bold">{row[col]}</td>'
            elif col == 'Asset Class':
                rows_html += f'<td style="text-align:left;white-space:nowrap;font-size:10px;color:#555">{row[col]}</td>'
            elif col in ('Pct1Y','Pct3Y','Pct5Y'):
                style = pct_color_style(row[col])
                rows_html += f'<td style="text-align:right;{style}">{fmt(row[col], col)}</td>'
            else:
                style = net_color(row[col], col)
                rows_html += f'<td style="text-align:right;{style}">{fmt(row[col], col)}</td>'
        rows_html += '</tr>\n'

    # Header
    header_html = ''
    for i, col in enumerate(display_cols):
        tip   = col_tips.get(col, '')
        label = col_labels[col]
        if col == '_spark':
            header_html += f'<th title="{tip}" style="white-space:nowrap">{label}</th>'
        else:
            header_html += f'<th onclick="sortTable({i})" title="{tip}" style="cursor:pointer;white-space:nowrap">{label} &#8597;</th>'

    ts_json = json.dumps(ts_data, separators=(',', ':'))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>CFTC COT Dashboard</title>
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
  th {{ background:#2c3e50; color:white; padding:8px 10px; font-size:11px; position:sticky; top:0; cursor:pointer; white-space:nowrap; }}
  th:hover {{ background:#34495e; }}
  td {{ padding:5px 10px; border-bottom:1px solid #eee; font-size:11px; }}
  tr:hover td {{ background:#f0f7ff !important; }}
  tr {{ cursor:pointer; }}
  /* Modal */
  #overlay {{ display:none; position:fixed; inset:0; background:rgba(0,0,0,0.55); z-index:200; justify-content:center; align-items:center; }}
  #overlay.show {{ display:flex; }}
  #modal {{ background:white; border-radius:8px; padding:20px; width:820px; max-width:95vw; position:relative; box-shadow:0 4px 20px rgba(0,0,0,0.3); }}
  #modal h2 {{ font-size:14px; margin-bottom:4px; }}
  #modal .msub {{ font-size:11px; color:#777; margin-bottom:12px; }}
  #modal .btns {{ margin-bottom:10px; }}
  #modal .btns button {{ padding:4px 12px; margin-right:6px; font-size:11px; cursor:pointer; border:1px solid #ccc; border-radius:4px; background:#f8f8f8; }}
  #modal .btns button.active {{ background:#2c3e50; color:white; border-color:#2c3e50; }}
  #closeBtn {{ position:absolute; top:12px; right:14px; font-size:18px; cursor:pointer; color:#777; border:none; background:none; }}
  #chartBox {{ position:relative; height:320px; }}
</style>
</head>
<body>

<h1>CFTC Commitments of Traders — Positioning Matrix</h1>
<div class="subtitle">Latest report: <b>{report_date}</b> &nbsp;|&nbsp; {n_markets} markets &nbsp;|&nbsp; Click any row for time series &nbsp;|&nbsp; Hover column headers for definitions</div>
<div class="controls">
  <input type="text" id="search" onkeyup="filterTable()" placeholder="Search ticker or market name...">
  <select id="assetFilter" onchange="filterTable()">
    <option value="">All</option>
    <option>Currencies</option>
    <option>Indices</option>
    <option>Financials</option>
    <option>Metals</option>
    <option>Energies</option>
    <option>Grains</option>
    <option>Softs</option>
    <option>Livestock &amp; Dairy</option>
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
  var n     = Math.min(weeks, d.d.length);
  var dates = d.d.slice(-n);
  var spec  = d.s.slice(-n);
  var comm  = d.c.slice(-n);
  var nr    = d.n.slice(-n);
  var price = (d.p && d.p.length) ? d.p.slice(-n) : [];
  var hasPrice = price.some(function(v) {{ return v !== null && v !== undefined; }});

  if (chart) chart.destroy();
  var ctx = document.getElementById('myChart').getContext('2d');

  var datasets = [
    {{ label: 'Spec Net', data: spec, borderColor:'#2980b9', backgroundColor:'rgba(41,128,185,0.08)', borderWidth:2, pointRadius:0, tension:0.3, yAxisID:'y' }},
    {{ label: 'Comm Net', data: comm, borderColor:'#e67e22', backgroundColor:'rgba(230,126,34,0.08)',  borderWidth:2, pointRadius:0, tension:0.3, yAxisID:'y' }},
    {{ label: 'NR Net',   data: nr,   borderColor:'#95a5a6', backgroundColor:'rgba(149,165,166,0.08)', borderWidth:1.5, pointRadius:0, tension:0.3, borderDash:[4,4], yAxisID:'y' }},
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
    var mAC = !ac || aclass === ac || aclass === ac.replace('&amp;','&');
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
    print("Loading data...")
    raw = load_all_data()
    print("Computing nets...")
    raw = compute_nets(raw)
    print("Building matrix...")
    matrix, ts_data = build_matrix(raw)
    print("Loading prices...")
    ts_data = load_prices(ts_data)
    print("Generating HTML...")
    build_html(matrix, ts_data, 'dashboard.html')
