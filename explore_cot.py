"""
CFTC COT Data Explorer
Step 1: Download and understand the data structure
"""
import urllib.request
import zipfile
import io
import pandas as pd
import os

PYTHON = r"C:\Users\xzhou\anaconda3\python.exe"

# CFTC publishes several COT report types. We start with the Legacy report
# which is the most widely used and covers all markets.
# URL for current year's Legacy COT (futures only)
LEGACY_URL = "https://www.cftc.gov/files/dea/history/fut_fin_xls_2025.zip"
LEGACY_URL_HIST = "https://www.cftc.gov/files/dea/history/com_disagg_xls_2025.zip"

# Actually let's use the combined historical file
COT_URLS = {
    "legacy_fin":   "https://www.cftc.gov/files/dea/history/fut_fin_xls_2025.zip",   # Financial futures (TFF format)
    "legacy_com":   "https://www.cftc.gov/files/dea/history/fut_disagg_xls_2025.zip", # Commodity futures (Disaggregated)
    "legacy_all":   "https://www.cftc.gov/files/dea/history/deafut_xls_2025.zip",     # Legacy All futures
}

def download_cot(url, label):
    print(f"\nDownloading {label}...")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
        z = zipfile.ZipFile(io.BytesIO(data))
        names = z.namelist()
        print(f"  Files in zip: {names}")
        # Read the first xls/xlsx/csv file
        for name in names:
            if name.lower().endswith(('.xls', '.xlsx', '.csv')):
                with z.open(name) as f:
                    if name.lower().endswith('.csv'):
                        df = pd.read_csv(f, encoding='latin1', low_memory=False)
                    else:
                        df = pd.read_excel(f)
                print(f"  Loaded: {name} -> {len(df)} rows x {len(df.columns)} cols")
                return df
    except Exception as e:
        print(f"  ERROR: {e}")
        return None

if __name__ == "__main__":
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.width', 120)
    pd.set_option('display.max_colwidth', 30)

    print("=" * 60)
    print("CFTC COMMITMENTS OF TRADERS (COT) DATA EXPLORER")
    print("=" * 60)

    # Download Legacy (all futures) - simplest starting point
    df = download_cot(COT_URLS["legacy_all"], "Legacy COT (All Futures)")

    if df is not None:
        print("\n--- COLUMNS ---")
        for i, col in enumerate(df.columns):
            print(f"  [{i:2d}] {col}")

        print("\n--- SAMPLE MARKETS (first 20 unique) ---")
        market_col = [c for c in df.columns if 'market' in c.lower() or 'name' in c.lower()][0]
        print(f"  (column: '{market_col}')")
        markets = df[market_col].unique()[:20]
        for m in markets:
            print(f"  {m}")

        print("\n--- DATE RANGE ---")
        date_col = [c for c in df.columns if 'date' in c.lower() or 'report' in c.lower()][0]
        print(f"  (column: '{date_col}')")
        print(f"  Earliest: {df[date_col].min()}")
        print(f"  Latest:   {df[date_col].max()}")
        print(f"  Total rows: {len(df)}")

        print("\n--- FIRST ROW (all fields) ---")
        row = df.iloc[0]
        for col, val in row.items():
            print(f"  {col:<50} {val}")

        # Save for later use
        os.makedirs("data", exist_ok=True)
        df.to_csv("data/legacy_cot_2025.csv", index=False)
        print("\n\nSaved to data/legacy_cot_2025.csv")
